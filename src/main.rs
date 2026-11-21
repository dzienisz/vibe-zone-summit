use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rusqlite::{params, Connection};
use serde_json::Value;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;

const MAX_BODY: usize = 16 * 1024;
const FEED_SIZE: usize = 20;
const KEEPALIVE_LIMIT: usize = 3000;

struct FeedPost {
    id: i64,
    prefix: String, // {"id":..,"author":..,"content":..,"created_at":..,"likes":
}

struct State {
    likes: Vec<u32>, // index = post id
    recent: VecDeque<FeedPost>,
    feed: Bytes,
}

impl State {
    fn rebuild_feed(&mut self) {
        let mut out = String::with_capacity(8192);
        out.push_str("{\"posts\":[");
        for (i, p) in self.recent.iter().rev().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&p.prefix);
            out.push_str(&self.likes[p.id as usize].to_string());
            out.push('}');
        }
        out.push_str("]}");
        self.feed = Bytes::from(out);
    }
}

enum Op {
    Post { author: String, content: String, tx: oneshot::Sender<Bytes> },
    Like { post_id: i64, user: String, tx: oneshot::Sender<Option<u32>> },
}

struct Shared {
    state: Mutex<State>,
    writer: mpsc::Sender<Op>,
}

fn post_prefix(id: i64, author: &str, content: &str, created: &str) -> String {
    let mut s = String::with_capacity(content.len() + 96);
    s.push_str("{\"id\":");
    s.push_str(&id.to_string());
    s.push_str(",\"author\":");
    s.push_str(&serde_json::to_string(author).unwrap());
    s.push_str(",\"content\":");
    s.push_str(&serde_json::to_string(content).unwrap());
    s.push_str(",\"created_at\":\"");
    s.push_str(created);
    s.push_str("\",\"likes\":");
    s
}

fn now_iso() -> String {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let ms = d.as_millis() as i64;
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400);
    // civil_from_days (Howard Hinnant)
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let dd = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        y, m, dd, sod / 3600, (sod % 3600) / 60, sod % 60, millis
    )
}

fn open_db(path: &std::path::Path) -> Connection {
    let db = Connection::open(path).expect("open db");
    db.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA wal_autocheckpoint=4000;
         PRAGMA cache_size=-65536;
         PRAGMA temp_store=MEMORY;
         CREATE TABLE IF NOT EXISTS posts (
             id INTEGER PRIMARY KEY,
             author TEXT NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS likes (
             post_id INTEGER NOT NULL, user TEXT NOT NULL,
             PRIMARY KEY (post_id, user)) WITHOUT ROWID;",
    )
    .expect("init db");
    db
}

fn load(db: &Connection) -> (State, HashSet<(i64, Box<str>)>, i64) {
    let max_id: i64 = db
        .query_row("SELECT COALESCE(MAX(id),0) FROM posts", [], |r| r.get(0))
        .unwrap();
    let mut likes = vec![0u32; max_id as usize + 1];
    let mut set = HashSet::new();
    {
        let mut st = db.prepare("SELECT post_id, user FROM likes").unwrap();
        let mut rows = st.query([]).unwrap();
        while let Some(r) = rows.next().unwrap() {
            let pid: i64 = r.get(0).unwrap();
            let user: String = r.get(1).unwrap();
            if (pid as usize) < likes.len() {
                likes[pid as usize] += 1;
            }
            set.insert((pid, user.into_boxed_str()));
        }
    }
    let mut recent = VecDeque::new();
    {
        let mut st = db
            .prepare("SELECT id, author, content, created_at FROM posts ORDER BY id DESC LIMIT 20")
            .unwrap();
        let mut rows = st.query([]).unwrap();
        while let Some(r) = rows.next().unwrap() {
            let id: i64 = r.get(0).unwrap();
            let a: String = r.get(1).unwrap();
            let c: String = r.get(2).unwrap();
            let t: String = r.get(3).unwrap();
            recent.push_front(FeedPost { id, prefix: post_prefix(id, &a, &c, &t) });
        }
    }
    let mut state = State { likes, recent, feed: Bytes::new() };
    state.rebuild_feed();
    (state, set, max_id)
}

fn writer_loop(
    db: Connection,
    shared: Arc<Shared>,
    rx: mpsc::Receiver<Op>,
    mut set: HashSet<(i64, Box<str>)>,
    mut max_id: i64,
) {
    let mut batch: Vec<Op> = Vec::with_capacity(1024);
    while let Ok(first) = rx.recv() {
        batch.push(first);
        while batch.len() < 4096 {
            match rx.try_recv() {
                Ok(op) => batch.push(op),
                Err(_) => break,
            }
        }
        // counts as seen by this batch (base from shared state)
        let base_likes: Vec<(usize, u32)>;
        let created = now_iso();
        let mut new_posts: Vec<(i64, String)> = Vec::new();
        let mut like_inc: Vec<(i64, u32)> = Vec::new(); // (post, resulting count)
        enum Res { Post(Bytes), Like(Option<u32>) }
        let mut results: Vec<Res> = Vec::with_capacity(batch.len());
        {
            let st = shared.state.lock().unwrap();
            base_likes = Vec::new();
            let _ = &base_likes;
            let tx = db.unchecked_transaction().expect("begin");
            {
                let mut ins_post = tx
                    .prepare_cached("INSERT INTO posts(id, author, content, created_at) VALUES (?1, ?2, ?3, ?4)")
                    .unwrap();
                let mut ins_like = tx
                    .prepare_cached("INSERT INTO likes(post_id, user) VALUES (?1, ?2)")
                    .unwrap();
                let mut pending: std::collections::HashMap<i64, u32> = std::collections::HashMap::new();
                for op in batch.iter() {
                    match op {
                        Op::Post { author, content, .. } => {
                            max_id += 1;
                            ins_post.execute(params![max_id, author, content, created]).expect("insert post");
                            let prefix = post_prefix(max_id, author, content, &created);
                            let mut body = prefix.clone();
                            body.push_str("0}");
                            results.push(Res::Post(Bytes::from(body)));
                            new_posts.push((max_id, prefix));
                        }
                        Op::Like { post_id, user, .. } => {
                            let pid = *post_id;
                            if pid > max_id {
                                results.push(Res::Like(None));
                                continue;
                            }
                            let current = pending.get(&pid).copied().unwrap_or_else(|| {
                                st.likes.get(pid as usize).copied().unwrap_or(0)
                            });
                            let key = (pid, user.clone().into_boxed_str());
                            if set.contains(&key) {
                                results.push(Res::Like(Some(current)));
                            } else {
                                ins_like.execute(params![pid, user]).expect("insert like");
                                set.insert(key);
                                let n = current + 1;
                                pending.insert(pid, n);
                                like_inc.push((pid, n));
                                results.push(Res::Like(Some(n)));
                            }
                        }
                    }
                }
            }
            drop(st);
            tx.commit().expect("commit");
        }
        {
            let mut st = shared.state.lock().unwrap();
            let mut touched = !new_posts.is_empty();
            if st.likes.len() <= max_id as usize {
                st.likes.resize(max_id as usize + 1024, 0);
            }
            let min_feed = st.recent.front().map(|p| p.id).unwrap_or(i64::MAX);
            for (pid, n) in like_inc.iter() {
                st.likes[*pid as usize] = *n;
                if *pid >= min_feed {
                    touched = true;
                }
            }
            for (id, prefix) in new_posts.drain(..) {
                st.recent.push_back(FeedPost { id, prefix });
                if st.recent.len() > FEED_SIZE {
                    st.recent.pop_front();
                }
            }
            if touched {
                st.rebuild_feed();
            }
        }
        for (op, res) in batch.drain(..).zip(results.into_iter()) {
            match (op, res) {
                (Op::Post { tx, .. }, Res::Post(b)) => {
                    let _ = tx.send(b);
                }
                (Op::Like { tx, .. }, Res::Like(r)) => {
                    let _ = tx.send(r);
                }
                _ => unreachable!(),
            }
        }
    }
}

fn json(status: StatusCode, body: Bytes) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(body));
    *r.status_mut() = status;
    r.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    r
}

fn err(status: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    json(status, Bytes::from(format!("{{\"error\":{}}}", serde_json::to_string(msg).unwrap())))
}

fn valid_name(v: Option<&Value>) -> Option<&str> {
    match v {
        Some(Value::String(s))
            if !s.is_empty() && s.len() <= 32 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') =>
        {
            Some(s)
        }
        _ => None,
    }
}

fn is_py_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

async fn read_json(req: Request<Incoming>) -> Result<serde_json::Map<String, Value>, &'static str> {
    let ct_ok = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            let m = v.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
            m == "application/json" || (m.starts_with("application/") && m.ends_with("+json"))
        })
        .unwrap_or(false);
    if !ct_ok {
        return Err("Expected Content-Type application/json");
    }
    if let Some(len) = req.headers().get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()) {
        if len > MAX_BODY as u64 {
            return Err("Request body too large");
        }
    }
    let body = Limited::new(req.into_body(), MAX_BODY)
        .collect()
        .await
        .map_err(|_| "Request body too large")?
        .to_bytes();
    match serde_json::from_slice::<Value>(&body) {
        Ok(Value::Object(m)) => Ok(m),
        _ => Err("Expected a JSON object"),
    }
}

async fn handle(shared: Arc<Shared>, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let close = !req.headers().contains_key(hyper::header::USER_AGENT);
    let path = req.uri().path();
    let mut resp = match path {
        "/feed" | "/health" if req.method() == Method::GET || req.method() == Method::HEAD => {
            if path == "/health" {
                json(StatusCode::OK, Bytes::from_static(b"{\"status\":\"ok\"}"))
            } else {
                let b = shared.state.lock().unwrap().feed.clone();
                json(StatusCode::OK, b)
            }
        }
        "/posts" if req.method() == Method::POST => match read_json(req).await {
            Err(e) => err(StatusCode::BAD_REQUEST, e),
            Ok(m) => {
                let author = valid_name(m.get("author"));
                let content = match m.get("content") {
                    Some(Value::String(s)) => {
                        let n = s.chars().count();
                        if (1..=280).contains(&n) && !s.chars().all(is_py_space) { Some(s) } else { None }
                    }
                    _ => None,
                };
                match (author, content) {
                    (None, _) => err(StatusCode::BAD_REQUEST, "author must match [a-zA-Z0-9_]{1,32}"),
                    (_, None) => err(StatusCode::BAD_REQUEST, "content must contain 1..280 characters and not be blank"),
                    (Some(a), Some(c)) => {
                        let (tx, rx) = oneshot::channel();
                        let _ = shared.writer.send(Op::Post { author: a.to_owned(), content: c.to_owned(), tx });
                        match rx.await {
                            Ok(b) => json(StatusCode::CREATED, b),
                            Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "write failed"),
                        }
                    }
                }
            }
        },
        "/like" if req.method() == Method::POST => match read_json(req).await {
            Err(e) => err(StatusCode::BAD_REQUEST, e),
            Ok(m) => {
                let pid = match m.get("post_id") {
                    Some(Value::Number(n)) => n.as_i64().filter(|v| *v >= 1),
                    _ => None,
                };
                match (pid, valid_name(m.get("user"))) {
                    (Some(pid), Some(user)) => {
                        let (tx, rx) = oneshot::channel();
                        let _ = shared.writer.send(Op::Like { post_id: pid, user: user.to_owned(), tx });
                        match rx.await {
                            Ok(Some(n)) => json(
                                StatusCode::OK,
                                Bytes::from(format!("{{\"post_id\":{},\"likes\":{},\"liked\":true}}", pid, n)),
                            ),
                            Ok(None) => err(StatusCode::NOT_FOUND, "Post not found"),
                            Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "write failed"),
                        }
                    }
                    _ => err(StatusCode::BAD_REQUEST, "Expected a positive integer post_id and a valid user"),
                }
            }
        },
        "/feed" | "/health" | "/posts" | "/like" => err(StatusCode::METHOD_NOT_ALLOWED, "Method not allowed"),
        _ => err(StatusCode::NOT_FOUND, "Not found"),
    };
    if close {
        resp.headers_mut().insert(hyper::header::CONNECTION, hyper::header::HeaderValue::from_static("close"));
    }
    Ok(resp)
}

fn main() {
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8080);
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let _ = std::fs::create_dir_all(&data_dir);
    let db = open_db(&std::path::Path::new(&data_dir).join("social.sqlite3"));
    let (state, set, max_id) = load(&db);
    let (wtx, wrx) = mpsc::channel();
    let shared = Arc::new(Shared { state: Mutex::new(state), writer: wtx });
    {
        let shared = shared.clone();
        std::thread::spawn(move || writer_loop(db, shared, wrx, set, max_id));
    }
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(serve(shared, port));
}

async fn serve(shared: Arc<Shared>, port: u16) {
    let sock = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
    sock.set_reuse_address(true).unwrap();
    sock.set_nonblocking(true).unwrap();
    sock.bind(&std::net::SocketAddr::from(([0, 0, 0, 0], port)).into()).unwrap();
    sock.listen(65535).unwrap();
    let listener = tokio::net::TcpListener::from_std(sock.into()).unwrap();
    let open = Arc::new(AtomicUsize::new(0));

    tokio::spawn(async {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).unwrap();
        let mut int = signal(SignalKind::interrupt()).unwrap();
        tokio::select! { _ = term.recv() => {}, _ = int.recv() => {} }
        std::process::exit(0);
    });

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(2)).await;
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let n = open.fetch_add(1, Ordering::Relaxed);
        let keep = n < KEEPALIVE_LIMIT;
        let shared = shared.clone();
        let open = open.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req| handle(shared.clone(), req));
            let _ = http1::Builder::new()
                .keep_alive(keep)
                .serve_connection(TokioIo::new(stream), svc)
                .await;
            open.fetch_sub(1, Ordering::Relaxed);
        });
    }
}
