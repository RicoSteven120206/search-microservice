use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use axum::{
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};

use serde::{Deserialize, Serialize};

const K1: f64 = 1.2;
const B: f64 = 0.75;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Doc {
    id: u64,
    name: String,
    #[serde(default)]
    description: String,
}

struct StoredDoc {
    doc: Doc,
    len: u32,
    terms: Vec<String>,
}

#[derive(Default)]
struct Index {
    docs: HashMap<u64, StoredDoc>,
    postings: HashMap<String, HashMap<u64, u32>>,
    total_len: u64,
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

impl Index {
    fn remove(&mut self, id: u64) {
        if let Some(old) = self.docs.remove(&id) {
            self.total_len -= old.len as u64;
            for term in old.terms {
                if let Some(list) = self.postings.get_mut(&term) {
                    list.remove(&id);
                    if list.is_empty() {
                        self.postings.remove(&term);
                    }
                }
            }
        }
    }

    fn add(&mut self, doc: Doc) {
        self.remove(doc.id);
        let tokens = tokenize(&format!("{} {}", doc.name, doc.description));
        let mut tf: HashMap<&str, u32> = HashMap::new();
        for t in &tokens {
            *tf.entry(t.as_str()).or_insert(0) += 1;
        }
        let mut terms = Vec::with_capacity(tf.len());
        for (term, count) in tf {
            self.postings.entry(term.to_string()).or_default().insert(doc.id, count);
            terms.push(term.to_string())
        }
        self.total_len += tokens.len() as u64;
        self.docs.insert(doc.id, StoredDoc { len: tokens.len() as u32, doc, terms });
    }

    fn search(&self, query: &str, limit: usize) -> Vec<Hit> {
        let n = self.docs.len() as f64;
        if n == 0.0 {
            return vec![];
        }
        let avgdl = self.total_len as f64 / n;
        let mut scores: HashMap<u64, f64> = HashMap::new();

        for term in tokenize(query) {
            let Some(list) = self.postings.get(&term) else { continue };
            let df = list.len() as f64;
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
            for (&doc_id, &tf) in list {
                let dl = self.docs[&doc_id].len as f64;
                let tf = tf as f64;
                let score = idf * (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / avgdl));
                *scores.entry(doc_id).or_insert(0.0) += score;
            }
        }

        let mut ranked: Vec<(u64, f64)> = scores.into_iter().collect();
        let cmp = |a: &(u64, f64), b: &(u64, f64)| b.1.partial_cmp(&a.1).unwrap().then(a.0.cmp(&b.0));
        if ranked.len() > limit {
            ranked.select_nth_unstable_by(limit, cmp);
            ranked.truncate(limit);
        }
        ranked.sort_by(cmp);
        ranked
            .into_iter()
            .map(|(id, score)| Hit { doc: self.docs[&id].doc.clone(), score })
            .collect()
    }
}

#[derive(Serialize)]
struct Hit {
    #[serde(flatten)]
    doc: Doc,
    score: f64,
}

#[derive(Serialize)]
struct SearchResponse {
    total: usize,
    hits: Vec<Hit>,
}

#[derive(Deserialize)]
struct SearchParams {
    q: String,
    limit: Option<usize>,
}

type Shared = Arc<RwLock<Index>>;

async fn health() -> &'static str {
    "anton suka makan cabai"
}

async fn index_one(State(s): State<Shared>, Json(doc): Json<Doc>) -> StatusCode {
    s.write().unwrap().add(doc);
    StatusCode::NO_CONTENT
}

async fn index_bulk(State(s): State<Shared>, Json(docs): Json<Vec<Doc>>) -> StatusCode {
    let mut idx = s.write().unwrap();
    for d in docs {
        idx.add(d);
    }
    StatusCode::NO_CONTENT
}

async fn search(State(s): State<Shared>, Query(p): Query<SearchParams>) -> Json<SearchResponse> {
    let hits = s.read().unwrap().search(&p.q, p.limit.unwrap_or(10));
    Json(SearchResponse { total: hits.len(), hits })
}

#[tokio::main]
async fn main() {
    let state: Shared = Arc::new(RwLock::new(Index::default()));
    let app = Router::new()
        .route("/health", get(health))
        .route("/index", post(index_one))
        .route("/index/bulk", post(index_bulk).layer(DefaultBodyLimit::max(64 * 1024 * 1024)))
        .route("/search", get(search))
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8082".into());
    let addr: SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
    println!("search-rs listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}