use std::path::PathBuf;
use std::sync::Arc;

use actix_cors::Cors;
use actix_files::Files;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use serde_json::Value;

#[derive(Clone)]
struct AppState {
    manifest: Arc<Value>,
}

async fn manifest_handler(state: web::Data<AppState>) -> impl Responder {
    let node_count = state
        .manifest
        .get("nodes")
        .and_then(|n| n.as_array())
        .map(|nodes| nodes.len())
        .unwrap_or_default();

    log::info!("serving manifest with {node_count} nodes");
    HttpResponse::Ok().json(&*state.manifest)
}

async fn health_handler() -> impl Responder {
    log::info!("health check requested");
    HttpResponse::Ok().finish()
}

#[derive(Debug, Clone)]
pub struct BackendConfig {
    pub manifest: PathBuf,
    pub addr: String,
    pub static_dir: Option<PathBuf>,
}

pub fn init_logging() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .try_init();
}

pub async fn run_backend(cfg: BackendConfig) -> std::io::Result<()> {
    let manifest_data = std::fs::read_to_string(&cfg.manifest).map_err(|err| {
        log::error!(
            "failed to read manifest file at '{}': {}",
            cfg.manifest.display(),
            err
        );
        err
    })?;

    let manifest_json: Value = serde_json::from_str(&manifest_data).map_err(|err| {
        log::error!(
            "failed to parse manifest file at '{}': {}",
            cfg.manifest.display(),
            err
        );
        err
    })?;

    let state = web::Data::new(AppState {
        manifest: Arc::new(manifest_json),
    });

    let static_dir = cfg.static_dir.clone();

    log::info!(
        "starting server on {} serving manifest {}",
        cfg.addr,
        cfg.manifest.display()
    );

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        let mut app = App::new()
            .wrap(Logger::default())
            .wrap(cors)
            .app_data(state.clone())
            .service(web::scope("/api").route("/manifest", web::get().to(manifest_handler)))
            .route("/healthz", web::get().to(health_handler));

        if let Some(dir) = static_dir.clone() {
            app = app.service(
                Files::new("/", dir)
                    .prefer_utf8(true)
                    .index_file("index.html"),
            );
        }

        app
    })
    .bind(cfg.addr)?
    .run()
    .await
}
