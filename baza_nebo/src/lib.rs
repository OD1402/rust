#[allow(unused_imports)]
use anyhow::{anyhow, bail, Error, Result};
#[allow(unused_imports)]
use tracing::{debug, error, info, span, trace, warn, Level};

use axum::{
    routing::{get},
    Extension, Router,
};
use common_macros4::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::{services::ServeDir, trace::TraceLayer};

mod endpoint;

declare_settings! {
    keep_alive_secs: u64,
}

pub async fn server(
    port: u16,
    op_mode: op_mode::OpMode,
    pkg_name: &'static str,
    pkg_version: &'static str,
) -> Result<()> {
    let shared_state = Arc::new(tokio::sync::RwLock::new(AppState {
        pkg_name,
        pkg_version,
        op_mode,
    }));
    let app = endpoint::router()
        .layer(Extension(shared_state.clone()))
        .nest_service("/assets", ServeDir::new("assets"))
        .with_state(shared_state);
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    info!("will start web server at PORT={port}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app.layer(TraceLayer::new_for_http()))
        .await
        .map_err(|err| anyhow!(err))
}

pub type SharedState = Arc<tokio::sync::RwLock<AppState>>;

pub struct AppState {
    pkg_name: &'static str,
    pkg_version: &'static str,
    op_mode: op_mode::OpMode,
}
