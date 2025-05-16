use super::*;
use crate::AppState;

use axum::{extract::Query, response::IntoResponse, Extension};

mod about;
pub use about::*;

mod feed;
pub use feed::*;

pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/about", get(about))
        .route("/feed", get(feed))
}
