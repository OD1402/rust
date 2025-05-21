use super::*;
use crate::AppState;

use axum::{extract::Query, response::IntoResponse, Extension, Json};

mod about;
pub use about::*;

mod adv;
pub use adv::*;

// mod index;
// pub use index::*;

// mod create_topic;
// pub use create_topic::*;

// mod push_message;
// pub use push_message::*;

// mod subscribe;
// pub use subscribe::*;

// mod config;
// pub use config::*;

pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/about", get(about))
        .route("/adv", get(adv))
    // .route("/", get(index))
    // .route("/topic/:name/create", get(create_topic))
    // .route("/topic/:name/push", get(push_message_by_get))
    // .route("/topic/:name/push", post(push_message_by_post))
    // .route("/topic/:name/subscribe", get(subscribe))
    // .route("/topic/:name/config", get(get_config))
    // .route("/topic/:name/config", post(set_config))
}
