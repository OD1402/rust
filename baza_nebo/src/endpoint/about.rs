use super::*;

pub async fn about(state: Extension<SharedState>) -> impl IntoResponse {
    let AppState {
        op_mode,
        pkg_name,
        pkg_version,
        ..
    } = &*state.read().await;
    format!("{op_mode} {pkg_name} {pkg_version}\n")
}
