use super::*;

pub async fn publish_deleted(
    channel: &rmq::Channel,
    queue: &str,
    url: &str,
    deleted: &[i64],
    run_for: &str,
    request: CheckRequest,
    worker_name: String,
) -> Result<()> {
    let avito_ids: HashSet<u64> = deleted.iter().map(|i| (*i).try_into().unwrap()).collect();
    let source: Vec<CheckResponseParamsSourceItem> = request
        .params
        .source
        .data
        .into_iter()
        .filter(|i| {
            i.external_id()
                .map(|external_id| avito_ids.contains(&external_id))
                .unwrap_or(false)
        })
        .map(|i| i.into())
        .collect();
    let params = CheckResponseParamsParams {
        dip_module_id: 13,
        file_name: "avito-do-not-pub".to_owned(),
    };
    let len = source.len();
    let payload = serde_json::to_string_pretty(&CheckResponse {
        worker_name,
        params: CheckResponseParams { params, source },
    })?;
    rmq::basic_publish_str(channel, queue, payload)
        .await
        .context(format!("{run_for}: basic_publish to {queue} @ {url}"))?;
    info!("publish_deleted: {len}");

    Ok(())
}
