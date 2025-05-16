use super::*;

pub async fn consume_dip_check_request(
    mut consumer: rmq::Consumer,
    run_for: String,
) -> Result<(Option<CheckRequest>, u64)> {
    let delivery = loop {
        if let Some(consumer_next) = consumer.next().await {
            match consumer_next {
                Err(err) => error!("{}: {:#?}", run_for, err),
                Ok((_channel, delivery)) => {
                    break delivery;
                }
            }
        } else {
            error!("failed to {}", run_for);
        }
    };

    let rmq::Delivery {
        data, delivery_tag, ..
    } = delivery;

    let s = std::str::from_utf8(&data)?;

    // let mut req: CheckRequest = serde_json::from_str(s)?;
    let req: CheckRequest = serde_json::from_str(s).map_err(|err| anyhow!("{err}:\n{s}"))?;
    debug!("got {} items in CheckRequest", req.params.source.data.len());
    let len = req.params.source.data.len();
    // req.params.source.data.retain(|i| i.url.is_some());
    if len != req.params.source.data.len() {
        debug!(
            "retained {} items in CheckRequest",
            req.params.source.data.len()
        );
    }
    let request = if req.params.source.data.is_empty() {
        None
    } else {
        Some(req)
    };
    Ok((request, delivery_tag))
}
