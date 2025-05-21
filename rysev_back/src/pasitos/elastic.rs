use super::*;

use elastic_scan_specific::ElasticContent;

pub type FetchResult = (
    ElasticContent,
    ElasticRequest,
    Result<ElasticRequestFetchRet>,
);
pub async fn fetch(content: ElasticContent, request: ElasticRequest) -> FetchResult {
    request.fetch(content).await
}
pub fn fetch_sync(res: FetchResult, mode: ExportFor) -> Result<()> {
    let (content, request, res) = res;
    match res {
        Err(err) => {
            error!("{}", err);
            let duration = std::time::Duration::from_secs(settings!(elastic.error_timeout_secs));
            pasitos!(delay ExportRetryFetch { request, content, mode } for duration );
        }
        Ok(ElasticRequestFetchRet::Continue) => {
            if settings!(elastic.fetch_once).unwrap_or(false) {
                content.shared.write().unwrap().need_finish = true;
                pasitos!(store push_back Save {
                    content_shared: content.shared,
                    next_fetch: None,
                });
            } else if content.shared.read().unwrap().bunches_to_save.len()
                >= settings!(store.bunches_to_save_max_len)
            {
                pasitos!(store push_back Save {
                    content_shared: content.shared.clone(),
                    next_fetch: Some(crate::pasitos::pasos::elastic::Arg::Fetch {
                        request,
                        content,
                        mode,
                    }),
                });
            } else {
                pasitos!(store push_back Save {
                    content_shared: content.shared.clone(),
                    next_fetch: None,
                });
                pasitos!(elastic push_back Fetch {
                    request,
                    content,
                    mode,
                });
            }
        }
        Ok(ElasticRequestFetchRet::Break) => {
            if std::sync::Arc::strong_count(&content.shared) == 1 {
                content.shared.write().unwrap().need_finish = true;
                pasitos!(store push_back Save {
                    content_shared: content.shared,
                    next_fetch: None,
                });
            }
        }
    }
    Ok(())
}
