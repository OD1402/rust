use super::*;

pub type SaveResult = Result<()>;
pub async fn save(content_shared: &elastic_scan_specific::ElasticContentShared) -> SaveResult {
    {
        let content_shared = &mut *content_shared.write().unwrap();
        content_shared.save()?;
        if content_shared.need_finish {
            content_shared.finish()?;
        }
    }
    Ok(())
}
pub fn save_sync(
    res: SaveResult,
    content_shared: elastic_scan_specific::ElasticContentShared,
    next_fetch: Option<crate::pasitos::pasos::elastic::Arg>,
) -> Result<()> {
    if let Err(err) = res {
        panic!("{}", err);
    }
    if !content_shared.read().unwrap().bunches_to_save.is_empty() {
        pasitos!(store push_back Save {
            content_shared,
            next_fetch: None,
        });
    }
    if let Some(crate::pasitos::pasos::elastic::Arg::Fetch {
        request,
        content,
        mode,
    }) = next_fetch
    {
        pasitos!(elastic push_back Fetch {
            request,
            content,
            mode,
        });
    }
    Ok(())
}
