use super::*;

use futures::StreamExt;

mod db;
mod dip;
mod dip_check_consume;
mod dip_publish;
mod scan;

pasitos!(fut_queue, run_for;
    init {
        let opt = (*OPT.read().unwrap()).clone().unwrap();

        if let Some(webdriver_port) = match &opt.cmd {
            Some(Command::ScanCards {webdriver_port, ..} | Command::ScanLists {webdriver_port, ..}) => Some(webdriver_port),
            _ => None,
        }{
            let webdriver_port = *webdriver_port;
            env_settings!(webdriver_port = webdriver_port);
            let webdriver = format!("http://localhost:{}", env_settings!(webdriver_port));
            let user_agent = settings!(user_agent).clone();
            will_did!(trace => "init_webdriver_client",
                init_webdriver_client(
                    &webdriver,
                    &user_agent
                ).await
            )?;
            let url = "TEST";
            // will_did!(trace => format!("goto({url})"), CLIENT.write().await.unwrap().as_mut().unwrap().goto(url).await)?;
            will_did!(trace => format!("goto({url})"), CLIENT.write().unwrap().as_mut().unwrap().goto(url).await)?;
            let url = settings!(ipify_url).clone();
            let try_limit = settings!(try_limit);
            // let ret = will_did!(trace => "ipify", fetch(CLIENT.write().await.unwrap().as_mut().unwrap(), &url, TryLimitFor::All(try_limit)).await)?;
            let ret = will_did!(trace => "ipify", fetch(CLIENT.write().unwrap().as_mut().unwrap(), &url, TryLimitFor::All(try_limit), FetchKind::Ipify).await)?;
            info!("{ret:?}");

            let url = format!("https://{}", settings!(avito.host));
            // will_did!(trace => format!("goto({url})"), CLIENT.write().await.unwrap().as_mut().unwrap().goto(&url).await)?;
            will_did!(trace => format!("goto({url})"), CLIENT.write().unwrap().as_mut().unwrap().goto(&url).await)?;
        }

        let start = std::time::Instant::now();

        match opt.cmd {
            None => {
                println!("no command");
            }
            Some(Command::ServeDipCheck { env }) => {
                pasitos!(dip_check_consume push_front Consume { env });
            }
            Some(Command::SendCards {
                to: send_to,
                ids,
                from,
                facet,
                page_limit,
                scan_deep_interval,
            }) => {
                let ids = if !ids.is_empty() {
                    Some(ids.clone())
                } else if let Some(file_path) = from {
                    use tokio::{
                        fs::File,
                        io::{AsyncBufReadExt, BufReader},
                    };
                    let file = File::open(&file_path)
                        .await
                        .map_err(|err| anyhow!("File::open({file_path:?}): {err}"))?;
                    let reader = BufReader::new(file);
                    let mut lines = reader.lines();
                    let mut ids = ids.clone();
                    while let Some(line) = lines.next_line().await? {
                        if let Ok(id) = line.parse::<i64>() {
                            ids.push(id)
                        }
                    }
                    Some(ids)
                } else {
                    None
                };
                if let Some(ids) = ids {
                    if !ids.is_empty() {
                        todo!();
                        // pasitos!(scan push_back Card {
                        //     ids,
                        //     send_to: *send_to,
                        //     output: *output,
                        //     facet: facet.clone(),
                        // });
                    } else {
                        bail!("no ids to send lists");
                    }
                } else if let Some(facet) = facet {
                    pasitos!(db push_back GetCardsToSend {
                        facet,
                        send_to,
                        page_limit,
                        scan_deep_interval: scan_deep_interval.clone(),
                    });
                } else {
                    bail!("-f | --facet must be specified");
                }
            },
            Some(Command::SendLists {
                to: send_to,
                // ids,
                // from,
                facet,
                page_limit,
                sub_cmd,
            }) => {
                    pasitos!(db push_back GetListsToSend {
                        facet,
                        send_to,
                        page_limit,
                        sub_cmd,
                    });
            },
            Some(Command::ScanCards {
                ids,
                from,
                send_to,
                output,
                facet,
                webdriver_port: _,
                follow_lists,
            }) => {
                let ids = if !ids.is_empty() {
                    Some(ids.clone())
                } else if let Some(file_path) = from {
                    use tokio::{
                        fs::File,
                        io::{AsyncBufReadExt, BufReader},
                    };
                    let file = File::open(&file_path)
                        .await
                        .map_err(|err| anyhow!("File::open({file_path:?}): {err}"))?;
                    let reader = BufReader::new(file);
                    let mut lines = reader.lines();
                    let mut ids = ids.clone();
                    while let Some(line) = lines.next_line().await? {
                        if let Ok(id) = line.parse::<i64>() {
                            ids.push(id)
                        }
                    }
                    Some(ids)
                } else {
                    None
                };
                if let Some(ids) = ids {
                    if !ids.is_empty() {
                        pasitos!(scan push_back Card {
                            ids,
                            send_to,
                            output,
                            facet,
                        });
                    } else {
                        bail!("no ids to scan cards");
                    }
                } else if let Some(facet) = facet {
                    pasitos!(db push_back GetCardsToScan {
                        facet,
                        send_to,
                        output,
                        follow_lists,
                    });
                } else {
                    bail!("either ids or facet MUST be specified to scan-cards");
                }
            }
            Some(Command::ScanLists {
                facet,
                page_limit,
                send_to,
                webdriver_port: _,
            }) => {
                info!("will scan lists for {}", facet);
                if page_limit.get() > 50 {
                    bail!("page_limit MUST be less or equal 50");
                }
                pasitos!(db push_back StartSession {
                    send_to,
                    page_limit,
                    facet,
                });
            }
        }
    }
    on_complete {
        info!(
            "{}, complete",
            arrange_millis::get(std::time::Instant::now().duration_since(start).as_millis()),
        );
        return Ok(());
    }
    on_next_end {
        if DID_TERMINATED.load(Ordering::Relaxed) {
            bail!("{}:{}: did terminated", file!(), line!());
        }
    }
    demoras {
        demora Stop({}) {
            pasitos!(stop);
        }
        demora ScanDiapPage({
            page: Option<pasitos::scan::Page>,
            accu: Arc<pasitos::db::ListAccu>,
            min_price: Option<i64>,
            facet: Facet,
            page_limit: NonZeroUsize,
            send_to: Option<Env>,
            try_count: usize,
            prices: HashMap<i64, usize>,
        }) {
            pasitos!(scan push_back DiapPage {
                page,
                accu,
                min_price,
                send_to,
                page_limit,
                facet,
                try_count,
                prices,
            });
        }
    }
    pasos stop {
        max_at_once: 1;
        paso Stop({
        }) -> ({
            need_stop: bool,
        }) {
            let need_stop = true;
        } => sync {
        }
    }

    pasos db {
        max_at_once: settings!(db).connection_max_count as usize;

        paso SelectDeleted ({
            env: Env,
            delivery_tag: u64,
            request: CheckRequest,
            offers_to_check: Vec<pasitos::db::OfferToCheck>,
        }) -> ( {
            res: pasitos::db::SelectDeletedResult,
            env: Env,
            delivery_tag: u64,
            request: CheckRequest,
        }) {
            let res = pasitos::db::select_deleted(offers_to_check).await;
        } => sync {
            pasitos::db::select_deleted_sync(res, env, delivery_tag, request)?;
        }

        paso SavePage ({
            created_at: chrono::DateTime<chrono::Utc>,
            scan_session_id: i32,
            min_price: Option<i64>,
            page_num: i16,
            items: Vec<serde_json::Value>,
            avito_count: i32,
        }) -> ( {
            res: pasitos::db::SavePageResult,
        }) {
            let res = pasitos::db::save_page(
                created_at,
                scan_session_id,
                min_price,
                page_num,
                items,
                avito_count,
            ).await;
        } => sync {
            pasitos::db::save_page_sync(res)?;
        }

        paso StartSession  ({
            facet: Facet,
            page_limit: NonZeroUsize,
            send_to: Option<Env>,
        }) -> ( {
            facet: Facet,
            page_limit: NonZeroUsize,
            send_to: Option<Env>,
            res: pasitos::db::StartSessionResult,
        }) {
            let res = pasitos::db::start_session(&facet).await;
        } => sync {
            pasitos::db::start_session_sync(res, facet, page_limit, send_to)?;
        }
        paso FinishSession ({
            session_id: i32,
            finished_at: chrono::DateTime<chrono::Utc>,
        }) -> ( {
            res: pasitos::db::FinishSessionResult,
        }) {
            let res = pasitos::db::finish_session(session_id, finished_at).await;
        } => sync {
            pasitos::db::finish_session_sync(res)?;
        }
        paso GetCardsToScan ({
            facet: Facet,
            send_to: Option<Env>,
            output: bool,
            follow_lists: bool,
        }) -> ( {
            res: pasitos::db::GetCardsToScanResult,
            facet: Facet,
            send_to: Option<Env>,
            output: bool,
        }) {
            let res = pasitos::db::get_cards_to_scan(&facet, !follow_lists).await;
        } => sync {
            pasitos::db::get_cards_to_scan_sync(res, facet, send_to, output)?;
        }

        paso GetListsToSend ({
            facet: Facet,
            send_to: Env,
            page_limit: NonZeroUsize,
            sub_cmd: CommandScanLists,
        }) -> ( {
            res: pasitos::db::GetListsToSendResult,
            facet: Facet,
            send_to: Env,
            page_limit: NonZeroUsize,
        }) {
            let res = pasitos::db::get_lists_to_send(&facet, sub_cmd).await;
        } => sync {
            pasitos::db::get_lists_to_send_sync(res, facet, send_to, page_limit)?;
        }

        paso GetCardsToSend ({
            facet: Facet,
            send_to: Env,
            page_limit: NonZeroUsize,
            scan_deep_interval: Option<String>,
        }) -> ( {
            res: pasitos::db::GetCardsToSendResult,
            facet: Facet,
            send_to: Env,
            page_limit: NonZeroUsize,
        }) {
            let res = pasitos::db::get_cards_to_send(&facet, true, scan_deep_interval).await;
        } => sync {
            pasitos::db::get_cards_to_send_sync(res, facet, send_to, page_limit)?;
        }

        paso SendLists ({
            ids: Vec<i32>,
            facet: Facet,
            send_to: Env,
            page_limit: NonZeroUsize,
        }) -> ( {
            res: pasitos::db::SendListsResult,
            facet: Facet,
            send_to: Env,
            page_limit: NonZeroUsize,
        }) {
            let res = pasitos::db::send_lists(ids, page_limit).await;
        } => sync {
            pasitos::db::send_lists_sync(res, facet, send_to, page_limit)?;
        }

        paso SendCards ({
            ids: Vec<i32>,
            facet: Facet,
            send_to: Env,
            page_limit: NonZeroUsize,
        }) -> ( {
            res: pasitos::db::SendCardsResult,
            facet: Facet,
            send_to: Env,
            page_limit: NonZeroUsize,
        }) {
            let res = pasitos::db::send_cards(ids, page_limit).await;
        } => sync {
            pasitos::db::send_cards_sync(res, facet, send_to, page_limit)?;
        }

        paso SaveCardJson ({
            created_at: chrono::DateTime<chrono::Utc>,
            card_json: serde_json::Value,
        }) -> ( {
            res: pasitos::db::SaveCardJsonResult,
        }) {
            let res = pasitos::db::save_card_json(created_at, card_json).await;
        } => sync {
            pasitos::db::save_card_json_sync(res)?;
        }
        paso SaveCardFailed ({
            created_at: chrono::DateTime<chrono::Utc>,
            id: i64,
            status: i16,
        }) -> ( {
            res: pasitos::db::SaveCardFailedResult,
        }) {
            let res = pasitos::db::save_card_failed(created_at, id, status).await;
        } => sync {
            pasitos::db::save_card_failed_sync(res)?;
        }
    }

    pasos dip {
        max_at_once: 1;
        paso Publish ({
            env: Env,
            facet: Facet,
            items: Vec<serde_json::Value>,
            and_then: pasitos::dip::PublishAndThen,
        }) -> ( {
            res: pasitos::dip::PublishResult,
            env: Env,
            facet: Facet,
            and_then: pasitos::dip::PublishAndThen,
        }) {
            let res = pasitos::dip::publish(&env, &facet, items).await;
        } => sync {
            pasitos::dip::publish_sync(res, env, facet, and_then)?;
        }
    }

    pasos scan {
        max_at_once: 1;

        paso Card ({
            ids: Vec<i64>,
            send_to: Option<Env>,
            output: bool,
            facet: Option<Facet>,
        }) -> ( {
            res: pasitos::scan::CardResult,
            send_to: Option<Env>,
            output: bool,
            facet: Option<Facet>,
        }) {
            let res = pasitos::scan::card(ids).await;
        } => sync {
            pasitos::scan::card_sync(
                res,
                send_to,
                output,
                facet,
            )?;
        }

        paso DiapPage ({
            page: Option<pasitos::scan::Page>,
            accu: Arc<pasitos::db::ListAccu>,
            min_price: Option<i64>,
            facet: Facet,
            page_limit: NonZeroUsize,
            send_to: Option<Env>,
            try_count: usize,
            prices: HashMap<i64, usize>,
        }) -> ( {
            res: pasitos::scan::DiapPageResult,
            page: Option<pasitos::scan::Page>,
            accu: Arc<pasitos::db::ListAccu>,
            min_price: Option<i64>,
            facet: Facet,
            page_limit: NonZeroUsize,
            send_to: Option<Env>,
            try_count: usize,
        }) {
            let res = pasitos::scan::diap_page(&page, &accu, page_limit, min_price, &facet, prices).await;
        } => sync {
            pasitos::scan::diap_page_sync(
                res,
                page,
                accu,
                min_price,
                facet,
                page_limit,
                send_to,
                try_count,
            )?;
        }
    }

// ==========================================================================
// ======================= DIP_CHECK ========================================

    pasos dip_check_consume {
        max_at_once: 1;
        paso Consume ({
            env: Env,
        }) -> ({
            env: Env,
            delivery_tag: u64,
            request: Option<CheckRequest>,
        }) {
            let prefetch_count = 1;
            let queue = settings!(dip.queue_check).clone();
            let url = settings!(dip).url(&env).to_owned();
            let consumer = rmq::get!(consumer =>
                CHECK_CONSUMER,
                prefetch_count,
                queue,
                queue,
                CHECK_CHANNEL,
                CHECK_CONN,
                RMQ_POOLS,
                url,
                run_for
            );
            let (request, delivery_tag) = pasitos::dip_check_consume::consume_dip_check_request(consumer, run_for).await?;
        } => sync {
            if let Some(request) = request {
                let offers_to_check: Vec<pasitos::db::OfferToCheck> = request.params.source.data.iter().filter_map(|item| match item.try_into() {
                    Err(err) => { error!("{}", err); None },
                    Ok(offer) => Some(offer),
                }).collect();
                pasitos!(db push_front SelectDeleted {
                    env,
                    delivery_tag,
                    request,
                    // dip_check_facet,
                    offers_to_check,
                });
            } else {
                pasitos!(dip_check_consume push_back Consume{ env });
            }
        }
    }

    pasos dip_publish {
        max_at_once: 1;
        paso Deleted ({
            env: Env,
            delivery_tag: u64,
            deleted: Vec<i64>,
            request: CheckRequest,
        }) -> ( {
            env: Env,
            delivery_tag: u64,
        }) {
            let url = settings!(dip).url(&env).to_owned();
            let channel = rmq::get!(channel => DIP_CHANNEL, DIP_CONN, RMQ_POOLS, url, run_for);

            let worker_name = settings!(dip.worker_name).clone();
            let queue = settings!(dip.queue).clone();

            let url = settings!(dip).url(&env).to_owned();
            pasitos::dip_publish::publish_deleted(&channel, &queue, &url, &deleted, &run_for, request, worker_name).await?;
        } => sync {
            pasitos!(dip_check_ack push_back Ack{env, delivery_tag});
        }
    }

    pasos dip_check_ack {
        max_at_once: 1;
        paso Ack ({
            env: Env,
            delivery_tag: u64,
        }) -> ({
            env: Env,
        }) {
            rmq::basic_ack((*CHECK_CHANNEL.read().unwrap()).as_ref().unwrap(), delivery_tag)
                .await
                .context(format!("{run_for} {delivery_tag}"))?;
        } => sync {
            pasitos!(dip_check_consume push_back Consume{env});
        }
    }
);

lazy_static::lazy_static! {
    // https://www.cloudamqp.com/blog/2017-12-29-part1-rabbitmq-best-practice.html#separate-connections-for-publisher-and-consumer
    pub static ref CHECK_CONN: RwLock<Option<Arc<rmq::Connection>>> = RwLock::new(None);
    pub static ref CHECK_CHANNEL: RwLock<Option<rmq::Channel>> = RwLock::new(None);
    pub static ref CHECK_CONSUMER: RwLock<Option<rmq::Consumer>> = RwLock::new(None);
    pub static ref DIP_CHANNEL: RwLock<Option<rmq::Channel>> = RwLock::new(None);
    pub static ref DIP_CONN: RwLock<Option<Arc<rmq::Connection>>> = RwLock::new(None);
    pub static ref RMQ_POOLS: RwLock<HashMap<String, rmq::Pool>> = RwLock::new(HashMap::new());
    // pub static ref LOAD_REQUEST_RECEIVER: RwLock<Option<tokio::sync::mpsc::Receiver<LoadRequest>>> = RwLock::new(None);
    // pub static ref LOAD_REQUEST_SENDER: RwLock<Option<tokio::sync::mpsc::Sender<Option<LoadRequest>>>> = RwLock::new(None);
    // pub static ref SAVE_REQUEST_RECEIVER: RwLock<Option<tokio::sync::mpsc::Receiver<SaveRequest>>> = RwLock::new(None);
    pub static ref SAVE_REQUEST_SENDER: RwLock<Option<tokio::sync::mpsc::Sender<Option<SaveRequest>>>> = RwLock::new(None);
    pub static ref WILL_READ_HOLDER: RwLock<HashSet<PathBuf>> = RwLock::new(HashSet::new());
    // pub static ref WILL_WRITE_FILE: RwLock<HashSet<PathBuf>> = RwLock::new(HashSet::new());
    // pub static ref DELAYED_GZ_ENCODED_SAVES: RwLock<HashMap<PathBuf, Vec<u8>>> = RwLock::new(HashMap::new());
    pub static ref IS_STOPPED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    // pub static ref DELETED_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
}

#[derive(Debug)]
pub struct SaveRequest {
    //     file_path: PathBuf,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CheckRequest {
    pub params: CheckRequestParams,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CheckRequestParams {
    // pub params: CheckRequestParamsParams,
    pub source: CheckRequestParamsSource,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CheckRequestParamsSource {
    pub data: Vec<CheckRequestParamsSourceDataItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CheckRequestParamsSourceDataItem {
    pub project_id: usize,
    pub guid: String,
    pub checking: bool,
    pub url: Option<String>,
}

impl CheckRequestParamsSourceDataItem {
    fn external_id(&self) -> Result<u64> {
        if let Some(url) = &self.url {
            Self::external_id_helper(url)
        } else {
            bail!("no url");
        }
    }
    fn external_id_helper(s: &str) -> Result<u64> {
        const RE_STR: &str = r"(\d+)$|(\d+)\?";
        lazy_static::lazy_static! {
            pub static ref RE: Regex = Regex::new(RE_STR).unwrap();
        }

        if let Some(caps) = RE.captures(s) {
            caps.get(1)
                .unwrap_or_else(|| caps.get(2).unwrap())
                .as_str()
                .parse::<u64>()
                .map_err(|err| anyhow!("{}:{}: {err}", file!(), line!()))
        } else {
            bail!("failed to match {:?} against {:?}", s, RE_STR);
        }
    }
}

#[cfg(test)]
mod tests {

    #[allow(unused_imports)]
    use anyhow::{anyhow, bail, Context, Error, Result};
    #[allow(unused_imports)]
    use log::{debug, error, info, trace, warn};

    use super::*;

    #[tokio::test]
    async fn test_external_id() -> Result<()> {
        dotenv::dotenv().context(format!(
            "file .env at {:?}",
            std::env::current_dir().unwrap()
        ))?;
        let _ = pretty_env_logger::try_init_timed();
        let url = "https://www.avito.ru/moskva/kvartiry/3-k._kvartira_110_m_712_et._2296409216?context=H4sIAAAAAAAA_0q0MrSqLrYytFKqULIutjI2tFIqsUhOK0urSE9LNDJJrMxLNK3MLC8uL83NyDAxyk8yUrKuBQQAAP__dO9wkjUAAAA";
        // debug!(
        //     "{} => {:?}",
        //     url,
        //     CheckRequestParamsSourceDataItem::external_id_helper(&url)
        // );
        assert_eq!(
            2296409216,
            CheckRequestParamsSourceDataItem::external_id_helper(&url)?
        );

        let url = "https://www.avito.ru/moskva/kvartiry/3-k._kvartira_110_m_712_et._2296409216";
        // debug!(
        //     "{} => {:?}",
        //     url,
        //     CheckRequestParamsSourceDataItem::external_id_helper(&url)
        // );
        assert_eq!(
            2296409216,
            CheckRequestParamsSourceDataItem::external_id_helper(&url)?
        );

        Ok(())
    }
}

#[derive(Serialize, Debug)]
pub struct CheckResponse {
    #[serde(rename = "workerName")]
    pub worker_name: String,
    pub params: CheckResponseParams,
}

#[derive(Serialize, Debug)]
pub struct CheckResponseParams {
    pub params: CheckResponseParamsParams,
    pub source: Vec<CheckResponseParamsSourceItem>,
}

#[derive(Serialize, Debug)]
pub struct CheckResponseParamsParams {
    pub dip_module_id: usize,
    #[serde(rename = "fileName")]
    pub file_name: String,
}

#[derive(Serialize, Debug)]
pub struct CheckResponseParamsSourceItem {
    pub project_id: usize,
    pub guid: String,
    pub external_url: String,
    pub external_id: String,
    pub status: String,
}

impl_from!(CheckRequestParamsSourceDataItem => CheckResponseParamsSourceItem,
    anyhow::Error, from,
    {
        // let external_url = match from.clone().url {
        //     Some(url) => url,
        //     None => {
        //         todo!("from: {:#?}", from);
        //     }
        // };
        match (from.url.clone(), from.external_id()) {
            (Some(external_url), Ok(external_id)) =>
                Ok(Self {
                    external_id: external_id.to_string(),
                    project_id: from.project_id,
                    guid: from.guid,
                    external_url,
                    status: "объявление закрыто".to_owned(),
                }),
            (_, Err(err)) => Err(err),
            (None, _) => Err(anyhow!("no url")),
        }
    }
);
