#[allow(unused_imports)]
use anyhow::{anyhow, bail, Context, Error, Result};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

// https://docs.rs/built/0.5.1/built/index.html
pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
use built_info::*;
use common_macros2::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use structopt::StructOpt;

mod common;
mod pasitos;
use common::*;

mod avito;
use avito::*;

#[tokio::main]
async fn main() -> Result<()> {
    main_helper().await
}

declare_env_settings! {
    settings_toml_path: std::path::PathBuf,
    webdriver_port: u16,
}

declare_settings! {
    db: pool_db::PoolDbSettings,
    dip: SettingsDip,
    avito: SettingsAvito,
    user_agent: String,
    connect_timeout_millis: usize,
    list_respite_millis: u64,
    card_respite_millis: u64,
    ipify_respite_millis: u64,
    recovery_millis: u64,
    try_limit: usize,
    change_ip_url: Option<String>,
    ipify_url: String,
    page_limit: usize,
    page_count_max: usize,
    data_root: std::path::PathBuf,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SettingsDip {
    pub worker_name: String,
    pub queue: String,
    pub queue_check: String,
    url: String,
    url_alpha: String,
    url_export: String,
}

impl SettingsDip {
    pub fn url(&self, env: &Env) -> &str {
        match env {
            Env::Alpha => &self.url_alpha,
            Env::Stable => &self.url,
            Env::Export => &self.url_export,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SettingsAvito {
    pub key: String,
    pub host: String,
    pub list_api: String,
    pub card_api: String,
    pub context: Option<String>,
    pub diap_page_try_count_limit: usize,
    pub diap_page_try_delay_millis: u64,
    pub page_num_max: usize,
}

#[derive(Debug, Clone, StructOpt)]
pub enum Command {
    ScanLists {
        #[structopt(short = "p", long)]
        webdriver_port: Option<u16>,

        #[structopt(short = "f", long)]
        facet: Facet,

        #[structopt(short = "l", default_value = "50")]
        page_limit: NonZeroUsize,

        #[structopt(long)]
        send_to: Option<Env>,
    },
    ScanCards {
        #[structopt(short = "p", long)]
        webdriver_port: Option<u16>,

        ids: Vec<i64>,
        #[structopt(short = "i", long)]
        from: Option<PathBuf>,

        #[structopt(short = "f", long)]
        facet: Option<Facet>,

        /// Use unfinished session to get cards to scan
        #[structopt(short = "l", long)]
        follow_lists: bool,

        #[structopt(long)]
        send_to: Option<Env>,

        #[structopt(short = "o", long)]
        output: bool,
    },
    SendCards {
        #[structopt(long)]
        to: Env,

        ids: Vec<i64>,
        #[structopt(short = "i", long)]
        from: Option<PathBuf>,

        #[structopt(short = "f", long)]
        facet: Option<Facet>,

        #[structopt(short = "l", default_value = "50")]
        page_limit: NonZeroUsize,

        #[structopt(short = "d")]
        scan_deep_interval: Option<String>,
    },
    SendLists {
        #[structopt(long)]
        to: Env,
        #[structopt(short = "f", long)]
        facet: Facet,

        #[structopt(short = "l", default_value = "50")]
        page_limit: NonZeroUsize,

        #[structopt(subcommand)]
        sub_cmd: CommandScanLists,
    },
    ServeDipCheck {
        #[structopt(default_value = "stable")]
        env: Env,
    },
}

#[derive(Debug, StructOpt, Clone, Copy)]
pub enum CommandScanLists {
    All,
    Rel {
        #[structopt(long)]
        to: Option<u32>,
    },
}

#[derive(Debug, strum::Display, strum::EnumIter, Clone, Copy, Hash, PartialEq, Eq)]
pub enum Env {
    #[strum(serialize = "stable")]
    Stable,
    #[strum(serialize = "alpha")]
    Alpha,
    #[strum(serialize = "export")]
    Export,
}
// use std::str::FromStr;
common_macros2::r#impl!(FromStr for Env; strum);

lazy_static::lazy_static! {
    // pub static ref JSON_TO_SEND: RwLock<HashMap<Env, VecDeque<serde_json::Value>>> = RwLock::new(HashMap::new());
    pub static ref EVALUATE_COUNT_TILL_RELOAD_BROWSER: AtomicUsize = AtomicUsize::new(0);
    pub static ref EVALUATE_COUNT_TILL_CHANGE_IP: AtomicUsize = AtomicUsize::new(0);
    // pub static ref CLIENT: tokio::sync::RwLock<Option<fantoccini::Client>> = tokio::sync::RwLock::new(None);
    pub static ref CLIENT: RwLock<Option<fantoccini::Client>> = RwLock::new(None);
    pub static ref DID_TERMINATED: AtomicBool = AtomicBool::new(false);
    pub static ref RMQ_POOLS: RwLock<HashMap<String, rmq::Pool>> = RwLock::new(HashMap::new());
    pub static ref DIP_CHANNEL: RwLock<Option<rmq::Channel>> = RwLock::new(None);
    pub static ref DIP_CONN: RwLock<Option<Arc<rmq::Connection>>> = RwLock::new(None);
    pub static ref PG_POOLS: RwLock<HashMap<String, sqlx::Pool<sqlx::Postgres>>> =
        RwLock::new(HashMap::new());
}

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

extern crate num;
extern crate num_derive;

async fn init_webdriver_client(webdriver: &str, user_agent: &str) -> Result<()> {
    let mut capabilities = fantoccini::wd::Capabilities::new();
    // https://docs.rs/fantoccini/latest/fantoccini/struct.ClientBuilder.html
    capabilities.insert(
        "moz:firefoxOptions".to_string(),
        serde_json::json!({
            // https://developer.mozilla.org/en-US/docs/Web/WebDriver/Capabilities/firefoxOptions#example
            "args": ["-headless"],
            // https://stackoverflow.com/questions/46771456/how-to-automate-firefox-mobile-with-selenium
            "prefs": {
                "general.useragent.override": user_agent,
            },
        }),
    );

    debug!("webdriver: {webdriver}");
    let c = fantoccini::ClientBuilder::native()
        .capabilities(capabilities)
        .connect(webdriver)
        .await
        .expect("failed to connect to WebDriver");
    if !DID_TERMINATED.load(Ordering::Relaxed) {
        // *CLIENT.write().await.unwrap() = Some(c);
        *CLIENT.write().unwrap() = Some(c);
        Ok(())
    } else {
        c.close().await.expect("close");
        bail!("did terminated")
    }
}

use std::num::NonZeroUsize;

#[derive(Debug, Serialize, Deserialize)]
pub struct Diap {
    price_min: Option<i32>,
    price_max: Option<i32>,
    count: usize,
}

#[derive(Serialize)]
pub struct MessageForDip {
    #[serde(rename = "workerName")]
    worker_name: String,
    params: MessageForDipParams,
}
#[derive(Serialize)]
pub struct MessageForDipParams {
    params: MessageForDipParamsParams,
    source: Vec<serde_json::Value>,
}
#[derive(Serialize)]
pub struct MessageForDipParamsParams {
    dip_module_id: usize,
    #[serde(rename = "fileName")]
    file_name: String,
}

async fn ipify() -> Result<()> {
    use tokio::time::{sleep, Duration};
    let ipify_url = settings!(ipify_url).clone();
    let recovery_millis = settings!(recovery_millis);
    loop {
        match reqwest::get(&ipify_url).await {
            Err(err) => {
                warn!("{}:{}: {err}", file!(), line!());
                sleep(Duration::from_millis(recovery_millis)).await;
            }
            Ok(response) => {
                let status = response.status();
                if status != reqwest::StatusCode::OK {
                    warn!("{}:{}: {status:?}", file!(), line!());
                    sleep(Duration::from_millis(recovery_millis)).await;
                } else {
                    match response.text().await {
                        Err(err) => {
                            warn!("{}:{}: {err}", file!(), line!());
                            sleep(Duration::from_millis(recovery_millis)).await;
                        }
                        Ok(text) => {
                            info!("current ip: {text}");
                            break Ok(());
                        }
                    }
                }
            }
        }
    }
}

pub enum TryLimitFor {
    All(usize),
    Specific {
        internal_server_error: usize,
        other: usize,
    },
}
pub enum FetchKind {
    List,
    Card,
    Ipify,
}
async fn fetch(
    c: &mut fantoccini::Client,
    url: &str,
    try_limit: TryLimitFor,
    fetch_kind: FetchKind,
) -> Result<std::result::Result<String, http::status::StatusCode>> {
    Ok({
        let mut try_count = 0;
        loop {
            use tokio::time::{sleep, Duration};
            let respite_millis = match fetch_kind {
                FetchKind::List => settings!(list_respite_millis),
                FetchKind::Card => settings!(card_respite_millis),
                FetchKind::Ipify => settings!(ipify_respite_millis),
            };
            sleep(Duration::from_millis(respite_millis)).await;
            match will_did!(trace => format!("evaluate {url:?}"),{
                EVALUATE_COUNT_TILL_RELOAD_BROWSER.fetch_add(1, Ordering::Relaxed);
                EVALUATE_COUNT_TILL_CHANGE_IP.fetch_add(1, Ordering::Relaxed);
                const JS: &str = r#"
                    const [ url, connect_timeout_millis, callback ] = arguments
                    const controller = new AbortController()
                    const timeoutId = setTimeout(
                        () => controller.abort(),
                        connect_timeout_millis
                    )
                    const signal = controller.signal
                    fetch( url, { signal } )
                        .then( resp =>
                            resp.text().then( text =>
                                callback( resp.ok ? { ok: text } : { text, status: resp.status  } )
                            )
                        )
                        .catch( e =>
                            callback( { err: e + '' } )
                        )
                "#;
                let connect_timeout_millis = settings!(connect_timeout_millis);
                c.execute_async(
                    JS,
                    vec![
                        serde_json::Value::String(url.to_owned()),
                        serde_json::Value::Number(connect_timeout_millis.into())
                    ],
                )
                .await
            }) {
                Err(err) => {
                    try_count += 1;
                    if try_count
                        >= match try_limit {
                            TryLimitFor::All(value) => value,
                            TryLimitFor::Specific { other, .. } => other,
                        }
                    {
                        bail!("{}:{}: evaluate {url:?}: {err}", file!(), line!());
                    } else {
                        warn!("{}:{}: evaluate {url:?}: {err}", file!(), line!());
                        let recovery_millis = settings!(recovery_millis);
                        sleep(Duration::from_millis(recovery_millis)).await;
                        continue;
                    }
                }
                Ok(resp) => {
                    #[derive(Deserialize)]
                    #[serde(untagged)]
                    enum Resp {
                        Ok { ok: String },
                        Err { err: String },
                        Status { text: String, status: u16 },
                    }

                    match serde_json::from_value::<Resp>(resp)
                        .map_err(|err| anyhow!("{}:{}: {err}", file!(), line!()))?
                    {
                        Resp::Ok { ok } => {
                            break Ok(ok);
                        }
                        Resp::Err { err } => {
                            try_count += 1;
                            if try_count
                                >= match try_limit {
                                    TryLimitFor::All(value) => value,
                                    TryLimitFor::Specific { other, .. } => other,
                                }
                            {
                                bail!("{}:{}: {err}", file!(), line!());
                            } else {
                                warn!("{}:{}: {err}", file!(), line!());
                                let recovery_millis = settings!(recovery_millis);
                                sleep(Duration::from_millis(recovery_millis)).await;
                                continue;
                            }
                        }
                        Resp::Status { text, status } => {
                            try_count += 1;
                            if status != 404
                                && try_count
                                    >= match try_limit {
                                        TryLimitFor::All(value) => value,
                                        TryLimitFor::Specific {
                                            other,
                                            internal_server_error,
                                        } => {
                                            if status
                                                == http::StatusCode::INTERNAL_SERVER_ERROR.as_u16()
                                            {
                                                internal_server_error
                                            } else {
                                                other
                                            }
                                        }
                                    }
                            {
                                bail!("{}:{}: {status:?}: {text}", file!(), line!());
                            } else {
                                match status {
                                    403 | 429 => {
                                        warn!(
                                            "{}:{}: {status}: {text}, EVALUATE_COUNT_TILL_CHANGE_IP: {}",
                                            file!(),
                                            line!(),
                                            EVALUATE_COUNT_TILL_CHANGE_IP.load(Ordering::Relaxed)
                                        );
                                        will_did!(trace => "ipify", ipify().await)?;
                                        let change_ip_url = settings!(change_ip_url).clone();
                                        if let Some(change_ip_url) = change_ip_url {
                                            let resp =
                                                reqwest::get(&change_ip_url).await?.text().await?;
                                            info!("onlinesim changeIp: {}", resp);
                                            will_did!(trace => "ipify", ipify().await)?;
                                        }
                                        EVALUATE_COUNT_TILL_RELOAD_BROWSER
                                            .store(0, Ordering::Relaxed);
                                        continue;
                                    }
                                    404 => {
                                        warn!("{}:{}: {status:?}: {text}", file!(), line!());
                                        break Err(http::StatusCode::NOT_FOUND);
                                    }
                                    500 => {
                                        warn!("{}:{}: {status:?}: {text}", file!(), line!());
                                        break Err(http::StatusCode::INTERNAL_SERVER_ERROR);
                                    }
                                    _ => {
                                        warn!("{}:{}: {status:?}: {text}", file!(), line!());
                                        let recovery_millis = settings!(recovery_millis);
                                        sleep(Duration::from_millis(recovery_millis)).await;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })
}

fn extract_items(
    from: Vec<serde_json::Value>,
    to: &mut Vec<serde_json::Value>,
    prices: &mut HashMap<i64, usize>,
) -> Result<()> {
    for from in from.into_iter() {
        #[derive(Deserialize)]
        struct Item {
            #[serde(rename(deserialize = "type"))]
            tp: Option<String>,
            value: Option<ItemValue>,
        }
        #[derive(Deserialize)]
        struct ItemValue {
            list: Option<Vec<serde_json::Value>>,
            _items: Option<Vec<serde_json::Value>>,
        }
        let item = serde_json::from_value::<Item>(from.clone())?;
        match item.tp.as_deref() {
            Some("mapBanner") => {
                // skip
            }
            Some("xlItem") => {
                to.push(from);
            }
            Some("vip") => {
                if let Some(value) = item.value {
                    if let Some(list) = value.list {
                        extract_items(list, to, prices)?
                    } else {
                        unreachable!();
                    }
                } else {
                    unreachable!();
                }
            }
            Some("witcher") => {
                // skip // но это не точно !!!!!!!

                // if let Some(value) = item.value {
                //     if let Some(list) = value.items {
                //         extract_items(list, to, prices)?
                //     } else {
                //         unreachable!();
                //     }
                // } else {
                //     unreachable!();
                // }
            }
            Some("item") => {
                if let Some((price, json)) = get_price(from)? {
                    if let Some(price) = price {
                        entry!(prices, price =>
                           and_modify |e| { *e += 1; }
                           or_insert 1
                        );
                    }
                    to.push(json);
                }
            }
            Some(tp) => {
                warn!("unexpected type: {tp:?}");
            }
            None => {
                unreachable!();
            }
        }
    }
    Ok(())
}

use json::{By, Json, JsonSource};

fn get_price(from: serde_json::Value) -> Result<Option<(Option<i64>, serde_json::Value)>> {
    let json = Json::new(from, JsonSource::Name("item".to_string()));
    Ok(
        if let Ok(price) = json
            .get([By::key("value"), By::key("price"), By::key("current")])
            .or_else(|_| json.get([By::key("value"), By::key("price")]))
            .or_else(|_| json.get([By::key("price"), By::key("current")]))
            .or_else(|_| json.get([By::key("price")]))
            .or_else(|_| json.get([By::key("priceDetailed"), By::key("fullString")]))
            .map_err(|err| anyhow!("{}:{}: {err}", file!(), line!()))?
            .as_str()
        {
            lazy_static::lazy_static! {
                pub static ref RE_MATCH: regex::Regex = regex::Regex::new(r"[\d\s]*₽").unwrap();
                pub static ref RE_REPLACE: regex::Regex = regex::Regex::new(r"\D").unwrap();
            }
            let price = if RE_MATCH.is_match(price) {
                let s = RE_REPLACE.replace_all(price, "");
                match s.parse::<i64>() {
                    Ok(value) => Some(value),
                    Err(err) => {
                        warn!("{s:?}.parse::<i32>(): {err}");
                        None
                    }
                }
            } else {
                None
            };
            Some((price, json.value))
        } else {
            warn!(
                ".value.price not found at {}",
                serde_json::to_string_pretty(&json.value).unwrap()
            );
            None
        },
    )
}

use std::path::PathBuf;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_price() -> Result<()> {
        dotenv::dotenv().context("file .env")?;
        let _ = pretty_env_logger::try_init_timed();
        let from = serde_json::json!({
            "value": {
                "price": {
                  "current": "1 100 000 ₽",
                  "sale": null
                }
            }
        });
        if let Some((price, _)) = super::get_price(from)? {
            if let Some(price) = price {
                info!("price: {price}");
            } else {
                error!("price failed to parse");
            }
        } else {
            error!("price not found");
        }
        let from = serde_json::json!({
            "value": {
                "price": "1 100 000 ₽"
            }
        });
        if let Some((price, _)) = super::get_price(from)? {
            if let Some(price) = price {
                info!("price: {price}");
            } else {
                error!("price failed to parse");
            }
        } else {
            error!("price not found");
        }
        Ok(())
    }
}
