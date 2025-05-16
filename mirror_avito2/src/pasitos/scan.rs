use super::*;
use std::process::Command;

pub type DiapPageResult = Result<(
    usize,
    Vec<serde_json::Value>,
    chrono::DateTime<chrono::Utc>,
    HashMap<i64, usize>,
)>;

#[derive(Debug)]
pub struct Page {
    pub num: usize,
}
pub async fn diap_page(
    page: &Option<Page>,
    accu: &Arc<pasitos::db::ListAccu>,
    _page_limit: NonZeroUsize,
    min_price: Option<i64>, // TODO: remove it or Page::min_price
    facet: &Facet,
    mut prices: HashMap<i64, usize>,
) -> DiapPageResult {
    let mut avito_url = format!(
        "https://{}/web/{}/js/items?_=&categoryId={}&locationId={}",
        settings!(avito.host),
        settings!(avito.list_api),
        facet.category_id(),
        facet.location_id(),
    );

    if let Some(value) = min_price {
        println!("min_price: {:?}", min_price);
        avito_url.push_str(&format!("&pmin={value}"));
    }

    avito_url.push_str("&cd=0");

    avito_url.push_str("&s=1"); // s=1 - это priceAsc (сортировка по цене)

    if let Some(Page { num: page_num, .. }) = page.as_ref() {
        println!("page_num: {:?}", page);
        avito_url.push_str(&format!("&p={page_num}"));
    } else {
        avito_url.push_str("&p=1");
    };

    avito_url.push_str(&format!(
        // было "&params=&params[204]=1075", стало "&params[204]=1075"
        "{}",
        facet.params(),
    ));

    avito_url.push_str("&verticalCategoryId=1&rootCategoryId=4&localPriority=0&disabledFilters[ids][0]=byTitle&disabledFilters[slugs][0]=bt");

    // хакасия гараж - десктоп json (новый вариант)
    // let avito_url = "https://www.avito.ru/web/1/js/items?_=&categoryId=85&locationId=650890&pmin=100&pmax=2000&cd=0&s=1&p=1&params%5B204%5D=1075&verticalCategoryId=1&rootCategoryId=4&localPriority=1&disabledFilters%5Bids%5D%5B0%5D=byTitle&disabledFilters%5Bslugs%5D%5B0%5D=bt&subscription%5Bvisible%5D=true&subscription%5BisShowSavedTooltip%5D=false&subscription%5BisErrorSaved%5D=false&subscription%5BisAuthenticated%5D=true".to_string();

    // хакасия гараж - моб json (старый вариант)
    // let avito_url = "https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params=&params[204]=1075&categoryId=85&locationId=650890&sort=priceAsc&page=1&lastStamp=1716483860&display=list&limit=50&page=1".to_string();

    // cargo run -p mirror_avito2 -- -w mirror_avito2 scan-lists -f reshakas-stall-rent

    use tokio::time::{sleep, Duration};
    sleep(Duration::from_millis(settings!(list_respite_millis))).await;

    ////////////////////

    // let mut resp2 = {
    //     let c = &mut CLIENT.write().unwrap();
    //     let c = c.as_mut().ok_or_else(|| anyhow!("no client"))?;
    //     let try_limit = settings!(try_limit);
    //     fetch(c, &avito_url, TryLimitFor::All(try_limit), FetchKind::List).await?
    // };

    // panic!("111111111111111 resp2 {:?}", resp2);
    //////////////////////

    use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};

    let client = reqwest::Client::new();
    let user_agent = settings!(user_agent).clone();

    let mut headers = HeaderMap::new();
    // headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/25.0 Chrome/121.0.0.0 Mobile Safari/537.3"));

    if let Ok(header_value) = HeaderValue::from_str(&user_agent) {
        headers.insert(USER_AGENT, header_value);
    } else {
        eprintln!("Invalid User-Agent header value: {}", user_agent);
    }

    println!("Сканируем avito_url: {}", avito_url);

    // headers.insert(
    //     HeaderValue::from_static("Accept"),
    //     HeaderValue::from_static(
    //         "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    //     ),
    // );

    let response = client
        .get(&avito_url)
        .headers(headers)
        .send()
        .await?;

    if response.status().is_success() {
        let body = response.text().await?;

        let created_at = chrono::Utc::now();

        #[derive(Deserialize)]
        struct Response {
            catalog: ResponseCatalog,
            #[serde(rename(deserialize = "mainCount"))]
            count: usize,
        }
        #[derive(Deserialize)]
        struct ResponseCatalog {
            items: Vec<serde_json::Value>,
        }

        let resp_json = serde_json::from_str::<Response>(&body)?;
        let mut items: Vec<serde_json::Value> = vec![];

        extract_items(resp_json.catalog.items, &mut items, &mut prices)?;

        Ok((resp_json.count, items, created_at, prices))
    } else {
        println!("avito_url {}", avito_url);
        bail!("no data. response: {response:?}");
    }
    //////////////////////

    // let mut resp = Command::new("wget");
    // resp.arg(&avito_url).arg("-O").arg("-");

    // let resp = resp.output().expect("failed to execute process");

    // let resp_str = String::from_utf8_lossy(resp.stdout.as_slice());

    // println!("Сканируем avito_url: {}", avito_url);

    // if resp.status.success() {
    //     let created_at = chrono::Utc::now();

    //     #[derive(Deserialize)]
    //     struct Response {
    //         catalog: ResponseCatalog,
    //         #[serde(rename(deserialize = "mainCount"))]
    //         count: usize,
    //     }
    //     #[derive(Deserialize)]
    //     struct ResponseCatalog {
    //         items: Vec<serde_json::Value>,
    //     }

    //     let resp_json = serde_json::from_str::<Response>(&resp_str)?;
    //     let mut items: Vec<serde_json::Value> = vec![];

    //     extract_items(resp_json.catalog.items, &mut items, &mut prices)?;

    //     Ok((resp_json.count, items, created_at, prices))
    // } else {
    //     println!("avito_url {}", avito_url);
    //     bail!("no data. resp: {resp:?}");
    // }
}

#[allow(clippy::too_many_arguments)]
pub fn diap_page_sync(
    res: DiapPageResult,
    page: Option<Page>,
    accu: Arc<pasitos::db::ListAccu>,
    min_price: Option<i64>,
    facet: Facet,
    page_limit: NonZeroUsize,
    send_to: Option<Env>,
    try_count: usize,
) -> Result<()> {
    let (avito_count, items_page, created_at, prices) = res?;
    let is_empty = items_page.is_empty();

    // =================================
    /*
    Можно получить за раз:
    * 100 страниц
    * 50 объявлений на странице
    * 5000 объявлений всего
    // ================================= */

    let page_num = page.as_ref().map(|page| page.num).unwrap_or(1);
    let avito_page_count = (avito_count as f32 / page_limit.get() as f32).ceil() as usize;

    if !is_empty {
        pasitos!(db push_back SavePage {
            created_at,
            scan_session_id: accu.session.id(),
            min_price,
            page_num: page_num as i16,
            items: items_page,
            avito_count: avito_count as i32,
        });
    }

    fn min_price_new(prices: &HashMap<i64, usize>) -> Option<i64> {
        let mut ret = None;
        if prices.len() > 1 {
            let mut prices = prices.iter().collect::<Vec<_>>();
            let count = itertools::fold(&prices, 0, |ret, (_, count)| ret + **count);
            prices.sort_by_key(|(price, _)| *price);
            prices.reverse();
            let limit = std::cmp::max(std::cmp::min(10, count), count / 100);
            let mut sum = 0;
            for (price, count) in prices.iter() {
                sum += **count;
                if sum >= limit {
                    ret = Some(**price);
                    break;
                }
            }
            warn!(
                "{}:{}: max_price: {:?}, count: {count}, limit: {limit}, sum: {sum}, min_price_new: {ret:?}",
                file!(),
                line!(),
                prices.first().map(|(price, _)| price)
            );
            // prices.reverse();
            // todo!(
            //     "{}:{}: prices: {prices:#?}\nmin_price_new: {ret:?}",
            //     file!(),
            //     line!(),
            // );
        }
        ret
    }
    if let Some(_env) = send_to {
        todo!();
        // pasitos!(dip push_back Publish {
        //     env,
        //     facet,
        //     items: items_page,
        // });
    }
    if is_empty {
        // если массив с объявлениями items_page пустой
        let count = itertools::fold(&prices, 0, |ret, (_, count)| ret + *count);
        if avito_count > 0 && count > avito_count * 9 / 10 {
            warn!(
                "{}:{}: will FinishSession due to items_page.is_empty() and count={count} and avito_count={avito_count}",
                file!(),
                line!()
            );
            pasitos!(db push_front FinishSession {
                session_id: accu.session.id(),
                finished_at: chrono::Utc::now(),
            });
        } else if try_count < settings!(avito.diap_page_try_count_limit) {
            warn!(
                "{}:{}: page: {page:?}, min_price: {min_price:?}, facet: {facet}, try_count: {try_count}",
                file!(),
                line!()
            );

            let millis = settings!(avito.diap_page_try_delay_millis);
            warn!("{}:{}: will delay ScanDiapPage {{ page: {page:?}, min_price: {min_price:?}, facet: {facet}, try_count: {try_count} }} for {millis} millis", file!(), line!());
            pasitos!(delay ScanDiapPage {
                page,
                accu,
                min_price: min_price_new(&prices).or_else(|| {
                    let mut prices = prices.keys().collect::<Vec<_>>();
                    prices.sort();
                    prices.reverse();
                    prices.first().copied().copied()
                }).or(min_price),
                send_to,
                page_limit,
                facet,
                try_count: try_count + 1,
                prices,
            } for std::time::Duration::from_millis(millis));
            // pasitos!(db push_front FinishSession {
            //     session_id: accu.session.id(),
            //     finished_at: chrono::Utc::now(),
            // });
            // pasitos!(scan push_back DiapPage {
            //     page,
            //     accu,
            //     min_price,
            //     send_to,
            //     page_limit,
            //     facet,
            //     try_count: try_count + 1,
            //     prices,
            // });
        } else {
            warn!(
                "{}:{}: page: {page:?}, min_price: {min_price:?}, facet: {facet}, try_count: {try_count}",
                file!(),
                line!()
            );
            pasitos!(db push_front FinishSession {
                session_id: accu.session.id(),
                finished_at: chrono::Utc::now(),
            });
        }
    } else if avito_page_count > settings!(page_count_max) && page_num >= settings!(page_count_max)
    {
        warn!("{}:{}: page_num: {page_num}", file!(), line!());
        pasitos!(scan push_back DiapPage {
            page: None,
            accu,
            min_price: min_price_new(&prices),
            send_to,
            page_limit,
            facet,
            try_count: 0,
            prices: HashMap::new(),
        });
    } else if page_num <= settings!(page_count_max) {
        let next_page = Some(Page { num: page_num + 1 });

        if avito_page_count >= page_num + 1 {
            pasitos!(scan push_back DiapPage {
                page: next_page,
                accu,
                min_price,
                send_to,
                page_limit,
                facet,
                try_count: 0,
                prices,
            });
        } else {
            println!(
                "страницы закончились, закрываем сессию {:?}",
                accu.session.id()
            );
            pasitos!(db push_front FinishSession {
                session_id: accu.session.id(),
                finished_at: chrono::Utc::now(),
            });
        }
    } else {
        panic!(
            "{}:{}: page: {page:?}, min_price: {min_price:?}, facet: {facet}, try_count: {try_count}",
            file!(),
            line!()
        );
    }
    Ok(())
}

pub type CardResult = Result<(
    Vec<i64>,
    chrono::DateTime<chrono::Utc>,
    std::result::Result<serde_json::Value, (i64, http::StatusCode)>,
)>;
pub async fn card(mut ids: Vec<i64>) -> CardResult {
    let (created_at, card_result) = {
        let id = ids.pop().unwrap();
        let avito_url = format!(
            "https://{}/api/{}/items/{}?key={}&action=view",
            settings!(avito.host),
            settings!(avito.card_api),
            id,
            settings!(avito.key),
        );
        let resp = {
            let c = &mut CLIENT.write().unwrap();
            let c = c.as_mut().ok_or_else(|| anyhow!("no client"))?;
            let try_limit = settings!(try_limit);
            // #[allow(clippy::await_holding_lock)]
            fetch(
                c,
                &avito_url,
                TryLimitFor::Specific {
                    other: try_limit,
                    internal_server_error: 0,
                },
                FetchKind::Card,
            )
            .await?
        };
        (
            chrono::Utc::now(),
            match resp {
                Ok(resp) => {
                    let card_json = serde_json::from_str::<serde_json::Value>(&resp)?;
                    Ok(card_json)
                }
                Err(status_code) => Err((id, status_code)),
            },
        )
    };
    Ok((ids, created_at, card_result))
}

pub fn card_sync(
    res: CardResult,
    send_to: Option<Env>,
    output: bool,
    facet: Option<Facet>,
) -> Result<()> {
    let (ids, created_at, card_result) = res?;
    match card_result {
        Ok(card_json) => {
            if output {
                println!("{}", serde_json::to_string_pretty(&card_json)?);
            }

            if let Some(env) = send_to {
                if let Some(facet) = &facet {
                    pasitos!(dip push_back Publish {
                        env,
                        facet: *facet,
                        items: vec![card_json.clone()],
                        and_then:pasitos::dip::PublishAndThen::None,
                    });
                } else {
                    bail!("option'--facet | -f' MUST be specified to send card.json to dip");
                }
            }
            pasitos!(db push_back SaveCardJson {
                created_at,
                card_json,
            });
        }
        Err((id, status)) => {
            warn!("{id}: {status}");
            pasitos!(db push_back SaveCardFailed {
                created_at,
                id,
                status: status.as_u16() as i16,
            });
        }
    }
    if !ids.is_empty() {
        pasitos!(scan push_back Card {
            ids,
            send_to,
            output,
            facet,
        });
    }
    Ok(())
}
