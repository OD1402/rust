use super::*;

pub enum PublishAndThen {
    None,
    SendCards {
        ids: Vec<i32>,
        page_limit: NonZeroUsize,
    },
    SendLists {
        ids: Vec<i32>,
        page_limit: NonZeroUsize,
    },
}
pub type PublishResult = Result<usize>;
pub async fn publish(env: &Env, facet: &Facet, items: Vec<serde_json::Value>) -> PublishResult {
    // let url = settings!(dip).url(matches!(env, Env::Alpha)).to_owned();
    let url = settings!(dip).url(env).to_owned();
    let run_for = "send_to_dip";
    let channel = rmq::get!(channel => DIP_CHANNEL, DIP_CONN, RMQ_POOLS, url.clone(), run_for);
    let len = items.len();
    for mut item in items.into_iter() {
        prepare_item(&mut item);
        let item = match item {
            serde_json::Value::Object(mut map) => {
                map.insert(
                    "facet".to_string(),
                    serde_json::Value::String(facet.to_string()),
                );
                serde_json::Value::Object(map)
            }
            _ => unreachable!(),
        };
        let msg = MessageForDip {
            worker_name: settings!(dip.worker_name).clone(),
            params: MessageForDipParams {
                params: MessageForDipParamsParams {
                    dip_module_id: 5,
                    file_name: facet.dip_code(),
                },
                source: vec![item],
            },
        };
        let queue = (*SETTINGS.read().unwrap())
            .as_ref()
            .unwrap()
            .content
            .dip
            .queue
            .clone();
        rmq::basic_publish_str(&channel, queue, serde_json::to_string_pretty(&msg)?).await?;
    }
    Ok(len)
}

pub fn publish_sync(
    res: PublishResult,
    // ids: Vec<i32>,
    // page_limit: NonZeroUsize,
    env: Env,
    facet: Facet,
    and_then: PublishAndThen,
) -> Result<()> {
    let len = res?;
    info!("did send {len} items of {facet} to DIP {env}");
    match and_then {
        PublishAndThen::None => {}
        PublishAndThen::SendCards { ids, page_limit } => {
            if !ids.is_empty() {
                pasitos!(db push_back SendCards {
                    ids,
                    send_to: env,
                    facet,
                    page_limit,
                });
            }
        }
        PublishAndThen::SendLists { ids, page_limit } => {
            if !ids.is_empty() {
                pasitos!(db push_back SendLists {
                    ids,
                    send_to: env,
                    facet,
                    page_limit,
                });
            }
        }
    }
    Ok(())
}

fn prepare_item(item: &mut serde_json::Value) {
    match item {
        serde_json::Value::String(s) => {
            *s = s
                .chars()
                .filter(|ch| ch.len_utf8() <= 2)
                .collect::<String>()
        }
        serde_json::Value::Array(vec) => {
            for item in vec.iter_mut() {
                prepare_item(item);
            }
        }
        serde_json::Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                match key.as_str() {
                    "price" | "normalizedPrice" => {}
                    _ => prepare_item(value),
                }
            }
        }
        _ => {}
    }
}

// =====================================
// =====================================
// =====================================

#[cfg(test)]
mod tests {

    #[allow(unused_imports)]
    use anyhow::{anyhow, bail, Context, Error, Result};
    #[allow(unused_imports)]
    use log::{debug, error, info, trace, warn};

    use super::*;

    #[tokio::test]
    async fn test_prepare_item() -> Result<()> {
        dotenv::dotenv().context(format!(
            "file .env at {:?}",
            std::env::current_dir().unwrap()
        ))?;
        let _ = pretty_env_logger::try_init_timed();
        // load_settings();
        //

        let mut item = serde_json::json!({
          "type": "item",
          "value": {
            "id": 2342989510i64,
            "uri": "ru.avito://1/item/show?context=H4sIAAAAAAAA_0q0MrSqLrYytFKqULIutjI2tFIqNEjMNzI0MyrLzyoqLUjNK6gqTs3JyrNILywqTrIsV7KuBQQAAP__bygBIzUAAAA&itemId=2342989510",
            "shop": {
              "id": 1416496,
              "name": "Эко-поселок \"Светлая поляна\" 🌱"
            },
            "time": 1648624616,
            "price": "12 500 000 ₽",
            "title": "Коттедж 130 м² на участке 9,4 сот.",
            "coords": {
              "lat": "55.2220717755831",
              "lng": "37.7804531819091"
            },
            "images": {
              "main": {
                "75x55": "https://73.img.avito.st/image/1/1.VCPyzbaz-MrSb5Ra9rcOl0Rs_t5Gbg.AAqxw98TKqgzUcY0Dr9x0kYRzBlSki5oV_HLVKaxzBs",
                "80x60": "https://73.img.avito.st/image/1/1.VCPyzbaz-Mrkb4Ja9rcOl0Rs_t5Gbg.py9OlvnW4zrMuPmGN0R8LjL5M3yhG7NnvnfB__JN2pM",
                "100x75": "https://73.img.avito.st/image/1/1.VCPyzbay-MqMb2zJ1twjPBtu-MxSbPo.mjQ8Bz351BHKbiyHc1JHp2RZf8aJgXxyE68y8ml3lpY",
                "140x105": "https://73.img.avito.st/image/1/1.VCPyzbay-MrcbCjJ1twjPBtu-MxSbPo.42FTDduOf1xdKoDLpydbMIVpEvXLZaLXPJzMkEKBJB8",
                "208x156": "https://73.img.avito.st/image/1/1.VCPyzbay-MrkbULK1twjPBtu-MxSbPo.d0rPC2Cl6rK2QsQyEcPbfaiosGvOkR0Djyz0ay-QzLw",
                "240x180": "https://73.img.avito.st/image/1/1.VCPyzbay-MqkbRLK1twjPBtu-MxSbPo.ImvaX2alLzWLJ2glhrAT0ivUcl6yejPCr6N3Nvlv3tg",
                "288x216": "https://73.img.avito.st/image/1/1.VCPyzbay-MqEakrL1twjPBtu-MxSbPo.Xzu9G70tasXWKiDgF3_B6z7Otrl-GXZfEhm25hxLDhg",
                "432x324": "https://73.img.avito.st/image/1/1.VCPyzbay-MqkaHLN1twjPBtu-MxSbPo.5Sb2x0Zm8qDd0rWqHHul6kIoXhpT0CtqoFJ9sqWLb-s",
                "640x480": "https://73.img.avito.st/image/1/1.VCPyzbay-MrEZDrP1twjPBtu-MxSbPo.XtTinF7gwKpwEfPWML5Sz92Utljn7QdY9Bi4K7Vl8Q8",
                "1280x960": "https://73.img.avito.st/image/1/1.VCPyzbay-MrEenrH1twjPBtu-MxSbPo.8er9GXniOKky8lHi1aBIykaEHxhw0WdkOxvJ7ZkG0Dk"
              },
              "count": 13
            },
            "address": "д. Острожки, эко-посёлок Светлая Поляна",
            "badgeBar": {
              "badges": [
                {
                  "id": 21,
                  "style": {
                    "fontColor": {
                      "value": "#000000",
                      "valueDark": "#E3E3E3"
                    },
                    "backgroundColor": {
                      "value": "#EBEBEB",
                      "valueDark": "#2E2E2E"
                    }
                  },
                  "title": "Онлайн-показ"
                }
              ]
            },
            "category": {
              "id": 25,
              "name": "Дома, дачи, коттеджи"
            },
            "hasVideo": false,
            "location": "Новокаширское шоссе, 40 км",
            "uri_mweb": "/domodedovo/doma_dachi_kottedzhi/kottedzh_130_m_na_uchastke_94_sot._2342989510",
            "userType": "private",
            "isFavorite": false,
            "isVerified": false,
            "geoReferences": [
              {
                "content": "Новокаширское шоссе, 40 км"
              }
            ],
            "contactlessView": true,
            "normalizedPrice": "96 154 ₽ за м²"
          }
        });

        prepare_item(&mut item);
        debug!(
            "test_prepare_item: {}",
            serde_json::to_string_pretty(&item)?
        );

        // let mut p = ScanUrlProvider::default();
        // info!("get_for_check(): {}", p.get_for_check().value());

        // let config = Config::init()?;
        //
        // info!("current dir: {:?}", std::env::current_dir().unwrap());
        // let mut file_path = config.data_root;
        // file_path.push("packed");
        // file_path.push("msk");
        // file_path.push("habit_sale");

        // {
        //     let mut file_path = file_path.clone();
        //     file_path.push("23");
        //     file_path.push("0");
        //     let bunch: OfferBunch = load_json(&file_path).await?;
        //     for (id_tail, offer) in bunch.by_id_tail.iter() {
        //         info!("id_tail: {}", id_tail);
        //
        //         if let Some(Ok(phone_no)) = offer.phone {
        //             let msg =
        //                 get_msg_for_dip(bunch.id_base, *id_tail, phone_no, offer, &file_path)?;
        //             info!("msg: {}", serde_json::to_string_pretty(&msg)?);
        //         } else {
        //             warn!("phone not found: {:?}", offer.phone);
        //         }
        //     }
        // }
        // {
        //     // video issue
        //     file_path.push("21");
        //     file_path.push("99");
        //     let bunch: OfferBunch = load_json(&file_path).await?;
        //     let id_tail = 645810;
        //     let offer = bunch.by_id_tail.get(&id_tail).unwrap();
        //     if let Some(Ok(phone_no)) = offer.phone {
        //         let msg = get_msg_for_dip(bunch.id_base, id_tail, phone_no, offer, &file_path)?;
        //         info!("msg: {}", serde_json::to_string_pretty(&msg)?);
        //     } else {
        //         warn!("phone not found: {:?}", offer.phone);
        //     }
        // }

        // for year in 2020..=2022 {
        //     for s in (1..=365u32)
        //         .into_iter()
        //         .map(|o| NaiveDate::from_yo(year, o).to_string())
        //     {
        //         let ret = YearQuartConverted::try_from(s.to_owned())?;
        //         debug!("ret: {}", ret);
        //         assert_eq!(ret.to_string(), s);
        //     }
        // }

        // let lat = 55.869122;
        // let lon = 37.473959;
        // let dist = 0.002;
        // let (lat, lon) = LatLonBoundary::new_vec(lat, lon, dist);
        // debug!("lat: {:?}, lon: {:?}", lat, lon);

        Ok(())
    }

    // #[tokio::test]
    // async fn test_env_display() -> Result<()> {
    //     dotenv().context(format!(
    //         "file .env at {:?}",
    //         std::env::current_dir().unwrap()
    //     ))?;
    //     let _ = pretty_env_logger::try_init_timed();
    //     info!("sent_to_{}", Env::Alpha);
    //     Ok(())
    // }
}
