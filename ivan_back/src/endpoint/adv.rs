use super::*;

use reqwest::Client;
use serde_json::json;

// пример запроса:
// http://localhost:42084/adv?region=spb&lat=59.926640&lon=30.321932&radius=250&realty_type_id=23,24

#[derive(Debug, serde::Deserialize)]
pub struct QueryFilter {
    region: Region,
    realty_type_id: String,
    lat: f64,
    lon: f64,
    radius: u8, // условных единиц 1 уе = 20 м

                /////////////////
                // point: Option<Location>,
                // // external_address: Option<String>,
                // is_auction: Option<u8>, //Option<bool>,
                // point: Option<String>,  // Option<Location>,
                // radius: Option<u16>,
                // // deal_type_id: Option<u8>,
                // min_price: Option<f64>, // Минимальная цена
                // max_price: Option<f64>, // Максимальная цена
}

#[derive(Debug, PartialEq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Region {
    Msk,
    Mo,
    Spb,
    Lo,
}

pub async fn adv(Query(params): Query<QueryFilter>) -> impl IntoResponse {
    // let min_price = params.min_price;
    // let max_price = params.max_price;

    let mut url = "http://stable-mls-search2.baza-winner.ru:9200/msk_commre_advs_v1/_search?";
    if params.region == Region::Spb || params.region == Region::Lo {
        url = "http://elastic:wRtC3jqdQM@v9z.ru:53101/rgn_commre_advs_v1/_search?";
    }

    let geo_base_spot_code: Vec<&str> = match params.region {
        Region::Msk => {
            vec!["ru-msk-old", "ru-msk-new", "ru-msk"]
        }
        Region::Mo => {
            vec!["ru-mo"]
        }
        Region::Spb => {
            vec!["ru-spb"]
        }
        Region::Lo => {
            vec!["ru-lo"]
        }
    };

    println!("geo_base_spot_code {:?}", geo_base_spot_code);

    let client = Client::new();

    let mut query = json!({
        "query": {
            "bool": {
                // "must": {
                //     "range": {
                //         "price_rub": {
                //             "gte": min_price,
                //             "lte": max_price
                //         }
                //     }
                // },
                "filter": [
                    {
                        "terms": {
                            "geo_base_spot_code": geo_base_spot_code
                        }
                    },
                    {
                        "term": {
                            "deal_status_id": 1 /* Только актуальные */
                        }
                    }
                ]
            }
        },
        "_source": [
            "external_address",
            "external_url",
            "geo_cache_state_name",
            "location",
            "max_square",
            "min_square",
            "note",
            "price_rub",
            "realty_type_id",
            "realty_type_name",
            "is_auction",
            "deal_type_name"
        ],
        "size": 500
    });

    {
        let realty_type_ids: Vec<u8> = params
            .realty_type_id
            .split(',')
            .map(|s| {
                s.trim()
                    .parse::<u8>()
                    .expect("Некорректное значение для realty_type_id")
            })
            .collect();

        query["query"]["bool"]["filter"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "terms": {
                    "realty_type_id": realty_type_ids
                }
            }));
    }

    {
        println!("params.radius {:#?}", params.radius);

        let radius_num = (params.radius as u16) * 20; // u16
        let radius_string = format!("{}m", radius_num);

        println!("radius_string {:#?}", radius_string);

        let geo_distance = json!({
            "distance": radius_string,
            "location": json!({
                "lat": params.lat,
                "lon": params.lon
              })
        });

        query["query"]["bool"]["must"] = json!([{ "geo_distance": geo_distance }]);
    }

    println!("query {:?}", query.to_string());

    // if let Some(external_address) = params.external_address {
    //     query["query"]["bool"]["filter"]
    //         .as_array_mut()
    //         .unwrap()
    //         .push(json!({
    //             "terms": {
    //                 "external_address": [
    //                     external_address
    //                 ]
    //             }
    //         }));
    // }

    // поиск по is_auction
    // if let Some(is_auction) = params.is_auction {
    //     if is_auction == 1 {
    //         // Только торги
    //         query["query"]["bool"]["filter"]
    //             .as_array_mut()
    //             .unwrap()
    //             .push(json!({
    //                 "term": {
    //                     "is_auction": 1
    //                 }
    //             }));
    //     } else if is_auction == 0 {
    //         // Кроме торгов - в ES указано не false, а null

    //         let bool_clause = query["query"]["bool"].as_object_mut().unwrap();

    //         // Добавляем условие must_not
    //         let must_not = bool_clause
    //             .entry("must_not")
    //             .or_insert(json!([]))
    //             .as_array_mut()
    //             .unwrap();

    //         // Исключаем объекты, где is_auction == 1
    //         must_not.push(json!({
    //             "term": {
    //                 "is_auction": 1
    //             }
    //         }));

    //         // Добавляем условия should
    //         let should = bool_clause
    //             .entry("should")
    //             .or_insert(json!([]))
    //             .as_array_mut()
    //             .unwrap();

    //         // Включаем объекты, где is_auction == 0
    //         should.push(json!({
    //             "term": {
    //                 "is_auction": 0
    //             }
    //         }));

    //         // Включаем объекты, где is_auction == null
    //         should.push(json!({
    //             "bool": {
    //                 "must_not": {
    //                     "exists": {
    //                         "field": "is_auction"
    //                     }
    //                 }
    //             }
    //         }));
    //     }
    // }

    let response = client
        .post(url)
        .json(&query)
        .send()
        .await
        .expect("Failed to send request");

    let json_response = response
        .json::<serde_json::Value>()
        .await
        .expect("Failed to parse JSON");

    Json(json_response)
}
