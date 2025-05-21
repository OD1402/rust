#[allow(unused_imports)]
use anyhow::{anyhow, bail, Context, Error, Result};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::address_parsing;
use crate::district_spb;

use super::*;
use base64::encode;
use regex::Regex;
use std::collections::HashMap;
use std::string::ToString;

use serde::{Deserialize, Serialize};

pub use elastic_scan::{ElasticContentTrait, ElasticRequest, ElasticRequestArg};

use std::time::SystemTime;

pub type ElasticSource = serde_json::Value;
pub struct ElasticContentSharedInner {
    pub bunches_to_save: VecDeque<OffersToSave>,
    pub need_finish: bool,
    specific: Option<ElasticContentSharedInnerSpecific>,
}
use std::collections::VecDeque;
type OffersToSave = Vec<ElasticSource>;
pub type ElasticContentSharedInnerSpecific = Arc<RwLock<CsvSpecific>>;

impl ElasticContentSharedInner {
    pub fn new(specific: ElasticContentSharedInnerSpecific) -> Result<Self> {
        Ok(Self {
            specific: Some(specific),
            bunches_to_save: VecDeque::new(),
            need_finish: false,
        })
    }
    pub fn finish(&mut self) -> Result<()> {
        struct RenameTask {
            from: PathBuf,
            to: PathBuf,
        }
        let rename_tasks: Vec<RenameTask> = {
            let specific = self
                .specific
                .take()
                .ok_or_else(|| anyhow!("{}:{}: specific is none", file!(), line!()))?;

            if let Some(specific) = Arc::into_inner(specific) {
                let offer = specific.into_inner().unwrap();

                let CsvSpecific {
                    writer: mut offer_writer,
                    temp_filepath: offer_temp_filepath,
                    output_filepath: offer_output_filepath,
                    ..
                } = offer;
                offer_writer.flush()?;
                vec![RenameTask {
                    from: offer_temp_filepath,
                    to: offer_output_filepath,
                }]
            } else {
                vec![]
            }
        };
        for RenameTask { from, to } in rename_tasks.iter() {
            std::fs::rename(from, to)
                .map_err(|err| anyhow!("Failed to rename {from:?} to {to:?}: {err}"))?;
        }
        println!(
            "result saved to {}",
            rename_tasks
                .into_iter()
                .map(|RenameTask { to, .. }| format!("{to:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        Ok(())
    }
}

pub type ElasticContentShared = Arc<RwLock<ElasticContentSharedInner>>;
pub struct ElasticContent {
    pub index: MlsFacet,
    pub shared: ElasticContentShared,
}

impl ElasticContent {
    pub fn new(index: MlsFacet, shared: ElasticContentShared) -> Self {
        Self { index, shared }
    }
    pub fn new_request(&self, mode: ExportFor) -> ElasticRequest {
        let fields_for_elastic_request = vec![
            // "guid",
            "project_name",
            "is_new_building",
            "geo_cache_district_name",
            "total_room_count",
            "is_studio",
            "is_free_planning",
            "external_address", // берем исходный адрес для минимизации искажений
            "storey",
            "storeys_count",
            "walls_material_type_name",
            "total_square",
            "life_square",
            "kitchen_square",
            "house_square",
            "water_closet_type_id",
            "price_rub",
            "external_seller_2",
            "phone_list",
            "note",
            "sale_type_name",
            "built_year",
            "external_url",
            "location.lat",
            "location.lon",
        ];

        let fields_for_elastic_request = fields_for_elastic_request
            .iter()
            .map(|&s| s.to_string())
            .collect();

        ElasticRequest::new(ElasticRequestArg {
            host: settings!(elastic.host).clone(),
            index_url_part: self.index.elastic_index(),
            query: match mode {
                ExportFor::Analytics(ForSiteFacet::Habit) => serde_json::json!({
                  "bool": {
                    "must": [
                      {
                        "range": {
                          "pub_datetime": {
                              "gte": "now-50d/d" /* за 50 дня */
                          }
                        }
                      },
                      {
                        "range": {
                            "price_rub": {
                                "gte": 20000000
                            }
                        }
                      },
                      {
                          "term": {
                              "deal_status_id": 1 /* Только актуальные */
                          }
                      },
                      {
                        "term": {
                            "realty_type_id": 1 /* Только квартиры (доли и комнаты не нужны) */
                        }
                      },
                      {
                        "term": {
                            "geo_state_guid": "21349991-CF4A-4963-B4D4-9A2F3968CE27" /* Спб */
                        }
                      },
                      {
                          "terms": {
                              "media_id": [
                                17 /* Cian */,
                                21 /* Avito */,
                                23 /* Яндекс */,
                                37 /* LifeDeluxe */,
                              ]
                          }
                      }
                    ]
                  }
                }),
                ExportFor::Analytics(ForSiteFacet::Cottage) => serde_json::json!({
                  "bool": {
                    "must": [
                      {
                        "range": {
                          "pub_datetime": {
                              "gte": "now-50d/d" /* за 50 дня */
                          }
                        }
                      },
                      {
                        "range": {
                            "price_rub": {
                                "gte": 20000000
                            }
                        }
                      },
                      {
                          "term": {
                              "deal_status_id": 1 /* Только актуальные */
                          }
                      },
                      {
                          "terms": {
                              "realty_type_id": [
                                3 /* дом */,
                               // 5 /* дача */,
                                6 /* дуплекс */,
                                7 /* квадрохаус */,
                                8 /* коттедж */,
                                9 /* коттедж в КП */,
                                10 /* таунхаус */,
                                11 /* усадьба */,
                              ]
                          }
                      },
                      {
                        "term": {
                            "geo_state_guid": "21349991-CF4A-4963-B4D4-9A2F3968CE27" /* Спб */
                        }
                      },
                      {
                          "terms": {
                              "media_id": [
                                17 /* Cian */,
                                // 37 /* LifeDeluxe */,
                              ]
                          }
                      }
                    ]
                  }
                }),
                // ExportFor::Analytics => serde_json::json!({
                //   "bool": {
                //     "must": [
                //       {
                //         "range": {
                //           "pub_datetime": {
                //               "gte": "now-5d/d" /* за 50 дня */
                //           }
                //         }
                //       },
                //       {
                //         "range": {
                //             "price_rub": {
                //                 "gte": 20000000
                //             }
                //         }
                //       },
                //       {
                //           "term": {
                //               "deal_status_id": 1 /* Только актуальные */
                //           }
                //       },
                //       {
                //         "term": {
                //             "realty_type_id": 1 /* Только квартиры (доли и комнаты не нужны) */
                //         }
                //       },
                //       {
                //         "term": {
                //             "geo_state_guid": "21349991-CF4A-4963-B4D4-9A2F3968CE27" /* Спб */
                //         }
                //       },
                //       {
                //           "terms": {
                //               "media_id": [
                //                 // 17 /* Cian */,
                //                 // 21 /* Avito */,
                //                 // 23 /* Яндекс */,
                //                 37 /* LifeDeluxe */,
                //               ]
                //           }
                //       }
                //     ]
                //   }
                // }),
            },
            fields: fields_for_elastic_request,
            fetch_limit: settings!(elastic.fetch_limit),
            scroll_timeout: settings!(elastic.scroll_timeout_secs),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OfferFormatRysev {
    // cargo run --features debug // https://doc.rust-lang.org/cargo/commands/cargo-run.html
    #[cfg(feature = "debug")]
    guid: Option<String>,
    source: Option<String>,    // источник  Option<String>
    market: Option<String>,    // вторичка, новостройка
    district: Option<String>,  // район города
    rooms: Option<String>,     // кол-во комнат, студия, св планировка
    address: Option<String>,   // адрес
    floor: Option<String>,     // этаж/этажность
    flat_type: Option<String>, // тип дома
    area_total: Option<f64>,
    area_live: Option<f64>,
    area_kitchen: Option<f64>,
    wc: Option<String>,         // наличие санузна. Раздельный - Р, совместный - С
    price: Option<f64>,         // цена. Цифра. всегда делим на 1000
    agency: Option<String>,     // продавец/компания/агент
    phone: Option<String>,      // номер телефона
    additional: Option<String>, // описание объявления
    sale_type: Option<String>,  // Свободная продажа, альтернатива
    condition: Option<String>,  // ищем в тексте объявления "Переуступка", заменяем на "У"
    rend_end: Option<u16>,      // срок/год сдачи дома
    url: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
}

impl ElasticContentSharedInner {
    pub fn save(&mut self) -> Result<()> {
        if let Some(mut offers_to_save) = self.bunches_to_save.pop_front() {
            let offer_specific_lock = self.specific.as_ref().unwrap();
            let mut offer_specific = offer_specific_lock.write().unwrap();

            let rx = Regex::new(r"\[([^\]]+)\]").unwrap();

            for offer in &mut offers_to_save {
                let mut record: Vec<String> = vec![];
                let offer = serde_json::to_value(offer).unwrap();
                let offer = Json::new(offer, JsonSource::Name("offer".to_owned()));

                let mut offer_for_rysev = OfferFormatRysev {
                    // cargo run --features debug // https://doc.rust-lang.org/cargo/commands/cargo-run.html
                    #[cfg(feature = "debug")]
                    guid: None,
                    source: None,
                    market: None,
                    district: None,
                    rooms: None,
                    address: None,
                    floor: None,
                    flat_type: None,
                    area_total: None,
                    area_live: None,
                    area_kitchen: None,
                    wc: None,
                    price: None,
                    agency: None,
                    phone: None,
                    additional: None,
                    sale_type: None,
                    condition: None,
                    rend_end: None,
                    url: None,
                    lat: None,
                    lon: None,
                };

                // cargo run --features debug // https://doc.rust-lang.org/cargo/commands/cargo-run.html
                #[cfg(feature = "debug")]
                if let Ok(guid) = offer
                    .get([By::key("guid")])
                    .and_then(|value| value.as_string(false))
                {
                    offer_for_rysev.guid = Some(guid.to_string());
                }

                if let Ok(mut project_name) = offer
                    .get([By::key("project_name")])
                    .and_then(|value| value.as_string(false))
                {
                    if project_name == "Яндекс" {
                        project_name = "Я".to_string();
                    } else if project_name == "cian.ru" {
                        project_name = "Ц".to_string();
                    } else if project_name == "AVITO.ru" {
                        project_name = "Ф".to_string();
                    } else if project_name == "LifeDeluxe" {
                        project_name = "D".to_string();
                    }

                    offer_for_rysev.source = Some(project_name.to_string());
                }

                if let Ok(is_new_building) = offer
                    .get([By::key("is_new_building")])
                    .and_then(|value| value.as_u16())
                    .map(|value| if value == 0 { "в" } else { "п" }.to_string())
                {
                    offer_for_rysev.market = Some(is_new_building.clone());
                }

                if let Ok(is_studio) = offer
                    .get([By::key("is_studio")])
                    .and_then(|value| value.as_u16())
                {
                    if is_studio == 1 {
                        offer_for_rysev.rooms = Some("Студия".to_string());
                    }
                }

                if let Ok(is_free_planning) = offer
                    .get([By::key("is_free_planning")])
                    .and_then(|value| value.as_u16())
                {
                    if is_free_planning == 1 {
                        offer_for_rysev.rooms = Some("Своб. планировка".to_string());
                    }
                }

                if let Ok(total_room_count) = offer
                    .get([By::key("total_room_count")])
                    .and_then(|value| value.as_string(false))
                {
                    offer_for_rysev.rooms = Some(total_room_count.to_string());
                }

                if let Ok(external_address) = offer
                    .get([By::key("external_address")])
                    .and_then(|value| value.as_string(false))
                {
                    println!("исходный address 000 {:#?}", &external_address);

                    let updated_address = address_parsing::address_normalization(
                        external_address.to_string(),
                        offer_for_rysev
                            .source
                            .clone()
                            .expect("Ой, источник данных не определен")
                            .to_string(),
                    );

                    println!("итоговый address 111 {:#?}", &updated_address);
                    offer_for_rysev.address = Some(updated_address);
                }

                let mut floor = String::from("");

                if let Ok(storey) = offer
                    .get([By::key("storey")])
                    .and_then(|value| value.as_string(false))
                {
                    floor.push_str(&storey);
                } else {
                    floor.push_str("?");
                }

                if let Ok(storeys_count) = offer
                    .get([By::key("storeys_count")])
                    .and_then(|value| value.as_string(false))
                {
                    floor.push_str("/");
                    floor.push_str(&storeys_count);
                } else {
                    floor.push_str("/?");
                }

                offer_for_rysev.floor = Some(floor);

                if let Ok(walls_material_type_name) = offer
                    .get([By::key("walls_material_type_name")])
                    .and_then(|value| value.as_string(false))
                {
                    offer_for_rysev.flat_type = Some(walls_material_type_name);
                }

                if let Ok(total_square) = offer
                    .get([By::key("total_square")])
                    .and_then(|value| value.as_string(false))
                {
                    let area_total: f64 = total_square
                        .parse()
                        .expect("Failed to parse string 'total_square' to float");

                    offer_for_rysev.area_total = Some(area_total);
                }

                if let Ok(house_square) = offer
                    .get([By::key("house_square")])
                    .and_then(|value| value.as_string(false))
                {
                    let area_total: f64 = house_square
                        .parse()
                        .expect("Failed to parse string 'house_square' to float");

                    offer_for_rysev.area_total = Some(area_total);
                }

                if let Ok(life_square) = offer
                    .get([By::key("life_square")])
                    .and_then(|value| value.as_string(false))
                {
                    let area_live: f64 = life_square
                        .parse()
                        .expect("Failed to parse string to float");

                    offer_for_rysev.area_live = Some(area_live);
                }

                if let Ok(kitchen_square) = offer
                    .get([By::key("kitchen_square")])
                    .and_then(|value| value.as_string(false))
                {
                    let area_kitchen: f64 = kitchen_square
                        .parse()
                        .expect("Failed to parse string to float");

                    offer_for_rysev.area_kitchen = Some(area_kitchen);
                }

                if let Ok(water_closet_type_id) = offer
                    .get([By::key("water_closet_type_id")])
                    .and_then(|value| value.as_i8())
                {
                    let water_closet_type = HashMap::from([
                        (1, "-"),
                        (2, "С"),
                        (3, "Р"),
                        (4, "2"),
                        (5, "3"),
                        (6, "4"),
                        (7, "2С"),
                        (8, "2Р"),
                        (9, "3С"),
                        (10, "3Р"),
                        (11, "4С"),
                        (12, "4Р"),
                        (13, "+"),
                        (2, "С"),
                    ]);

                    offer_for_rysev.wc = Some(water_closet_type[&water_closet_type_id].to_string());
                }

                if let Ok(price_rub) = offer
                    .get([By::key("price_rub")])
                    .and_then(|value| value.as_string(false))
                {
                    let price: f64 = price_rub.parse().expect("Failed to parse string to float");

                    offer_for_rysev.price = Some(price / 1000.0);
                }

                if let Ok(seller) = offer
                    .get([By::key("external_seller_2")])
                    .and_then(|value| value.as_string(false))
                {
                    // external_seller_2: "[Агентство Русский Фонд Недвижимости, Илатовская Тамара Валерьевна, ЯНДЕКС ID 238402290](https://realty.ya.ru/sankt-peterburg_i_leningradskaya_oblast/agentstva/russkij-fond-nedvizhimosti-238402290/)",

                    if let Some(captures) = rx.captures(&seller) {
                        if let Some(matched) = captures.get(1) {
                            offer_for_rysev.agency = Some(matched.as_str().to_string());
                        }
                    } else {
                        offer_for_rysev.agency = Some(seller.to_string());
                    }
                }

                if let Ok(phone_list) = offer
                    .get([By::key("phone_list")])
                    .and_then(|value| value.as_string(false))
                {
                    if offer_for_rysev.source != Some("Ф".to_string()) // Авито
                                && offer_for_rysev.source != Some("Я".to_string())
                    // Яндекс
                    {
                        offer_for_rysev.phone = Some(phone_list);
                    }
                }

                if let Ok(mut sale_type_name) = offer
                    .get([By::key("sale_type_name")])
                    .and_then(|value| value.as_string(false))
                {
                    if sale_type_name == "прямая продажа" {
                        sale_type_name = "П".to_string();
                    } else if sale_type_name == "альтернатива" {
                        sale_type_name = "В".to_string();
                    }

                    offer_for_rysev.sale_type = Some(sale_type_name.to_string());
                }

                if let Ok(note) = offer
                    .get([By::key("note")])
                    .and_then(|value| value.as_string(false))
                {
                    offer_for_rysev.additional = Some(note.to_string());

                    let rx_assign =
                        Regex::new(r"(?i)переуступк|уступка прав|уступки прав").unwrap();
                    if rx_assign.is_match(&note) {
                        offer_for_rysev.condition = Some("У".to_string());
                    }

                    // есть объявления, где альтернатива ошибочно определена, поэтому сделаем анализ примечания
                    let rx_free = Regex::new(r"(?i)прямая продажа|свободная продажа").unwrap();
                    if rx_free.is_match(&note) {
                        offer_for_rysev.sale_type = Some("П".to_string());
                    }
                    let rx_alt =
                        Regex::new(r"(?i)альтернативная продажа|встречная(\))? продажа").unwrap();
                    if rx_alt.is_match(&note) {
                        offer_for_rysev.sale_type = Some("В".to_string());
                    }
                }

                if let Ok(built_year) = offer
                    .get([By::key("built_year")])
                    .and_then(|value| value.as_i16())
                {
                    offer_for_rysev.rend_end = Some(built_year.try_into().unwrap());
                }

                if let Ok(external_url) = offer
                    .get([By::key("external_url")])
                    .and_then(|value| value.as_string(false))
                {
                    let external_url_tmp = external_url.to_string();

                    println!("external_url 111 {:#?}", &external_url_tmp);

                    let rx_www = Regex::new(r"https?:\/\/(www\.)?").unwrap();
                    let rx_context = Regex::new(r"\?context=.+").unwrap();

                    let external_url = rx_www.replace_all(&external_url, "").into_owned();
                    let external_url = rx_context.replace_all(&external_url, "").into_owned();

                    external_url.to_string();

                    let external_url = encode(external_url);

                    offer_for_rysev.url = Some(external_url.to_string());
                }

                let mut point: (f64, f64) = (0.0, 0.0);

                if let Ok(location) = offer.get([By::key("location")]) {
                    if let Ok(lat) = location
                        .get([By::key("lat")])
                        .and_then(|value| value.as_string(false))
                    {
                        let lat_point: f64 = lat.parse().expect("Failed to parse string to float");

                        offer_for_rysev.lat = Some(lat_point);
                        point.0 = lat_point;
                    }

                    if let Ok(lon) = location
                        .get([By::key("lon")])
                        .and_then(|value| value.as_string(false))
                    {
                        let lon_point: f64 = lon.parse().expect("Failed to parse string to float");

                        offer_for_rysev.lon = Some(lon_point);
                        point.1 = lon_point;
                    }

                    let district = get_district(point);
                    offer_for_rysev.district = Some(district);
                }

                let mut is_new_building_lifedeluxe: bool = false;

                if offer_for_rysev.source == Some("D".to_string()) {
                    if offer_for_rysev.market == Some("п".to_string()) {
                        is_new_building_lifedeluxe = true;
                    } else {
                        offer_for_rysev.market = Some("в".to_string())
                    }
                }

                // if offer_for_rysev.source == Some("Ц".to_string()) && offer_for_rysev.market.is_none() {
                //     offer_for_rysev.market = Some("в".to_string())
                // }

                // if offer_for_rysev.source == Some("Ф".to_string()) && offer_for_rysev.market.is_none() {
                //     offer_for_rysev.market = Some("в".to_string())
                // }

                if offer_for_rysev.market.is_none() && (offer_for_rysev.source == Some("Ф".to_string()) || offer_for_rysev.source == Some("Ц".to_string())) {
                    offer_for_rysev.market = Some("в".to_string())
                }

                if offer_for_rysev.district != Some("Колпинский".to_string())
                    && offer_for_rysev.district != Some("Кронштадтский".to_string())
                    && !is_new_building_lifedeluxe
                {

                    if offer_for_rysev.additional.is_none() && offer_for_rysev.source == Some("Я".to_string()) {
                        println!("нет описания");
                    }

                    let offer_for_rysev = serde_json::to_value(&offer_for_rysev).unwrap();

                    let json_offer_for_rysev = Json::new(
                        offer_for_rysev,
                        JsonSource::Name("offer_for_rysev".to_owned()),
                    );

                    for field_name in offer_specific.fields.iter() {
                        record.push(field_value(&json_offer_for_rysev, field_name));
                    }

                    println!("===============");

                    offer_specific.writer.write_record(&record)?;
                }
            }
        }
        Ok(())
    }
}

fn is_point_in_polygon(point: (f64, f64), polygon: &[(f64, f64)]) -> bool {
    let (x, y) = (point.1, point.0);
    let mut inside = false;
    let n = polygon.len();

    for i in 0..n {
        let j = (i + 1) % n;
        let (xi, yi) = polygon[i];
        let (xj, yj) = polygon[j];

        let intersect = (yi > y) != (yj > y) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi);
        if intersect {
            inside = !inside;
        }
    }
    inside
}

fn get_district(point: (f64, f64)) -> String {
    let mut spot = String::from("");

    let dist_arr = district_spb::DISTRICT;

    for district in dist_arr {
        if is_point_in_polygon(point, &district.polygon) {
            spot = district.name.to_string();
            break;
        }
    }

    spot
}

impl ElasticContentTrait<ElasticSource> for ElasticContent {
    fn extend(&mut self, source: Vec<ElasticSource>, _scan_start: SystemTime) {
        self.shared
            .write()
            .unwrap()
            .bunches_to_save
            .push_back(source);
    }
    fn fields(&self) -> Vec<String> {
        let shared = &self.shared.read().unwrap();
        let offer_lock = shared.specific.as_ref().unwrap();
        let offer = &*offer_lock.read().unwrap();

        let offer_specific = offer;
        let offer_fields = &offer_specific.fields;
        offer_fields
            .iter()
            .filter_map(|field_name| field_name.split('.').next().map(|s| s.to_owned()))
            .collect()
    }
}

const USE_COMMA_FOR_FLOATS: bool = false;
use json::{By, Json, JsonSource};
fn field_value(container: &Json, field_name: &str) -> String {
    container
        .get(field_name.split('.').map(By::key).collect::<Vec<_>>())
        .ok()
        .and_then(|value| value.as_string(false).ok())
        .and_then(|value| {
            if field_name.ends_with("_datetime") {
                chrono::DateTime::parse_from_rfc3339(&value)
                    .ok()
                    .map(|datetime| {
                        datetime
                            .with_timezone(&chrono_tz::Europe::Moscow)
                            .format("%F") // https://docs.rs/chrono/latest/chrono/format/strftime/index.html
                            .to_string()
                    })
            } else {
                Some(value)
            }
        })
        .map(|value| {
            if !USE_COMMA_FOR_FLOATS {
                value
            } else {
                match value.parse::<f64>() {
                    Err(_err) => value,
                    Ok(value) => {
                        if value.fract() == 0f64 {
                            (value as i64).to_string()
                        } else {
                            value
                                .to_string()
                                .chars()
                                .map(|ch| match ch {
                                    '.' => ',',
                                    _ => ch,
                                })
                                .collect()
                        }
                    }
                }
            }
        })
        .map(|value| {
            if value.starts_with('=') {
                format!("'{}", value)
            } else {
                value
            }
        })
        .unwrap_or_else(|| "".to_owned())
}
