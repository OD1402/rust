use super::*;

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use serde_with::{serde_as, DisplayFromStr};

#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy, Hash)]
pub struct Facet {
    #[serde_as(as = "DisplayFromStr")]
    pub region: Region,
    #[serde_as(as = "DisplayFromStr")]
    pub category: Category,
    #[serde_as(as = "DisplayFromStr")]
    pub deal: Deal,
}
use std::str::FromStr;

#[derive(PartialEq, Eq, Debug, Clone, Copy, strum::Display, strum::EnumIter, Hash)]
pub enum Region {
    #[strum(serialize = "Москва")]
    Москва,
    #[strum(serialize = "Московская область")]
    МосковскаяОбласть,
    #[strum(serialize = "Санкт-Петербург")]
    СанктПетербург,
    #[strum(serialize = "Ленинградская область")]
    ЛенинградскаяОбласть,
    #[strum(serialize = "Краснодарский край")]
    КраснодарскийКрай,
    #[strum(serialize = "Республика Хакасия")]
    РеспубликаХакасия,
    #[strum(serialize = "Приморский край")]
    ПриморскийКрай,
    #[strum(serialize = "Воронежская область")]
    ВоронежскаяОбласть,
    #[strum(serialize = "Ростовская область")]
    РостовскаяОбласть,
    #[strum(serialize = "Тверская область")]
    ТверскаяОбласть,
    #[strum(serialize = "Калининградская область")]
    КалининградскаяОбласть,
    #[strum(serialize = "Республика Татарстан")]
    РеспубликаТатарстан,
    #[strum(serialize = "Нижегородская область")]
    НижегородскаяОбласть, 
    #[strum(serialize = "Новосибирская область")]
    НовосибирскаяОбласть,
    #[strum(serialize = "Оренбургская область")]
    ОренбургскаяОбласть,
    #[strum(serialize = "Республика Крым")]
    РеспубликаКрым,
    #[strum(serialize = "Владимирская область")]
    ВладимирскаяОбласть,
    #[strum(serialize = "Кемеровская область")]
    КемеровскаяОбласть,
    #[strum(serialize = "Калужская область")]
    КалужскаяОбласть,
    #[strum(serialize = "Липецкая область")]
    ЛипецкаяОбласть, 
    #[strum(serialize = "Мурманская область")]
    МурманскаяОбласть,
    #[strum(serialize = "Пензенская область")]
    ПензенскаяОбласть,
    #[strum(serialize = "Республика Удмуртия")]
    РеспубликаУдмуртия,
    #[strum(serialize = "Красноярский край")]
    КрасноярскийКрай, 
    
}
common_macros2::r#impl!(FromStr for Region; strum);

#[derive(PartialEq, Eq, Debug, Clone, Copy, strum::Display, strum::EnumIter, Hash)]
pub enum Deal {
    #[strum(serialize = "Купить")]
    Купить,
    #[strum(serialize = "Снять")]
    Снять,
}
common_macros2::r#impl!(FromStr for Deal; strum);

#[derive(PartialEq, Eq, Debug, Clone, Copy, strum::Display, strum::EnumIter, Hash)]
pub enum Category {
    #[strum(serialize = "Квартира")]
    Квартира,
    #[strum(serialize = "Комната")]
    Комната,
    #[strum(serialize = "Коммерческая недвижимость")]
    КоммерческаяНедвижимость,
    #[strum(serialize = "Дом, дача, коттедж")]
    ДомДачаKоттедж,
    #[strum(serialize = "Гараж и машиноместо")]
    ГаражИМашиноместо,
    #[strum(serialize = "Земельный участок")]
    ЗемельныйУчасток,
}
common_macros2::r#impl!(FromStr for Category; strum);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParamValue {
    Scalar(usize),
    List(Vec<usize>),
}

impl PartialEq for ParamValue {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Scalar(value_self) => {
                if let Self::Scalar(value_other) = other {
                    value_self == value_other
                } else {
                    false
                }
            }
            Self::List(value_self) => {
                if let Self::List(value_other) = other {
                    let value_self = value_self.iter().collect::<HashSet<_>>();
                    let value_other = value_other.iter().collect::<HashSet<_>>();
                    value_self == value_other
                } else {
                    false
                }
            }
        }
    }
}
impl Eq for ParamValue {}

pub struct Params(pub HashMap<usize, ParamValue>);

impl_display!(Params, self, f, {
    if self.0.is_empty() {
        write!(f, "")
    } else {
        write!(
            f,
            "{}",
            self.0
                .iter()
                .map(|(key, value)| {
                    match value {
                        ParamValue::Scalar(value) => {
                            format!("&params[{key}]={value}")
                        }
                        ParamValue::List(values) => values
                            .iter()
                            .map(|value| format!("&params[{key}][]={value}"))
                            .collect::<Vec<_>>()
                            .join(""),
                    }
                })
                .collect::<Vec<_>>()
                .join("")
        )
    }
});

impl Facet {
    pub fn category_id(&self) -> usize {
        match self.category {
            Category::Комната => 23,
            Category::Квартира => 24,
            Category::ДомДачаKоттедж => 25,
            Category::КоммерческаяНедвижимость => 42,
            Category::ГаражИМашиноместо => 85,
            Category::ЗемельныйУчасток => 26,
        }
    }
    pub fn location_id(&self) -> usize {
        // https://www.avito.ru/web/1/js/items?_=&categoryId=24&locationId=636370&cd=1&s=101&p=1&params%5B201%5D=1059&params%5B499%5D%5B0%5D=5254&params%5B549%5D%5B0%5D=5698&params%5B549%5D%5B1%5D=5697&params%5B549%5D%5B2%5D=5696&verticalCategoryId=1&rootCategoryId=4&localPriority=0&countOnly=1
        match self.region {
            Region::Москва => 637640,
            Region::МосковскаяОбласть => 637680,
            Region::СанктПетербург => 653240,
            Region::ЛенинградскаяОбласть => 636370,
            Region::КраснодарскийКрай => 632660,
            Region::РеспубликаХакасия => 650890,
            Region::ПриморскийКрай => 644490,
            Region::ВоронежскаяОбласть => 625670,
            Region::РостовскаяОбласть => 651110,
            Region::ТверскаяОбласть => 656890,
            Region::КалининградскаяОбласть => 629990,
            Region::РеспубликаТатарстан => 650130,
            Region::НижегородскаяОбласть => 640310,
            Region::НовосибирскаяОбласть => 641470,
            Region::ОренбургскаяОбласть => 642480,
            Region::РеспубликаКрым => 621550,
            Region::ВладимирскаяОбласть=> 624300,
            Region::КемеровскаяОбласть=> 631080,
            Region::КалужскаяОбласть=> 630270,
            Region::ЛипецкаяОбласть=> 637260,
            Region::МурманскаяОбласть=>640000,
            Region::ПензенскаяОбласть=>643250,
            Region::РеспубликаУдмуртия=>659200,
            Region::КрасноярскийКрай=>634930,
        }
    }
    pub fn params(&self) -> Params {
        Params(match self.category {
            Category::Комната => {
                let mut vec = vec![(
                    200,
                    ParamValue::Scalar(match self.deal {
                        Deal::Купить => 1054,
                        Deal::Снять => 1055,
                    }),
                )];
                if matches!(self.deal, Deal::Снять) {
                    vec.push((596, ParamValue::Scalar(6203)))
                }
                vec.into_iter().collect()
            }
            Category::Квартира => {
                let mut vec = vec![(
                    201,
                    ParamValue::Scalar(match self.deal {
                        Deal::Купить => 1059,
                        Deal::Снять => 1060,
                    }),
                )];
                if matches!(self.deal, Deal::Снять) {
                    vec.push((504, ParamValue::Scalar(5256)))
                }
                vec.into_iter().collect()
            }
            Category::ДомДачаKоттедж => {
                let mut vec = vec![(
                    202,
                    ParamValue::Scalar(match self.deal {
                        Deal::Купить => 1064,
                        Deal::Снять => 1065,
                    }),
                )];
                if matches!(self.deal, Deal::Снять) {
                    vec.push((528, ParamValue::Scalar(5476)))
                }
                vec.into_iter().collect()
            }
            Category::КоммерческаяНедвижимость => vec![(
                536,
                ParamValue::Scalar(match self.deal {
                    Deal::Купить => 5545,
                    Deal::Снять => 5546,
                }),
            )]
            .into_iter()
            .collect(),
            // params[110799]: 472642 - Офис
            // params[110799]: 472643 - Свободного Назначения
            // params[110799]: 472644 - Торговая площадь
            // params[110799]: 472645 - Склад
            // params[110799]: 472646 - Произодство
            // params[110799]: 472647 - Общепит
            // params[110799]: 472648 - Гостиница
            // params[110799]: 473329 - Автосервис
            // params[110799]: 473330 - Здание


            Category::ГаражИМашиноместо => vec![(
                204,
                ParamValue::Scalar(match self.deal {
                    Deal::Купить => 1074,
                    Deal::Снять => 1075,
                }),
            )]
            .into_iter()
            .collect(),
            Category::ЗемельныйУчасток => vec![(
                203,
                ParamValue::Scalar(match self.deal {
                    Deal::Купить => 1069,
                    Deal::Снять => 1070,
                }),
            )]
            .into_iter()
            .collect(),
        })
    }
    pub fn dip_code(&self) -> String {
        format!(
            "avito-{}-{}-{}",
            match self.deal {
                Deal::Купить => "sale",
                Deal::Снять => "rent",
            },
            match self.category {
                Category::Квартира => "flat",
                Category::Комната => "room",
                Category::КоммерческаяНедвижимость => "comm",
                Category::ДомДачаKоттедж => "cottage",
                Category::ГаражИМашиноместо => "stall",
                Category::ЗемельныйУчасток => "land",
            },
            match self.region {
                Region::Москва => "msk",
                Region::МосковскаяОбласть => "mo",
                Region::СанктПетербург => "spb",
                Region::ЛенинградскаяОбласть => "lo",
                Region::КраснодарскийКрай => "kr",
                Region::РеспубликаХакасия => "reshakas",
                Region::ПриморскийКрай => "primorskiy",
                Region::ВоронежскаяОбласть => "voronezh",
                Region::РостовскаяОбласть => "rostov",
                Region::ТверскаяОбласть => "tver",
                Region::КалининградскаяОбласть => "kaliningrad",
                Region::РеспубликаТатарстан => "tatarstan",
                Region::НижегородскаяОбласть => "nn",
                Region::НовосибирскаяОбласть => "novosib",
                Region::ОренбургскаяОбласть => "orenburg",
                Region::РеспубликаКрым => "krym",
                Region::ВладимирскаяОбласть => "vladimir",
                Region::КемеровскаяОбласть => "kemerovo",
                Region::КалужскаяОбласть => "kaluga",
                Region::ЛипецкаяОбласть => "lipetsk",
                Region::МурманскаяОбласть => "murmansk",
                Region::ПензенскаяОбласть => "penza",
                Region::РеспубликаУдмуртия => "udmurtiya",
                Region::КрасноярскийКрай => "krasnoyarsk",
            },
        )
    }
}

impl_display!(
    Facet,
    self,
    "{}::{}::{}",
    self.region,
    self.category,
    self.deal
);

impl FromStr for Facet {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = match s.strip_suffix(".sh") {
            Some(s) => s,
            None => s,
        };
        let ss = s.split('-').collect::<Vec<_>>();
        if ss.len() != 3 {
            bail!("3 parts expected instead of {:?}", ss);
        }
        let mut region: Option<Region> = None;
        let mut deal: Option<Deal> = None;
        let mut category: Option<Category> = None;
        for s in ss {
            match s {
                "spb" => {
                    region = Some(Region::СанктПетербург);
                }
                "lo" => {
                    region = Some(Region::ЛенинградскаяОбласть);
                }
                "kr" => {
                    region = Some(Region::КраснодарскийКрай);
                }
                "reshakas" => {
                    region = Some(Region::РеспубликаХакасия);
                }
                "kaliningrad" => {
                    region = Some(Region::КалининградскаяОбласть);
                }
                "tatarstan" => {
                    region = Some(Region::РеспубликаТатарстан);
                }
                "primorskiy" => {
                    region = Some(Region::ПриморскийКрай);
                }
                "voronezh" => {
                    region = Some(Region::ВоронежскаяОбласть);
                }
                "rostov" => {
                    region = Some(Region::РостовскаяОбласть);
                }
                "tver" => {
                    region = Some(Region::ТверскаяОбласть);
                }
                "nn" => {
                    region = Some(Region::НижегородскаяОбласть);
                }
                "novosib" => {
                    region = Some(Region::НовосибирскаяОбласть);
                }
                "orenburg" => {
                    region = Some(Region::ОренбургскаяОбласть);
                }
                "krym" => {
                    region = Some(Region::РеспубликаКрым);
                }
                "vladimir" => {
                    region = Some(Region::ВладимирскаяОбласть);
                }
                "kemerovo" => {
                    region = Some(Region::КемеровскаяОбласть);
                }
                "kaluga" => {
                    region = Some(Region::КалужскаяОбласть);
                }
                "lipetsk" => {
                    region = Some(Region::ЛипецкаяОбласть);
                }
                "murmansk" => {
                    region = Some(Region::МурманскаяОбласть);
                }
                "penza" => {
                    region = Some(Region::ПензенскаяОбласть);
                }
                "udmurtiya" => {
                    region = Some(Region::РеспубликаУдмуртия);
                }
                "krasnoyarsk" => {
                    region = Some(Region::КрасноярскийКрай);
                }
                "msk" => {
                    region = Some(Region::Москва);
                }
                "mo" => {
                    region = Some(Region::МосковскаяОбласть);
                }
                "sale" => {
                    deal = Some(Deal::Купить);
                }
                "rent" => {
                    deal = Some(Deal::Снять);
                }
                "flat" | "habit" => {
                    category = Some(Category::Квартира);
                }
                "room" => {
                    category = Some(Category::Комната);
                }
                "commre" | "comm" | "comre" | "commercial" => {
                    category = Some(Category::КоммерческаяНедвижимость);
                }
                "cottage" => {
                    category = Some(Category::ДомДачаKоттедж);
                }
                "garage" | "stall" => {
                    category = Some(Category::ГаражИМашиноместо);
                }
                "land" | "lot" => {
                    category = Some(Category::ЗемельныйУчасток);
                }
                s => bail!("unexpected '{s}'"),
            }
        }
        if region.is_none() {
            bail!("region is not specified");
        }
        if category.is_none() {
            bail!("category is not specified");
        }
        if deal.is_none() {
            bail!("deal is not specified");
        }
        Ok(Self {
            region: region.unwrap(),
            category: category.unwrap(),
            deal: deal.unwrap(),
        })
    }
}
