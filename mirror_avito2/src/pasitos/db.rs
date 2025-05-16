use super::*;

pub type SavePageResult = Result<()>;
use op_mode::OpMode;
use pool_db::*;

pub async fn save_page(
    created_at: chrono::DateTime<chrono::Utc>,
    scan_session_id: i32,
    min_price: Option<i64>,
    page_num: i16,
    items: Vec<serde_json::Value>,
    avito_count: i32,
) -> SavePageResult {
    let settings = settings!(db).clone();
    let op_mode = OpMode::Prod;
    let pool = get(PoolDb::Pg, settings, op_mode).await?;

    #[derive(sqlx::FromRow)]
    struct Ret {
        id: Option<i32>,
    }

    let ret = sqlx::query_as!(
        Ret,
        "select save_scan_page_desktop($1, $2, $3, $4, $5, $6) as id",
        created_at,
        scan_session_id,
        min_price.unwrap_or(0),
        page_num,
        &items,
        avito_count
    )
    .fetch_one(pool_db::as_ref!(Pg pool))
    .await
    .map_err(|err| {
        anyhow!(
            "{}:{}: {err}: {}",
            file!(),
            line!(),
            serde_json::to_string_pretty(&items).unwrap()
        )
    })?;
    let _ = ret.id;
    Ok(())
}

pub fn save_page_sync(res: SavePageResult) -> Result<()> {
    res
}

#[derive(Clone, Debug, Serialize)]
pub struct Session {
    pub scan_session_id: i32,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub min_price: Option<i64>,
    pub page_num: Option<i16>,
    pub already_scanned_count: i32,
}
impl Session {
    pub fn _timestamp(&self) -> u32 {
        self.started_at.timestamp() as u32
    }
    pub fn id(&self) -> i32 {
        self.scan_session_id
    }
}

pub struct ListAccu {
    pub session: pasitos::db::Session,
    pub ids: RwLock<HashSet<usize>>,
}

pub type StartSessionResult = Result<Session>;

pub async fn start_session(facet: &Facet) -> StartSessionResult {
    let run_for = "select new_scan_session_desktop";
    let settings = settings!(db).clone();
    let op_mode = OpMode::Prod;
    let pool = get(PoolDb::Pg, settings, op_mode).await?;

    will_did!(trace => run_for, {
        #[derive(sqlx::FromRow, Debug)]
        struct Ret {
            scan_session_id: Option<i32>,
            started_at: Option<chrono::DateTime<chrono::Utc>>,
            min_price: Option<i64>,
            page_num: Option<i16>,
            already_scanned_count: Option<i32>,
        }
        let ret = sqlx::query_as!(
            Ret,
            "
            select * from start_scan_session($1) 
            ",
            facet.to_string(),
        )
        .fetch_one(pool_db::as_ref!(Pg pool))
        .await?;

        println!("Получен scan_session_id: {}", ret.scan_session_id.unwrap());
        if ret.page_num > Some(0) {
            println!("page_num: {}", ret.page_num.unwrap());
        }
        if ret.min_price > Some(0) {
            println!("min_price: {}", ret.min_price.unwrap());
        }

        Ok(Session {
            scan_session_id: ret.scan_session_id.unwrap(),
            started_at: ret.started_at.unwrap(),
            min_price: ret.min_price,
            page_num: ret.page_num,
            already_scanned_count: ret.already_scanned_count.unwrap_or(0),
        })
    })
}

pub fn start_session_sync(
    res: StartSessionResult,
    facet: Facet,
    page_limit: NonZeroUsize,
    send_to: Option<Env>,
) -> Result<()> {
    let session = res?;
    let min_price = session.min_price;

    let page_num = session.page_num;

    let mut page_num_u = 1;

    if page_num > Some(0) {
        page_num_u = page_num.unwrap() as usize;
    }

    let accu = Arc::new(ListAccu {
        session,
        ids: RwLock::new(HashSet::<usize>::new()),
    });

    use crate::pasitos::scan::Page;
    let page = Some(Page { num: page_num_u });

    pasitos!(scan push_back DiapPage {
        page: page,
        accu,
        min_price,
        send_to,
        page_limit,
        facet,
        try_count: 0,
        prices: HashMap::new(),
    });
    Ok(())
}

pub type FinishSessionResult = Result<()>;

pub async fn finish_session(
    session_id: i32,
    finished_at: chrono::DateTime<chrono::Utc>,
) -> SavePageResult {
    let settings = settings!(db).clone();
    let op_mode = OpMode::Prod;
    let pool = pool_db::get(PoolDb::Pg, settings, op_mode).await?;

    will_did!(trace => "update scan_sessions",
        sqlx::query!(
            "
            update scan_sessions
            set finished_at = $1
            where id = $2
            ",
            finished_at,
            session_id
        )
        .execute(pool_db::as_ref!(Pg pool))
        .await
    )?;
    Ok(())
}

pub fn finish_session_sync(res: SavePageResult) -> Result<()> {
    res
}

pub type GetListsToSendResult = Result<Vec<i32>>;
pub async fn get_lists_to_send(facet: &Facet, sub_cmd: CommandScanLists) -> GetListsToSendResult {
    let settings = settings!(db).clone();
    let op_mode = OpMode::Prod;
    let pool = pool_db::get(PoolDb::Pg, settings, op_mode).await?;

    #[derive(sqlx::FromRow)]
    struct Ret {
        ids: Option<Vec<i32>>,
    }
    let ret = match sub_cmd {
        CommandScanLists::All => {
            sqlx::query_as!(
                Ret,
                "select array_agg(id) as ids from list_shots_to_send_all($1, $2)",
                facet.to_string(),
                true,
            )
            .fetch_one(pool_db::as_ref!(Pg pool))
            .await?
        }
        CommandScanLists::Rel { to } => {
            sqlx::query_as!(
                Ret,
                "select array_agg(id) as ids from list_shots_to_send_relative($1, $2)",
                facet.to_string(),
                to.map(|to| to as i32),
            )
            .fetch_one(pool_db::as_ref!(Pg pool))
            .await?
        }
    };
    let ret = ret.ids.unwrap_or_default();
    Ok(ret)
}

pub fn get_lists_to_send_sync(
    res: GetListsToSendResult,
    facet: Facet,
    send_to: Env,
    page_limit: NonZeroUsize,
) -> Result<()> {
    let ids = res?;
    if ids.is_empty() {
        warn!("no lists to send for {facet}");
    } else {
        info!("{} lists will be send to {send_to} for {facet}", ids.len());
        pasitos!(db push_back SendLists {
            ids,
            send_to,
            facet,
            page_limit
        });
    }
    Ok(())
}

pub type GetCardsToSendResult = Result<Vec<i32>>;
pub async fn get_cards_to_send(
    facet: &Facet,
    is_finished: bool,
    scan_deep_interval: Option<String>,
) -> GetCardsToSendResult {
    let settings = settings!(db).clone();
    let op_mode = OpMode::Prod;
    let pool = pool_db::get(PoolDb::Pg, settings, op_mode).await?;

    #[derive(sqlx::FromRow)]
    struct Ret {
        ids: Option<Vec<i32>>,
    }
    let ret = sqlx::query_as!(
        Ret,
        "select array_agg(id) as ids from card_shots_to_send($1, $2, $3)",
        facet.to_string(),
        is_finished,
        scan_deep_interval,
    )
    .fetch_one(pool_db::as_ref!(Pg pool))
    .await?;
    let ret = ret.ids.unwrap_or_default();
    Ok(ret)
}

pub fn get_cards_to_send_sync(
    res: GetCardsToSendResult,
    facet: Facet,
    send_to: Env,
    page_limit: NonZeroUsize,
) -> Result<()> {
    let ids = res?;
    if ids.is_empty() {
        warn!("no cards to send for {facet}");
    } else {
        info!("{} cards will be send to {send_to} for {facet}", ids.len());
        pasitos!(db push_back SendCards {
            ids,
            send_to,
            facet,
            page_limit
        });
    }
    Ok(())
}

pub type GetCardsToScanResult = Result<Vec<i64>>;
pub async fn get_cards_to_scan(
    facet: &Facet,
    is_finished: bool,
) -> GetCardsToScanResult {
    let settings = settings!(db).clone();
    let op_mode = OpMode::Prod;
    let pool = pool_db::get(PoolDb::Pg, settings, op_mode).await?;

    #[derive(sqlx::FromRow)]
    struct Ret {
        avito_ids: Option<Vec<i64>>,
    }
    let ret = sqlx::query_as!(
        Ret,
        "select array_agg(avito_id) as avito_ids from cards_to_scan($1, $2)",
        facet.to_string(),
        is_finished,
    )
    .fetch_one(pool_db::as_ref!(Pg pool))
    .await?;
    let ret = ret.avito_ids.unwrap_or_default();
    Ok(ret)
}

pub fn get_cards_to_scan_sync(
    res: GetCardsToScanResult,
    facet: Facet,
    send_to: Option<Env>,
    output: bool,
) -> Result<()> {
    let ids = res?;
    if ids.is_empty() {
        warn!("no cards to scan for {facet}");
    } else {
        info!("{} cards will be scanned for {facet}", ids.len());
        pasitos!(scan push_back Card {
            ids,
            send_to,
            output,
            facet: Some(facet),
        });
    }
    Ok(())
}

pub type SaveCardJsonResult = Result<()>;
pub async fn save_card_json(
    created_at: chrono::DateTime<chrono::Utc>,
    card_json: serde_json::Value,
) -> SaveCardJsonResult {
    let settings = settings!(db).clone();
    let op_mode = OpMode::Prod;
    let pool = pool_db::get(PoolDb::Pg, settings, op_mode).await?;

    sqlx::query!("call save_card_shot($1, $2)", created_at, card_json)
        .execute(pool_db::as_ref!(Pg pool))
        .await?;
    Ok(())
}

pub fn save_card_json_sync(res: SaveCardJsonResult) -> Result<()> {
    res
}

pub type SaveCardFailedResult = Result<()>;
pub async fn save_card_failed(
    created_at: chrono::DateTime<chrono::Utc>,
    avito_id: i64,
    status: i16,
) -> SaveCardFailedResult {
    let settings = settings!(db).clone();
    let op_mode = OpMode::Prod;
    let pool = pool_db::get(PoolDb::Pg, settings, op_mode).await?;

    sqlx::query!(
        "call save_card_shot_failed($1, $2, $3)",
        created_at,
        avito_id,
        status
    )
    .execute(pool_db::as_ref!(Pg pool))
    .await?;
    Ok(())
}

pub fn save_card_failed_sync(res: SaveCardFailedResult) -> Result<()> {
    res
}

pub type SendCardsResult = Result<(Vec<serde_json::Value>, Vec<i32>)>;
pub async fn send_cards(mut ids: Vec<i32>, page_limit: NonZeroUsize) -> SendCardsResult {
    let mut ids_to_get: Vec<i32> = vec![];
    while let Some(id) = ids.pop() {
        ids_to_get.push(id);
        if ids_to_get.len() >= page_limit.get() {
            break;
        }
    }
    let settings = settings!(db).clone();
    let op_mode = OpMode::Prod;
    let pool = pool_db::get(PoolDb::Pg, settings, op_mode).await?;

    #[derive(sqlx::FromRow)]
    struct Ret {
        card_shots: Option<Vec<serde_json::Value>>,
    }
    let ret = sqlx::query_as!(
        Ret,
        "select array_agg(value) as card_shots from card_shots_to_send($1)",
        &ids_to_get
    )
    .fetch_one(pool_db::as_ref!(Pg pool))
    .await?;
    let cards_shots = ret.card_shots.unwrap();
    Ok((cards_shots, ids))
}

pub fn send_cards_sync(
    res: SendCardsResult,
    facet: Facet,
    send_to: Env,
    page_limit: NonZeroUsize,
) -> Result<()> {
    let (card_shots, ids) = res?;
    if !card_shots.is_empty() {
        pasitos!(dip push_back Publish {
            env: send_to,
            facet,
            items: card_shots,
            and_then: pasitos::dip::PublishAndThen::SendCards {
                ids,
                page_limit,
            },
        });
    }
    Ok(())
}

pub type SendListsResult = Result<(Vec<serde_json::Value>, Vec<i32>)>;
pub async fn send_lists(mut ids: Vec<i32>, page_limit: NonZeroUsize) -> SendListsResult {
    let mut ids_to_get: Vec<i32> = vec![];
    while let Some(id) = ids.pop() {
        ids_to_get.push(id);
        if ids_to_get.len() >= page_limit.get() {
            break;
        }
    }
    let settings = settings!(db).clone();
    let op_mode = OpMode::Prod;
    let pool = pool_db::get(PoolDb::Pg, settings, op_mode).await?;

    #[derive(sqlx::FromRow)]
    struct Ret {
        list_shots: Option<Vec<serde_json::Value>>,
    }
    let ret = sqlx::query_as!(
        Ret,
        "select array_agg(value) as list_shots from list_shots_to_send($1)",
        &ids_to_get
    )
    .fetch_one(pool_db::as_ref!(Pg pool))
    .await?;
    let lists_shots = ret.list_shots.unwrap();
    Ok((lists_shots, ids))
}
pub fn send_lists_sync(
    res: SendListsResult,
    facet: Facet,
    send_to: Env,
    page_limit: NonZeroUsize,
) -> Result<()> {
    let (list_shots, ids) = res?;
    if !list_shots.is_empty() {
        pasitos!(dip push_back Publish {
            env: send_to,
            facet,
            items: list_shots,
            and_then: pasitos::dip::PublishAndThen::SendLists {
                ids,
                page_limit,
            },
        });
    }
    Ok(())
}


#[derive(Debug)]
pub struct OfferToCheck {
    pub external_id: u64,
}

impl_try_from!(&CheckRequestParamsSourceDataItem => OfferToCheck, anyhow::Error, from, {
    from.external_id().map(|external_id| Self { external_id})
    // Ok(Self {
    //     external_id: from.external_id()?,
    // })
});

pub type SelectDeletedResult = Result<Option<Vec<i64>>>;
pub async fn select_deleted(
    offers_to_check: Vec<OfferToCheck>,
) -> Result<Option<Vec<i64>>> {
    let avito_ids: Vec<i64> = offers_to_check
        .into_iter()
        .map(|OfferToCheck { external_id }| external_id as i64)
        .collect::<Vec<_>>();
    let settings = settings!(db).clone();
    let op_mode = OpMode::Prod;
    let pool = pool_db::get(PoolDb::Pg, settings, op_mode).await?;

    #[derive(sqlx::FromRow)]
    struct Ret {
        avito_ids: Option<Vec<i64>>,
    }
    let ret = sqlx::query_as!(Ret, "select deleted($1) as avito_ids", &avito_ids)
        .fetch_one(pool_db::as_ref!(Pg pool))
        .await?;
    Ok(ret
        .avito_ids
        .and_then(|ids| if ids.is_empty() { None } else { Some(ids) }))
}

pub fn select_deleted_sync(
    res: SelectDeletedResult,
    env: Env,
    delivery_tag: u64,
    request: CheckRequest,
) -> Result<()> {
    if let Some(deleted) = res? {
        trace!("check found {} deleted", deleted.len());
        pasitos!(dip_publish push_back Deleted {
            env,
            delivery_tag,
            deleted,
            request,
        });
    } else {
        pasitos!(dip_check_ack push_back Ack {env, delivery_tag});
    }
    Ok(())
}
