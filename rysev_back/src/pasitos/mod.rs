use super::*;

use futures::StreamExt;
pub mod elastic;
pub mod store;

pasitos!(fut_queue, run_for;
    init {
        let start = std::time::Instant::now();
        let opt = (*OPT.read().unwrap()).clone();
        let opt = opt.unwrap();
        match &opt.cmd {
            Some(Command::ForAnalytics { }) => {
                let output_filepath = (*PARAMS.read().unwrap())
                    .as_ref()
                    .unwrap()
                    .run_dir
                    .clone()
                    .join(std::path::Path::new(
                        "for-analytics.rgn_habit.offer", // Очень странное название для файла,
                                                         // который:
                                                         // - предназначен для Рысева
                                                         // - содержит кроме квартир ще и
                                                         // кооттеджи
                    ));
                let output_filepath = get_csv_gz_filepath(output_filepath);
                let temp_filepath =
                    output_filepath
                        .parent()
                        .unwrap()
                        .join(std::path::Path::new(&{
                            let mut ret = std::ffi::OsString::from(".");
                            ret.push(output_filepath.file_name().unwrap());
                            ret
                        }));


                let file = std::fs::File::create(&temp_filepath)?;
                let encoder = GzEncoder::new(file, Compression::default());
                let mut buf_writer = BufWriter::new(encoder);
                // UTF-8 BOM
                buf_writer.write_all(&[0xEF, 0xBB, 0xBF])?;
                let writer = WriterBuilder::new().delimiter(b';').from_writer(buf_writer);
                // поля для заголовков в итоговом файле
                let fields = vec![
                    // "guid",
                    "source",
                    "market",
                    "district",
                    "rooms",
                    "address",
                    "floor",
                    "flatType",
                    "areaTotal",
                    "areaLive",
                    "areaKitchen",
                    "wc",
                    "price",
                    "agency",
                    "phone",
                    "additional",
                    "saleType",
                    "condition",
                    "rendEnd",
                    "url",
                    "lat",
                    "lon",
                ];

                let fields = fields.iter().map(|&s| s.to_string()).collect();

                let specfic = Arc::new(RwLock::new(CsvSpecific {
                    writer,
                    fields,
                    temp_filepath,
                    output_filepath,
                }));

                start_export(ExportFor::Analytics(ForSiteFacet::Habit), specfic.clone())?;
                start_export(ExportFor::Analytics(ForSiteFacet::Cottage), specfic.clone())?;
            }
            None => {
                bail!("No command specified. Check with --help");
            }
        }
    }
    on_complete {
        info!(
            "{}, complete",
            arrange_millis::get(std::time::Instant::now().duration_since(start).as_millis()),
        );
        std::process::exit(0);
    }
    on_next_end {
    }
    demoras {
        demora ExportRetryFetch ({
            request: ElasticRequest,
            content: elastic_scan_specific::ElasticContent,
            mode: ExportFor,
        }) {
            pasitos!(elastic push_front Fetch {
                request,
                content,
                mode,
            });
        }
    }
    pasos elastic {
        max_at_once: settings!(elastic.max_at_once);
        paso Fetch ({
            request: ElasticRequest,
            content: elastic_scan_specific::ElasticContent,
            mode: ExportFor,
        }) -> ({
            res: pasitos::elastic::FetchResult,
            mode: ExportFor,
        }) {
            let res = pasitos::elastic::fetch(content, request).await;
        } => sync {
            pasitos::elastic::fetch_sync(res, mode)?;
        }
    }
    pasos store {
        max_at_once: 1;
        paso Save ({
            content_shared: elastic_scan_specific::ElasticContentShared,
            next_fetch: Option<crate::pasitos::pasos::elastic::Arg>,
        }) -> ({
            res: pasitos::store::SaveResult,
            content_shared: elastic_scan_specific::ElasticContentShared,
            next_fetch: Option<crate::pasitos::pasos::elastic::Arg>,
        }) {
            let res = pasitos::store::save(&content_shared).await;
        } => sync {
            pasitos::store::save_sync(res, content_shared, next_fetch)?;
        }
    }
);

fn start_export(mode: ExportFor, specific: ElasticContentSharedInnerSpecific) -> Result<()> {
    match mode {
        ExportFor::Analytics(facet) => {
            // let facet = MlsFacet::RgnHabitSale;
            let facet = MlsFacet::from(facet);
            let shared = std::sync::Arc::new(std::sync::RwLock::new(
                elastic_scan_specific::ElasticContentSharedInner::new(specific)?,
            ));
            let content = elastic_scan_specific::ElasticContent::new(facet, shared);
            let request = content.new_request(mode);
            pasitos!(elastic push_back Fetch { request, content, mode });
        }
    }
    Ok(())
}
