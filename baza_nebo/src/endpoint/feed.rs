use super::*;

#[derive(Debug, serde::Deserialize)]
pub struct QueryFilter {
    r#for: BrokerName,
    // date: Option<String>, // передача даты будет реализована вторым шагом позже
}

#[derive(Debug, PartialEq, serde::Deserialize, Display, Clone)]
#[serde(rename_all = "lowercase")]
pub enum BrokerName {
    Vladis,
    Mlscenter,
    Ogrk24,
    Etagi,
}

use axum::{
    http::header::CONTENT_TYPE,
    http::StatusCode,
    response::{IntoResponse, Response},
};

use strum::Display;
// use tokio::fs::File;
// use tokio_util::io::ReaderStream;
// use tokio::io::AsyncReadExt;

pub async fn feed(Query(params): Query<QueryFilter>) -> impl IntoResponse {
    let broker_name = params.r#for;

    match get_file_pathlar(broker_name.clone()).await {
        Err(err) => {
            eprintln!("Ошибка при получении файлов для обработки: {:?}", err);

            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR) // код 500
                .header(CONTENT_TYPE, "text/plain")
                .body("Error while receiving files for processing".into_response())
                .unwrap();
        }
        Ok(file_pathlar) => {
            println!("\nФайлы для обработки: {:#?}", file_pathlar);

            match process_xml(file_pathlar) {
                Ok(result) => {
                    return Response::builder()
                        .header(CONTENT_TYPE, "application/xml")
                        .body(result.into_response())
                        .unwrap();
                }
                Err(e) => {
                    eprintln!("Ошибка формирования XML: {}", e);

                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header(CONTENT_TYPE, "text/plain")
                        .body("XML generation error".into_response())
                        .unwrap();
                }
            }
        }
    }
}

use std::fs;
use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::{Context, Result};

use quick_xml::events::{BytesDecl, BytesStart, BytesText, Event, Event::Decl};
use quick_xml::reader::Reader;
use quick_xml::writer::Writer;
use std::io::{BufReader, Cursor};

use flate2::read::GzDecoder;

fn process_xml(files: HashMap<String, PathBuf>) -> Result<String, Box<dyn std::error::Error>> {
    // Используем Cursor для записи XML в память // чтение и запись буфера как потока с текущей позицией (указателем).
    let mut output = Cursor::new(Vec::new());
    let mut xml_writer = Writer::new(&mut output);

    xml_writer.write_event(Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

    let el_start = BytesStart::new("realty-feed").with_attributes([(
        "xmlns",
        "http://webmaster.yandex.ru/schemas/feed/realty/2010-06",
    )]);
    let el_end = el_start.to_end();
    xml_writer.write_event(Event::Start(el_start.clone()))?;

    let current_time = chrono::Utc::now().to_rfc3339();

    xml_writer.write_event(Event::Start(BytesStart::new("generation-date")))?;
    xml_writer.write_event(Event::Text(BytesText::from_escaped(&current_time)))?;
    xml_writer.write_event(Event::End(BytesStart::new("generation-date").to_end()))?;

    // обработка файла
    for (broker_name, file_path) in files {
        let file = std::fs::File::open(&file_path)?;

        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);

        let mut xml_reader = Reader::from_reader(reader);

        // xml_reader.config_mut().trim_text(true); // а надо?

        let mut buf = Vec::new();

        let mut inside_target_element = false;

        loop {
            match xml_reader.read_event_into(&mut buf)? {
                Event::Start(e) if e.name().as_ref() == b"realty-feed" => {
                    continue;
                }
                Event::End(e) if e.name().as_ref() == b"realty-feed" => {
                    continue;
                }
                Event::Start(e) if e.name().as_ref() == b"generation-date" => {
                    inside_target_element = true;
                }
                Event::End(e) if e.name().as_ref() == b"generation-date" => {
                    inside_target_element = false;
                }
                Event::Decl(_e) => {
                    // заголовок <?xml version="1.0" ...
                    continue;
                }
                Event::Start(e) if e.name().as_ref() == b"offer" => {
                    // Записываем элемент <offer>
                    xml_writer.write_event(Event::Start(e))?;

                    // Добавляем элемент <nebo-broker>
                    xml_writer.write_event(Event::Start(BytesStart::new("nebo-broker")))?;
                    xml_writer.write_event(Event::Text(BytesText::from_escaped(
                        broker_name.to_string(),
                    )))?;
                    xml_writer.write_event(Event::End(BytesStart::new("nebo-broker").to_end()))?;
                }
                Event::End(e) if e.name().as_ref() == b"offer" => {
                    // Закрываем элемент <offer>
                    xml_writer.write_event(Event::End(e))?;
                }
                Event::Text(e) => {
                    if !inside_target_element {
                        xml_writer.write_event(Event::Text(e))?;
                    }
                }
                Event::Eof => break,
                e => xml_writer.write_event(e)?,
            }
            buf.clear();
        }
    }

    // Закрываем корневой элемент
    xml_writer.write_event(Event::End(el_end))?;

    // Преобразуем записанные данные в строку
    let result = String::from_utf8(output.into_inner())?;
    Ok(result)
}

pub async fn get_file_pathlar(
    broker_name: BrokerName,
) -> Result<HashMap<String, PathBuf>, Box<dyn std::error::Error>> {
    let broker_name = broker_name.to_string().to_lowercase();

    let mut latest_filelar = HashMap::new();

    let base_path = "./data/raw/";
    let mut dir = tokio::fs::read_dir(&base_path)
        .await
        .map_err(|err| anyhow!("read_dir{:?}: {}", base_path, err))?;

    // перебираем все папки с брокерами внутри /data/raw/
    while let Some(broker_dir) = dir.next_entry().await? {
        let broker_dir_name = broker_dir
            .file_name()
            .to_str()
            .context("Не получилось узнать имя брокера из broker_dir")?
            .to_string();
        // .ok_or_else(|| anyhow!("Не получилось узнать имя брокера из broker_dir"))?
        // .to_string();

        // Пропускаем папку с именем брокера, которое передали в for=
        if broker_dir_name == broker_name {
            continue;
        }

        let mut broker_dir = tokio::fs::read_dir(broker_dir.path())
            .await
            .map_err(|err| anyhow!("read_dir{:?}: {}", broker_dir, err))?;

        // перебираем все папки внутри /data/raw/брокер
        let mut date_dirlar = Vec::new();
        while let Some(date_dir) = broker_dir.next_entry().await? {
            date_dirlar.push(date_dir.path());
        }

        if !date_dirlar.is_empty() {
            date_dirlar.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

            println!(
                "{}: Самая свежая папка: {:#?}",
                broker_dir_name, date_dirlar[0]
            );

            // перебираем все файлы внутри папки /data/raw/брокер/дата
            for entry in fs::read_dir(date_dirlar[0].clone())? {
                let entry = entry?;
                let path = entry.path();

                if path.is_file() {
                    //latest_filelar.push(path.clone());

                    latest_filelar.insert(broker_dir_name.clone(), path);
                }
            }
        } else {
            println!("{}: Папок с файлами нет", broker_dir_name);
        }
    }

    // println!("latest_filelar: {:#?}", &latest_filelar);

    // Возвращаем список файлов для обработки
    Ok(latest_filelar)
}
