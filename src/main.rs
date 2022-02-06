use std::error::Error;
use std::io;
use std::process;
use std::io::Cursor;
use std::path::Path;
use std::ffi::OsStr;

use std::collections::HashMap;
use futures::stream::StreamExt;

type Record = HashMap<String, String>;

fn get_extension_from_filename(filename: &str) -> Option<&str> {
    // URLs may contain query parameters we need to remove
    let new_filename = filename.split('?').next().unwrap_or("");
    Path::new(new_filename)
        .extension()
        .and_then(OsStr::to_str)
}

fn process_csv() -> Result<Vec<Record>, Box<dyn Error>> {
    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::Reader::from_reader(io::stdin());
    let mut records: Vec<Record> = Vec::new();

    for result in rdr.deserialize() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record: Record = result?;
        // println!("Record: {:?}", record);
        records.push(record);
    }

    println!("Length of records: {:?}", records.len());

    Ok(records)
}

async fn fetch_url(url: &String, file_name: &String) -> Result<(), Box<dyn Error>> {
    match reqwest::get(url).await {
        Ok(response) => {
            let mut file = std::fs::File::create(file_name)?;
            match response.bytes().await {
                Ok(bytes) => {
                    let mut content =  Cursor::new(bytes);
                    std::io::copy(&mut content, &mut file)?;
                }
                Err(_) => println!("ERROR reading {}", url),
            }
        }
        Err(error) => println!("ERROR downloading {}: {}", url, error),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let records = process_csv().expect("Could not process the provided CSV file");

    let fetches = futures::stream::iter(
        records.into_iter().map(|record| {
            async move {
                let work_email = &record["Work Email"].trim().to_string();
                let personal_email = &record["Personal Email"].trim().to_string();
                let profile_url = &record["Picture URL"].trim().to_string();
                let use_email = if work_email != "" { work_email } else { personal_email };
                let file_name = format!("Profile_Pictures/{}.{:}", use_email, get_extension_from_filename(&profile_url).unwrap_or(""));
                if profile_url != "" && file_name != "" && !Path::new(&file_name).exists() {
                    println!("Downloading: {:} ...", file_name);
                    if let Err(error) = fetch_url(&profile_url, &file_name).await {
                        println!("Error fetching file: {}", error);
                        process::exit(1);
                    }
                }
                else {
                    println!("Skipping profile URL {} with filename: {}", profile_url, file_name);
                }
            }
    })
    ).buffer_unordered(8).collect::<Vec<()>>();
    
    println!("Waiting...");
    fetches.await;

    Ok(())
}
