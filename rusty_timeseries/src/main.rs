use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::interval;
use warp::Filter;

const SENSOR_NAME_SIZE: usize = 32;
const TIMESTAMP_SIZE: usize = 32;
const VALUE_SIZE: usize = std::mem::size_of::<f64>();
const FLAG_SIZE: usize = std::mem::size_of::<u8>(); // Optional flag
const TIMESERIES_ID_SIZE: usize = 32;

const ROW_SIZE: usize =
    SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE + FLAG_SIZE + TIMESERIES_ID_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TimeseriesData {
    sensor_name: String,
    timestamp: String,
    value: f64,
    fc1_flag: Option<u8>,  // Fault condition flag
    timeseries_id: String, // Associated Brick TimeseriesId
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TimeseriesReference {
    timeseries_id: String,
    stored_at: String,
}

struct Table {
    num_rows: u32,
    pages: Vec<Option<Box<[u8]>>>,
    file: File, // Add a file handle for disk persistence
}

impl Table {
    fn new(filename: &str) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
            .expect("Unable to open or create file");

        let mut table = Table {
            num_rows: 0,
            pages: Vec::with_capacity(TABLE_MAX_PAGES),
            file,
        };
        table.pages.resize_with(TABLE_MAX_PAGES, || None);
        table.load_from_disk();

        table
    }

    fn load_from_disk(&mut self) {
        self.file
            .seek(SeekFrom::Start(0))
            .expect("Error seeking file");
        let mut buffer = vec![0; PAGE_SIZE];
        for i in 0..TABLE_MAX_PAGES {
            match self.file.read_exact(&mut buffer) {
                Ok(_) => {
                    self.pages[i] = Some(buffer.clone().into_boxed_slice());
                }
                Err(_) => break, // Stop loading if we reach the end of the file
            }
        }

        self.num_rows = self.file.metadata().unwrap().len() as u32 / ROW_SIZE as u32;
    }

    fn save_to_disk(&mut self) {
        self.file
            .seek(SeekFrom::Start(0))
            .expect("Error seeking file");
        for page in &self.pages {
            if let Some(data) = page {
                self.file.write_all(data).expect("Error writing to file");
            }
        }
    }

    fn insert_timeseries_data(&mut self, data: TimeseriesData) -> Result<(), String> {
        if self.num_rows as usize >= TABLE_MAX_ROWS {
            return Err("Table full.".into());
        }
        let row_num = self.num_rows;
        let row_slot = self.row_slot(row_num);
        serialize_row(&data, row_slot);
        self.num_rows += 1;

        self.save_to_disk();

        Ok(())
    }

    fn update_timeseries_data(&mut self, data: TimeseriesData) -> Result<(), String> {
        for i in 0..self.num_rows {
            let row_slot = self.row_slot(i);
            let row = deserialize_row(row_slot);
            if row.timestamp == data.timestamp && row.timeseries_id == data.timeseries_id {
                serialize_row(&data, row_slot);
                self.save_to_disk();
                return Ok(());
            }
        }
        Err("Row not found.".into())
    }

    fn query_timeseries_data_by_id(
        &self,
        timeseries_id: &str,
        start_time: &str,
        end_time: &str,
    ) -> Vec<TimeseriesData> {
        let mut results: Vec<TimeseriesData> = Vec::new();
        for i in 0..self.num_rows {
            let row_slot = &self.pages[(i as usize / ROWS_PER_PAGE) as usize]
                .as_ref()
                .unwrap()[(i as usize % ROWS_PER_PAGE) * ROW_SIZE..];
            let row = deserialize_row(row_slot);
            if row.timeseries_id == timeseries_id
                && *row.timestamp >= *start_time
                && *row.timestamp <= *end_time
            {
                results.push(row);
            }
        }
        results
    }

    fn row_slot(&mut self, row_num: u32) -> &mut [u8] {
        let page_num = (row_num as usize) / ROWS_PER_PAGE;
        if self.pages[page_num].is_none() {
            self.pages[page_num] = Some(vec![0; PAGE_SIZE].into_boxed_slice());
        }
        let page = self.pages[page_num].as_mut().unwrap();
        let row_offset = (row_num as usize) % ROWS_PER_PAGE;
        &mut page[row_offset * ROW_SIZE..(row_offset + 1) * ROW_SIZE]
    }
}

fn serialize_row(row: &TimeseriesData, destination: &mut [u8]) {
    let sensor_name_bytes = row.sensor_name.as_bytes();
    let sensor_name_len = sensor_name_bytes.len().min(SENSOR_NAME_SIZE);
    destination[..sensor_name_len].copy_from_slice(&sensor_name_bytes[..sensor_name_len]);
    for i in sensor_name_len..SENSOR_NAME_SIZE {
        destination[i] = 0; // Padding with zeros
    }

    let timestamp_bytes = row.timestamp.as_bytes();
    let timestamp_len = timestamp_bytes.len().min(TIMESTAMP_SIZE);
    destination[SENSOR_NAME_SIZE..SENSOR_NAME_SIZE + timestamp_len]
        .copy_from_slice(&timestamp_bytes[..timestamp_len]);
    for i in SENSOR_NAME_SIZE + timestamp_len..SENSOR_NAME_SIZE + TIMESTAMP_SIZE {
        destination[i] = 0; // Padding with zeros
    }

    let value_bytes = row.value.to_ne_bytes();
    destination[SENSOR_NAME_SIZE + TIMESTAMP_SIZE..SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE]
        .copy_from_slice(&value_bytes);

    if let Some(flag) = row.fc1_flag {
        destination[SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE] = flag;
    } else {
        destination[SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE] = 0; // Default flag value if None
    }

    let timeseries_id_bytes = row.timeseries_id.as_bytes();
    let timeseries_id_len = timeseries_id_bytes.len().min(TIMESERIES_ID_SIZE);
    destination[SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE + FLAG_SIZE
        ..SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE + FLAG_SIZE + timeseries_id_len]
        .copy_from_slice(&timeseries_id_bytes[..timeseries_id_len]);
    for i in SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE + FLAG_SIZE + timeseries_id_len
        ..SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE + FLAG_SIZE + TIMESERIES_ID_SIZE
    {
        destination[i] = 0; // Padding with zeros
    }
}

fn deserialize_row(source: &[u8]) -> TimeseriesData {
    let sensor_name = String::from_utf8(source[..SENSOR_NAME_SIZE].to_vec())
        .unwrap()
        .trim_end_matches(char::from(0))
        .to_string();
    let timestamp =
        String::from_utf8(source[SENSOR_NAME_SIZE..SENSOR_NAME_SIZE + TIMESTAMP_SIZE].to_vec())
            .unwrap()
            .trim_end_matches(char::from(0))
            .to_string();
    let value = f64::from_ne_bytes(
        source[SENSOR_NAME_SIZE + TIMESTAMP_SIZE..SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE]
            .try_into()
            .unwrap(),
    );
    let fc1_flag = if source[SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE] != 0 {
        Some(source[SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE])
    } else {
        None
    };
    let timeseries_id = String::from_utf8(
        source[SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE + FLAG_SIZE
            ..SENSOR_NAME_SIZE + TIMESTAMP_SIZE + VALUE_SIZE + FLAG_SIZE + TIMESERIES_ID_SIZE]
            .to_vec(),
    )
    .unwrap()
    .trim_end_matches(char::from(0))
    .to_string();

    TimeseriesData {
        sensor_name,
        timestamp,
        value,
        fc1_flag,
        timeseries_id,
    }
}

#[tokio::main]
async fn main() {
    let table = Arc::new(Mutex::new(Table::new("brick_timeseries.db")));

    // Start the fault detection task
    let detection_table = table.clone();
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(300)); // Default 5 minutes
        loop {
            interval.tick().await;
            run_fault_detection(&detection_table);
        }
    });

    start_http_server(table.clone());

    let mut input = String::new();
    loop {
        input.clear();
        print!("db > ");
        io::stdout().flush().unwrap();
        io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();

        if input.starts_with("insert") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() < 5 {
                println!(
                    "Usage: insert <sensor_name> <timestamp> <value> <timeseries_id> [fc1_flag]"
                );
                continue;
            }

            let sensor_name = parts[1].to_string();
            let timestamp = parts[2].to_string();
            let value: f64 = parts[3].parse().unwrap_or(0.0);
            let timeseries_id = parts[4].to_string();
            let fc1_flag = if parts.len() > 5 {
                Some(parts[5].parse().unwrap_or(0))
            } else {
                None
            };

            let data = TimeseriesData {
                sensor_name,
                timestamp,
                value,
                fc1_flag,
                timeseries_id,
            };

            let mut table = table.lock().unwrap();
            if table.insert_timeseries_data(data).is_err() {
                println!("Error: Table Full");
            } else {
                println!("Inserted successfully");
            }
        } else if input.starts_with("set_interval") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() == 2 {
                if let Ok(seconds) = parts[1].parse::<u64>() {
                    let interval = Duration::from_secs(seconds);
                    let cloned_table = table.clone(); // Clone here to avoid moving the original
                    tokio::spawn(async move {
                        run_fault_detection(&cloned_table);
                        tokio::time::sleep(interval).await;
                    });
                    println!("Interval set to {} seconds.", seconds);
                } else {
                    println!("Invalid interval value.");
                }
            }
        } else if input.starts_with("select") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() != 4 {
                println!("Usage: select <timeseries_id> <start_time> <end_time>");
                continue;
            }

            let timeseries_id = parts[1].to_string();
            let start_time = parts[2].to_string();
            let end_time = parts[3].to_string();

            let table = table.lock().unwrap();
            let results = table.query_timeseries_data_by_id(&timeseries_id, &start_time, &end_time);
            for result in results {
                println!("{:?}", result);
            }
        } else if input == ".exit" {
            println!("Exiting...");
            break;
        } else {
            println!("Unrecognized command: '{}'", input);
        }
    }
}

fn start_http_server(table: Arc<Mutex<Table>>) {
    let log_table = table.clone();
    let log_route = warp::post()
        .and(warp::path("telemetry"))
        .and(warp::body::json())
        .and(warp::any().map(move || log_table.clone()))
        .and_then(|data, table| log_and_store_telemetry(data, table));

    let query_table_by_id = table.clone();
    let query_route_by_id = warp::get()
        .and(warp::path("query_by_id"))
        .and(warp::query::<QueryParamsById>())
        .and(warp::any().map(move || query_table_by_id.clone()))
        .and_then(|params, table| query_telemetry_by_id(params, table));

    let routes = log_route.or(query_route_by_id);

    tokio::spawn(async move {
        warp::serve(routes).run(([127, 0, 0, 1], 8000)).await;
    });
}

async fn log_and_store_telemetry(
    data: TimeseriesData,
    table: Arc<Mutex<Table>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut table = table.lock().unwrap();
    if table.insert_timeseries_data(data).is_err() {
        return Ok(warp::reply::with_status(
            "Table Full",
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ));
    }
    Ok(warp::reply::with_status(
        "Inserted",
        warp::http::StatusCode::OK,
    ))
}

async fn query_telemetry_by_id(
    params: QueryParamsById,
    table: Arc<Mutex<Table>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let table = table.lock().unwrap();
    let results = table.query_timeseries_data_by_id(
        &params.timeseries_id,
        &params.start_time,
        &params.end_time,
    );
    Ok(warp::reply::json(&results))
}

#[derive(Debug, Deserialize)]
struct QueryParamsById {
    timeseries_id: String,
    start_time: String,
    end_time: String,
}

fn run_fault_detection(table: &Arc<Mutex<Table>>) {
    let mut table = table.lock().unwrap();

    // Example fault detection logic:
    let threshold = 0.95;
    let timeseries_id = "8f541ba4-c437-43ba-ba1d-5c946583fe54"; // Example timeseries ID

    let results = table.query_timeseries_data_by_id(
        timeseries_id,
        "2024-08-28T12:00:00Z", // Example start time
        "2024-08-28T12:05:00Z", // Example end time
    );

    for mut result in results {
        if result.value > threshold {
            result.fc1_flag = Some(1); // Mark the fault
                                       // Update the stored row with the new fault status
            table
                .update_timeseries_data(result)
                .expect("Failed to update row");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_query_timeseries_data_by_id() {
        let mut table = Table::new("test.db");

        let data = TimeseriesData {
            sensor_name: "Sensor1".into(),
            timestamp: "2024-08-28T12:00:00Z".into(),
            value: 22.5,
            fc1_flag: Some(1),
            timeseries_id: "8f541ba4-c437-43ba-ba1d-5c946583fe54".into(),
        };

        assert!(table.insert_timeseries_data(data.clone()).is_ok());

        let results = table.query_timeseries_data_by_id(
            "8f541ba4-c437-43ba-ba1d-5c946583fe54",
            "2024-08-28T12:00:00Z",
            "2024-08-28T12:01:00Z",
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].sensor_name, data.sensor_name);
        assert_eq!(results[0].value, data.value);
        assert_eq!(results[0].fc1_flag, data.fc1_flag);
        assert_eq!(results[0].timeseries_id, data.timeseries_id);
    }

    #[test]
    fn test_insert_when_table_is_full() {
        let mut table = Table::new("test.db");

        for _ in 0..TABLE_MAX_ROWS {
            let data = TimeseriesData {
                sensor_name: "Sensor1".into(),
                timestamp: "2024-08-28T12:00:00Z".into(),
                value: 22.5,
                fc1_flag: Some(1),
                timeseries_id: "8f541ba4-c437-43ba-ba1d-5c946583fe54".into(),
            };
            assert!(table.insert_timeseries_data(data).is_ok());
        }

        let data = TimeseriesData {
            sensor_name: "Sensor1".into(),
            timestamp: "2024-08-28T12:00:00Z".into(),
            value: 22.5,
            fc1_flag: Some(1),
            timeseries_id: "8f541ba4-c437-43ba-ba1d-5c946583fe54".into(),
        };
        assert!(table.insert_timeseries_data(data).is_err());
    }

    #[test]
    fn test_query_empty_table() {
        let table = Table::new("test.db");
        let results = table.query_timeseries_data_by_id(
            "nonexistent_id",
            "2024-08-28T12:00:00Z",
            "2024-08-28T12:01:00Z",
        );
        assert!(results.is_empty());
    }

    #[test]
    fn test_simple_fault_detection() {
        let mut table = Table::new("test.db");

        let data = vec![
            TimeseriesData {
                sensor_name: "Sa_FanSpeed".into(),
                timestamp: "2024-08-28T12:00:00Z".into(),
                value: 0.8,
                fc1_flag: None,
                timeseries_id: "8f541ba4-c437-43ba-ba1d-5c946583fe54".into(),
            },
            TimeseriesData {
                sensor_name: "Sa_FanSpeed".into(),
                timestamp: "2024-08-28T12:01:00Z".into(),
                value: 0.9,
                fc1_flag: None,
                timeseries_id: "8f541ba4-c437-43ba-ba1d-5c946583fe54".into(),
            },
            TimeseriesData {
                sensor_name: "Sa_FanSpeed".into(),
                timestamp: "2024-08-28T12:02:00Z".into(),
                value: 1.0,
                fc1_flag: None,
                timeseries_id: "8f541ba4-c437-43ba-ba1d-5c946583fe54".into(),
            },
        ];

        for d in data {
            assert!(table.insert_timeseries_data(d).is_ok());
        }

        run_fault_detection(&Arc::new(Mutex::new(table)));

        let results = table.query_timeseries_data_by_id(
            "8f541ba4-c437-43ba-ba1d-5c946583fe54",
            "2024-08-28T12:00:00Z",
            "2024-08-28T12:03:00Z",
        );

        let fault_count = results.iter().filter(|r| r.fc1_flag == Some(1)).count();
        assert_eq!(fault_count, 1, "Expected one fault condition.");
    }
}
