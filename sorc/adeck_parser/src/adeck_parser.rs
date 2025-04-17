use std::fs::File;
use std::io::{BufRead, BufReader};
use serde::Serialize;
use pyo3::prelude::*;
use serde_json;

#[pyfunction]
fn parse_adeck() -> PyResult<()> {
    // Your function implementation here
    Ok(())
}

#[pymodule]
fn adeck_parser(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_adeck, m)?)?;
    Ok(())
}

#[derive(Serialize, Debug)]
struct AdeckEntry {
    basin: String,
    storm_number: u8,
    init_time: String,
    run_id: u8,
    model: String,
    forecast_hour: i16,
    latitude: String,
    longitude: String,
    wind_speed: u16,
    pressure: Option<u16>,
    storm_type: String,
    radii_34: Vec<u16>,
}

fn parse_adeck_line(line: &str) -> Option<AdeckEntry> {
    let fields: Vec<&str> = line.split(',').map(|f| f.trim()).collect();
    if fields.len() < 24 {
        return None; // Skip malformed lines
    }

    Some(AdeckEntry {
        basin: fields[0].to_string(),
        storm_number: fields[1].parse().unwrap_or(0),
        init_time: fields[2].to_string(),
        run_id: fields[3].parse().unwrap_or(0),
        model: fields[4].to_string(),
        forecast_hour: fields[5].parse().unwrap_or(0),
        latitude: fields[6].to_string(),
        longitude: fields[7].to_string(),
        wind_speed: fields[8].parse().unwrap_or(0),
        pressure: fields[9].parse().ok(),
        storm_type: fields[10].to_string(),
        radii_34: vec![
            fields[12].parse().unwrap_or(0),
            fields[13].parse().unwrap_or(0),
            fields[14].parse().unwrap_or(0),
            fields[15].parse().unwrap_or(0),
        ],
        radii_50: vec![
            fields[16].parse().unwrap_or(0),
            fields[17].parse().unwrap_or(0),
            fields[18].parse().unwrap_or(0),
            fields[19].parse().unwrap_or(0),
        ],
        radii_64: vec![
            fields[20].parse().unwrap_or(0),
            fields[21].parse().unwrap_or(0),
            fields[22].parse().unwrap_or(0),
            fields[23].parse().unwrap_or(0),
        ],
        metadata: fields[24..].iter().map(|&f| f.to_string()).collect(),
    })
}

fn read_adeck_file(filepath: &str) -> Vec<AdeckEntry> {
    let file = File::open(filepath).expect("Unable to open file");
    let reader = BufReader::new(file);

    reader
        .lines()
        .filter_map(|line| line.ok())
        .filter_map(|line| parse_adeck_line(&line))
        .collect()
}
