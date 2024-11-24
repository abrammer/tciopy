use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict};
use serde::Serialize;

#[derive(Serialize)]
enum Value {
    Str(String),
    Int(i64),
    Float(f64),
}

#[pyfunction]
fn parse_adeck(filepath: &str, py: Python) -> PyResult<PyObject> {
    let file = File::open(filepath).expect("Unable to open file");
    let reader = BufReader::new(file);

    let mut result: HashMap<String, Vec<Value>> = HashMap::new();
    result.insert("basin".to_string(), Vec::new());
    result.insert("number".to_string(), Vec::new());
    result.insert("datetime".to_string(), Vec::new());
    result.insert("tnum".to_string(), Vec::new());
    result.insert("tech".to_string(), Vec::new());
    result.insert("tau".to_string(), Vec::new());
    result.insert("lat".to_string(), Vec::new());
    result.insert("lon".to_string(), Vec::new());
    result.insert("vmax".to_string(), Vec::new());
    result.insert("mslp".to_string(), Vec::new());
    result.insert("type".to_string(), Vec::new());
    result.insert("rad".to_string(), Vec::new());
    result.insert("windcode".to_string(), Vec::new());
    result.insert("rad_NEQ".to_string(), Vec::new());
    result.insert("rad_SEQ".to_string(), Vec::new());
    result.insert("rad_SWQ".to_string(), Vec::new());
    result.insert("rad_NWQ".to_string(), Vec::new());
    result.insert("pouter".to_string(), Vec::new());
    result.insert("router".to_string(), Vec::new());
    result.insert("rmw".to_string(), Vec::new());
    result.insert("gusts".to_string(), Vec::new());
    result.insert("eye".to_string(), Vec::new());
    result.insert("subregion".to_string(), Vec::new());
    result.insert("maxseas".to_string(), Vec::new());
    result.insert("initials".to_string(), Vec::new());
    result.insert("direction".to_string(), Vec::new());
    result.insert("speed".to_string(), Vec::new());
    result.insert("stormname".to_string(), Vec::new());
    result.insert("depth".to_string(), Vec::new());
    result.insert("seas".to_string(), Vec::new());
    result.insert("seascode".to_string(), Vec::new());
    result.insert("seas1".to_string(), Vec::new());
    result.insert("seas2".to_string(), Vec::new());
    result.insert("seas3".to_string(), Vec::new());
    result.insert("seas4".to_string(), Vec::new());
    result.insert("userdefined1".to_string(), Vec::new());
    result.insert("userdata1".to_string(), Vec::new());
    result.insert("userdefined2".to_string(), Vec::new());
    result.insert("userdata2".to_string(), Vec::new());
    result.insert("userdefined3".to_string(), Vec::new());
    result.insert("userdata3".to_string(), Vec::new());
    result.insert("userdefined4".to_string(), Vec::new());
    result.insert("userdata4".to_string(), Vec::new());
    result.insert("userdefined5".to_string(), Vec::new());
    result.insert("userdata5".to_string(), Vec::new());

    for line in reader.lines().filter_map(|line| line.ok()) {
        if let Some(entry) = parse_adeck_line(&line) {
            result.get_mut("basin").unwrap().push(Value::Str(entry.basin));
            result.get_mut("number").unwrap().push(Value::Int(entry.number as i64));
            result.get_mut("datetime").unwrap().push(Value::Str(entry.datetime));
            result.get_mut("tnum").unwrap().push(Value::Int(entry.tnum as i64));
            result.get_mut("tech").unwrap().push(Value::Str(entry.tech));
            result.get_mut("tau").unwrap().push(Value::Int(entry.tau as i64));
            result.get_mut("lat").unwrap().push(Value::Float(entry.lat));
            result.get_mut("lon").unwrap().push(Value::Float(entry.lon));
            result.get_mut("vmax").unwrap().push(Value::Int(entry.vmax.unwrap_or(0) as i64));
            result.get_mut("mslp").unwrap().push(Value::Int(entry.mslp.unwrap_or(0) as i64));
            result.get_mut("type").unwrap().push(Value::Str(entry.storm_type.unwrap_or_default()));
            result.get_mut("rad").unwrap().push(Value::Str(entry.rad.unwrap_or_default()));
            result.get_mut("windcode").unwrap().push(Value::Str(entry.windcode.unwrap_or_default()));
            result.get_mut("rad_NEQ").unwrap().push(Value::Int(entry.rad_NEQ.unwrap_or(0) as i64));
            result.get_mut("rad_SEQ").unwrap().push(Value::Int(entry.rad_SEQ.unwrap_or(0) as i64));
            result.get_mut("rad_SWQ").unwrap().push(Value::Int(entry.rad_SWQ.unwrap_or(0) as i64));
            result.get_mut("rad_NWQ").unwrap().push(Value::Int(entry.rad_NWQ.unwrap_or(0) as i64));
            result.get_mut("pouter").unwrap().push(Value::Int(entry.pouter.unwrap_or(0) as i64));
            result.get_mut("router").unwrap().push(Value::Int(entry.router.unwrap_or(0) as i64));
            result.get_mut("rmw").unwrap().push(Value::Int(entry.rmw.unwrap_or(0) as i64));
            result.get_mut("gusts").unwrap().push(Value::Int(entry.gusts.unwrap_or(0) as i64));
            result.get_mut("eye").unwrap().push(Value::Int(entry.eye.unwrap_or(0) as i64));
            result.get_mut("subregion").unwrap().push(Value::Str(entry.subregion.unwrap_or_default()));
            result.get_mut("maxseas").unwrap().push(Value::Int(entry.maxseas.unwrap_or(0) as i64));
            result.get_mut("initials").unwrap().push(Value::Str(entry.initials.unwrap_or_default()));
            result.get_mut("direction").unwrap().push(Value::Int(entry.direction.unwrap_or(0) as i64));
            result.get_mut("speed").unwrap().push(Value::Int(entry.speed.unwrap_or(0) as i64));
            result.get_mut("stormname").unwrap().push(Value::Str(entry.stormname.unwrap_or_default()));
            result.get_mut("depth").unwrap().push(Value::Str(entry.depth.unwrap_or_default()));
            result.get_mut("seas").unwrap().push(Value::Int(entry.seas.unwrap_or(0) as i64));
            result.get_mut("seascode").unwrap().push(Value::Str(entry.seascode.unwrap_or_default()));
            result.get_mut("seas1").unwrap().push(Value::Int(entry.seas1.unwrap_or(0) as i64));
            result.get_mut("seas2").unwrap().push(Value::Int(entry.seas2.unwrap_or(0) as i64));
            result.get_mut("seas3").unwrap().push(Value::Int(entry.seas3.unwrap_or(0) as i64));
            result.get_mut("seas4").unwrap().push(Value::Int(entry.seas4.unwrap_or(0) as i64));
            result.get_mut("userdefined1").unwrap().push(Value::Str(entry.userdefined1.unwrap_or_default()));
            result.get_mut("userdata1").unwrap().push(Value::Str(entry.userdata1.unwrap_or_default()));
            result.get_mut("userdefined2").unwrap().push(Value::Str(entry.userdefined2.unwrap_or_default()));
            result.get_mut("userdata2").unwrap().push(Value::Str(entry.userdata2.unwrap_or_default()));
            result.get_mut("userdefined3").unwrap().push(Value::Str(entry.userdefined3.unwrap_or_default()));
            result.get_mut("userdata3").unwrap().push(Value::Str(entry.userdata3.unwrap_or_default()));
            result.get_mut("userdefined4").unwrap().push(Value::Str(entry.userdefined4.unwrap_or_default()));
            result.get_mut("userdata4").unwrap().push(Value::Str(entry.userdata4.unwrap_or_default()));
            result.get_mut("userdefined5").unwrap().push(Value::Str(entry.userdefined5.unwrap_or_default()));
            result.get_mut("userdata5").unwrap().push(Value::Str(entry.userdata5.unwrap_or_default()));
        }
    }

    let py_dict = PyDict::new(py);
    for (key, values) in result {
        let py_values: Vec<PyObject> = values.into_iter().map(|v| match v {
            Value::Str(s) => s.into_py(py),
            Value::Int(i) => i.into_py(py),
            Value::Float(f) => f.into_py(py),
        }).collect();
        py_dict.set_item(key, py_values).unwrap();
    }

    Ok(py_dict.into())
}

#[pymodule]
fn adeck_parser(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_adeck, m)?)?;
    Ok(())
}

#[derive(Serialize, Debug)]
struct AdeckEntry {
    basin: String,
    number: u8,
    datetime: String,
    tnum: u8,
    tech: String,
    tau: i16,
    lat: f64,
    lon: f64,
    vmax: Option<u16>,
    mslp: Option<u16>,
    storm_type: Option<String>,
    rad: Option<String>,
    windcode: Option<String>,
    rad_NEQ: Option<u16>,
    rad_SEQ: Option<u16>,
    rad_SWQ: Option<u16>,
    rad_NWQ: Option<u16>,
    pouter: Option<u16>,
    router: Option<u16>,
    rmw: Option<u16>,
    gusts: Option<u16>,
    eye: Option<u16>,
    subregion: Option<String>,
    maxseas: Option<u16>,
    initials: Option<String>,
    direction: Option<u16>,
    speed: Option<u16>,
    stormname: Option<String>,
    depth: Option<String>,
    seas: Option<u16>,
    seascode: Option<String>,
    seas1: Option<u16>,
    seas2: Option<u16>,
    seas3: Option<u16>,
    seas4: Option<u16>,
    userdefined1: Option<String>,
    userdata1: Option<String>,
    userdefined2: Option<String>,
    userdata2: Option<String>,
    userdefined3: Option<String>,
    userdata3: Option<String>,
    userdefined4: Option<String>,
    userdata4: Option<String>,
    userdefined5: Option<String>,
    userdata5: Option<String>,
}

fn parse_adeck_line(line: &str) -> Option<AdeckEntry> {
    let fields: Vec<&str> = line.split(',').map(|f| f.trim()).collect();
    if fields.len() < 8 {
        return None; // Skip malformed lines
    }

    let lat = parse_coordinate(fields[6], 'N', 'S')?;
    let lon = parse_coordinate(fields[7], 'E', 'W')?;

    Some(AdeckEntry {
        basin: fields[0].to_string(),
        number: fields[1].parse().unwrap_or(0),
        datetime: fields[2].to_string(),
        tnum: fields[3].parse().unwrap_or(0),
        tech: fields[4].to_string(),
        tau: fields[5].parse().unwrap_or(0),
        lat,
        lon,
        vmax: fields.get(8).and_then(|&s| s.parse().ok()),
        mslp: fields.get(9).and_then(|&s| s.parse().ok()),
        storm_type: fields.get(10).map(|&s| s.to_string()),
        rad: fields.get(11).map(|&s| s.to_string()),
        windcode: fields.get(12).map(|&s| s.to_string()),
        rad_NEQ: fields.get(13).and_then(|&s| s.parse().ok()),
        rad_SEQ: fields.get(14).and_then(|&s| s.parse().ok()),
        rad_SWQ: fields.get(15).and_then(|&s| s.parse().ok()),
        rad_NWQ: fields.get(16).and_then(|&s| s.parse().ok()),
        pouter: fields.get(17).and_then(|&s| s.parse().ok()),
        router: fields.get(18).and_then(|&s| s.parse().ok()),
        rmw: fields.get(19).and_then(|&s| s.parse().ok()),
        gusts: fields.get(20).and_then(|&s| s.parse().ok()),
        eye: fields.get(21).and_then(|&s| s.parse().ok()),
        subregion: fields.get(22).map(|&s| s.to_string()),
        maxseas: fields.get(23).and_then(|&s| s.parse().ok()),
        initials: fields.get(24).map(|&s| s.to_string()),
        direction: fields.get(25).and_then(|&s| s.parse().ok()),
        speed: fields.get(26).and_then(|&s| s.parse().ok()),
        stormname: fields.get(27).map(|&s| s.to_string()),
        depth: fields.get(28).map(|&s| s.to_string()),
        seas: fields.get(29).and_then(|&s| s.parse().ok()),
        seascode: fields.get(30).map(|&s| s.to_string()),
        seas1: fields.get(31).and_then(|&s| s.parse().ok()),
        seas2: fields.get(32).and_then(|&s| s.parse().ok()),
        seas3: fields.get(33).and_then(|&s| s.parse().ok()),
        seas4: fields.get(34).and_then(|&s| s.parse().ok()),
        userdefined1: fields.get(35).map(|&s| s.to_string()),
        userdata1: fields.get(36).map(|&s| s.to_string()),
        userdefined2: fields.get(37).map(|&s| s.to_string()),
        userdata2: fields.get(38).map(|&s| s.to_string()),
        userdefined3: fields.get(39).map(|&s| s.to_string()),
        userdata3: fields.get(40).map(|&s| s.to_string()),
        userdefined4: fields.get(41).map(|&s| s.to_string()),
        userdata4: fields.get(42).map(|&s| s.to_string()),
        userdefined5: fields.get(43).map(|&s| s.to_string()),
        userdata5: fields.get(44).map(|&s| s.to_string()),
    })
}

fn parse_coordinate(coord: &str, positive_hemisphere: char, negative_hemisphere: char) -> Option<f64> {
    if coord.len() < 2 {
        return None;
    }

    let (value, hemisphere) = coord.split_at(coord.len() - 1);
    let value: f64 = value.parse().ok()?;
    let sign = match hemisphere.chars().next()? {
        h if h == positive_hemisphere => 0.1,
        h if h == negative_hemisphere => -0.1,
        _ => return None,
    };

    Some(value * sign)
}

