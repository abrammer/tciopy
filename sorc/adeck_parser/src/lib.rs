use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::{Mutex, Arc};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict};
use serde::Serialize;
use arrow::record_batch::RecordBatch;
use arrow::pyarrow::PyArrowConvert;
use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray, StringBuilder, Float64Builder, TimestampNanosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use pyo3::wrap_pyfunction;



#[derive(Serialize)]
enum Value {
    Str(String),
    Int(i32),
    Float(f32),
}

// #[pyfunction]
// fn parse_adeck(filepath: &str, py: Python) -> PyResult<PyObject> {
//     let file = File::open(filepath).expect("Unable to open file");
//     let reader = BufReader::new(file);

//     let result = Mutex::new(HashMap::new());
//     {
//         let mut result = result.lock().unwrap();
//         result.insert("basin".to_string(), Vec::new());
//         result.insert("storm_number".to_string(), Vec::new());
//         result.insert("init_time".to_string(), Vec::new());
//         result.insert("run_id".to_string(), Vec::new());
//         result.insert("model".to_string(), Vec::new());
//         result.insert("forecast_hour".to_string(), Vec::new());
//         result.insert("latitude".to_string(), Vec::new());
//         result.insert("longitude".to_string(), Vec::new());
//         result.insert("wind_speed".to_string(), Vec::new());
//         result.insert("pressure".to_string(), Vec::new());
//         result.insert("storm_type".to_string(), Vec::new());
//         result.insert("rad".to_string(), Vec::new());
//         result.insert("windcode".to_string(), Vec::new());
//         result.insert("rad_NEQ".to_string(), Vec::new());
//         result.insert("rad_SEQ".to_string(), Vec::new());
//         result.insert("rad_SWQ".to_string(), Vec::new());
//         result.insert("rad_NWQ".to_string(), Vec::new());
//         result.insert("pouter".to_string(), Vec::new());
//         result.insert("router".to_string(), Vec::new());
//         result.insert("rmw".to_string(), Vec::new());
//         result.insert("gusts".to_string(), Vec::new());
//         result.insert("eye".to_string(), Vec::new());
//         result.insert("subregion".to_string(), Vec::new());
//         result.insert("maxseas".to_string(), Vec::new());
//         result.insert("initials".to_string(), Vec::new());
//         result.insert("direction".to_string(), Vec::new());
//         result.insert("speed".to_string(), Vec::new());
//         result.insert("stormname".to_string(), Vec::new());
//         result.insert("depth".to_string(), Vec::new());
//         result.insert("seas".to_string(), Vec::new());
//         result.insert("seascode".to_string(), Vec::new());
//         result.insert("seas1".to_string(), Vec::new());
//         result.insert("seas2".to_string(), Vec::new());
//         result.insert("seas3".to_string(), Vec::new());
//         result.insert("seas4".to_string(), Vec::new());
//         result.insert("userdefined1".to_string(), Vec::new());
//         result.insert("userdata1".to_string(), Vec::new());
//         result.insert("userdefined2".to_string(), Vec::new());
//         result.insert("userdata2".to_string(), Vec::new());
//         result.insert("userdefined3".to_string(), Vec::new());
//         result.insert("userdata3".to_string(), Vec::new());
//         result.insert("userdefined4".to_string(), Vec::new());
//         result.insert("userdata4".to_string(), Vec::new());
//         result.insert("userdefined5".to_string(), Vec::new());
//         result.insert("userdata5".to_string(), Vec::new());
//     }

//     let lines: Vec<String> = reader.lines().filter_map(|line| line.ok()).collect();
//     for line in lines.iter() {
//         let fields: Vec<&str> = line.split(',').map(|f| f.trim()).collect();

//         let latitude = parse_coordinate(fields.get(6).unwrap_or(&""), 'N', 'S');
//         let longitude = parse_coordinate(fields.get(7).unwrap_or(&""), 'E', 'W');

//         let mut result = result.lock().unwrap(); // Ensure mutable access to result

//         result.get_mut("basin").unwrap().push(Value::Str(fields.get(0).unwrap_or(&"").to_string()));
//         result.get_mut("storm_number").unwrap().push(Value::Int(fields.get(1).unwrap_or(&"0").parse().unwrap_or(0)));
//         result.get_mut("init_time").unwrap().push(Value::Str(fields.get(2).unwrap_or(&"").to_string()));
//         result.get_mut("run_id").unwrap().push(Value::Int(fields.get(3).unwrap_or(&"0").parse().unwrap_or(0)));
//         result.get_mut("model").unwrap().push(Value::Str(fields.get(4).unwrap_or(&"").to_string()));
//         result.get_mut("forecast_hour").unwrap().push(Value::Int(fields.get(5).unwrap_or(&"0").parse().unwrap_or(0)));
//         result.get_mut("latitude").unwrap().push(Value::Float(latitude.unwrap_or(0.0)));
//         result.get_mut("longitude").unwrap().push(Value::Float(longitude.unwrap_or(0.0)));
//         result.get_mut("wind_speed").unwrap().push(Value::Int(fields.get(8).unwrap_or(&"0").parse().unwrap_or(0)));
//         result.get_mut("pressure").unwrap().push(Value::Int(fields.get(9).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("storm_type").unwrap().push(Value::Str(fields.get(10).unwrap_or(&"").to_string()));
//         // result.get_mut("rad").unwrap().push(Value::Str(fields.get(11).unwrap_or(&"").to_string()));
//         // result.get_mut("windcode").unwrap().push(Value::Str(fields.get(12).unwrap_or(&"").to_string()));
//         // result.get_mut("rad_NEQ").unwrap().push(Value::Int(fields.get(13).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("rad_SEQ").unwrap().push(Value::Int(fields.get(14).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("rad_SWQ").unwrap().push(Value::Int(fields.get(15).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("rad_NWQ").unwrap().push(Value::Int(fields.get(16).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("pouter").unwrap().push(Value::Int(fields.get(17).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("router").unwrap().push(Value::Int(fields.get(18).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("rmw").unwrap().push(Value::Int(fields.get(19).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("gusts").unwrap().push(Value::Int(fields.get(20).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("eye").unwrap().push(Value::Int(fields.get(21).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("subregion").unwrap().push(Value::Str(fields.get(22).unwrap_or(&"").to_string()));
//         // result.get_mut("maxseas").unwrap().push(Value::Int(fields.get(23).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("initials").unwrap().push(Value::Str(fields.get(24).unwrap_or(&"").to_string()));
//         // result.get_mut("direction").unwrap().push(Value::Int(fields.get(25).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("speed").unwrap().push(Value::Int(fields.get(26).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("stormname").unwrap().push(Value::Str(fields.get(27).unwrap_or(&"").to_string()));
//         // result.get_mut("depth").unwrap().push(Value::Str(fields.get(28).unwrap_or(&"").to_string()));
//         // result.get_mut("seas").unwrap().push(Value::Int(fields.get(29).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("seascode").unwrap().push(Value::Str(fields.get(30).unwrap_or(&"").to_string()));
//         // result.get_mut("seas1").unwrap().push(Value::Int(fields.get(31).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("seas2").unwrap().push(Value::Int(fields.get(32).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("seas3").unwrap().push(Value::Int(fields.get(33).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("seas4").unwrap().push(Value::Int(fields.get(34).unwrap_or(&"0").parse().unwrap_or(0)));
//         // result.get_mut("userdefined1").unwrap().push(Value::Str(fields.get(35).unwrap_or(&"").to_string()));
//         // result.get_mut("userdata1").unwrap().push(Value::Str(fields.get(36).unwrap_or(&"").to_string()));
//         // result.get_mut("userdefined2").unwrap().push(Value::Str(fields.get(37).unwrap_or(&"").to_string()));
//         // result.get_mut("userdata2").unwrap().push(Value::Str(fields.get(38).unwrap_or(&"").to_string()));
//         // result.get_mut("userdefined3").unwrap().push(Value::Str(fields.get(39).unwrap_or(&"").to_string()));
//         // result.get_mut("userdata3").unwrap().push(Value::Str(fields.get(40).unwrap_or(&"").to_string()));
//         // result.get_mut("userdefined4").unwrap().push(Value::Str(fields.get(41).unwrap_or(&"").to_string()));
//         // result.get_mut("userdata4").unwrap().push(Value::Str(fields.get(42).unwrap_or(&"").to_string()));
//         // result.get_mut("userdefined5").unwrap().push(Value::Str(fields.get(43).unwrap_or(&"").to_string()));
//         // result.get_mut("userdata5").unwrap().push(Value::Str(fields.get(44).unwrap_or(&"").to_string()));
//     }

//     let result = result.lock().unwrap();
//     let py_dict = result.into_py_dict(py);
//     Ok(py_dict.into())
// }

fn parse_coordinate(coord: &str, pos: char, neg: char) -> Option<f32> {
    if coord.is_empty() {
        return None;
    }
    let last_char = coord.chars().last().unwrap();
    let value: f32 = coord[..coord.len() - 1].parse().ok()?;
    match last_char {
        c if c == pos => Some(value),
        c if c == neg => Some(-value),
        _ => None,
    }
}



#[pyfunction]
fn create_pyarrow_table(py: Python) -> PyResult<PyObject> {
    // Define schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("column1", DataType::Utf8, false),
        Field::new("column2", DataType::Float64, false),
        Field::new("column3", DataType::Float64, false),
        Field::new(
            "column4",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));

    // Create columns
    let column1 = Arc::new(StringArray::from(vec!["row1", "row2"])) as ArrayRef;
    let column2 = Arc::new(Float64Array::from(vec![1.1, 2.2])) as ArrayRef;
    let column3 = Arc::new(Float64Array::from(vec![3.3, 4.4])) as ArrayRef;
    let column4 = Arc::new(TimestampNanosecondArray::from(vec![1672531200000000000, 1672617600000000000])) as ArrayRef;

    // Create RecordBatch
    let batch = RecordBatch::try_new(schema.clone(), vec![column1, column2, column3, column4])
        .expect("Failed to create RecordBatch");

    // Convert RecordBatch to PyArrow Table
    let pyarrow_table = batch.to_pyarrow(py).expect("Failed to convert to PyArrow Table");

    Ok(pyarrow_table)
}

#[pyclass]
pub struct ArrowDataFrame {
    schema: Arc<Schema>,
    column1: StringBuilder,
    column2: Float64Builder,
    column3: Float64Builder,
    column4: TimestampNanosecondBuilder,
}

#[pymethods]
impl ArrowDataFrame {
    #[new]
    pub fn new() -> Self {
        // Define the schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("column1", DataType::Utf8, false),
            Field::new("column2", DataType::Float64, false),
            Field::new("column3", DataType::Float64, false),
            Field::new("column4", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        ]));

        // Initialize column builders
        Self {
            schema,
            column1: StringBuilder::new(),
            column2: Float64Builder::new(),
            column3: Float64Builder::new(),
            column4: TimestampNanosecondBuilder::new(),
        }
    }

    /// Append a single row to the ArrowDataFrame
    pub fn append_row(&mut self, val1: &str, val2: f64, val3: f64, val4: i64) -> PyResult<()> {
        self.column1.append_value(val1);
        self.column2.append_value(val2);
        self.column3.append_value(val3);
        self.column4.append_value(val4);
        Ok(())
    }

    /// Append values from 1 to 1000 to each column
    pub fn append_1000_rows(&mut self) -> PyResult<()> {
        for i in 1..=1000 {
            let val1 = format!("row{}", i);
            let val2 = i as f64;
            let val3 = (i * 2) as f64;
            let val4 = 1672531200000000000 + (i as i64) * 1_000_000; // Example timestamp
            self.append_row(&val1, val2, val3, val4)?;
        }
        Ok(())
    }

    /// Convert to PyArrow Table
    pub fn to_pyarrow(&mut self, py: Python) -> PyResult<PyObject> {
        // Finalize the Arrow arrays
        let array1: ArrayRef = Arc::new(self.column1.finish());
        let array2: ArrayRef = Arc::new(self.column2.finish());
        let array3: ArrayRef = Arc::new(self.column3.finish());
        let array4: ArrayRef = Arc::new(self.column4.finish());

        // Create a RecordBatch
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![array1, array2, array3, array4],
        )
        .expect("Failed to create RecordBatch");

        // Convert RecordBatch to PyArrow Table
        Ok(batch.to_pyarrow(py).expect("Failed to convert to PyArrow Table"))
    }
}



#[pymodule]
fn adeck_parser(_py: Python, m: &PyModule) -> PyResult<()> {
    // m.add_function(wrap_pyfunction!(parse_adeck, m)?)?;
    m.add_function(wrap_pyfunction!(create_pyarrow_table, m)?)?;
    m.add_class::<ArrowDataFrame>()?;
    Ok(())
}

