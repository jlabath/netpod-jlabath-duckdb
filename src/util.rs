use crate::Request;
use anyhow::{anyhow, Result};
use bendy::decoding::FromBencode;
use duckdb::{appender_params_from_iter, types::Value as DuckValue, AppenderParams};
use serde_json::{json, Value as JsonValue};

pub fn duck_to_json(value: DuckValue) -> JsonValue {
    match value {
        DuckValue::Boolean(b) => JsonValue::Bool(b),
        DuckValue::TinyInt(i) => json!(i),
        DuckValue::SmallInt(i) => json!(i),
        DuckValue::Int(i) => json!(i),
        DuckValue::BigInt(i) => json!(i),
        DuckValue::HugeInt(i) => json!(i),
        DuckValue::UTinyInt(i) => json!(i),
        DuckValue::USmallInt(i) => json!(i),
        DuckValue::UInt(i) => json!(i),
        DuckValue::UBigInt(i) => json!(i),
        DuckValue::Float(f) => json!(f),
        DuckValue::Double(d) => json!(d),
        DuckValue::Text(s) => JsonValue::String(s),
        DuckValue::Null => JsonValue::Null,
        // Handle any other variants as necessary
        v => {
            eprintln!("ERROR Unhandled duck value in duck_to_json: {:?}", v);
            JsonValue::Null
        } // Or another appropriate default for unsupported types
    }
}

pub fn decode_request(buffer: &[u8]) -> Result<Request> {
    // Check if the last byte is `e` (ASCII value for 'e') which marks dictionary termination
    if buffer[buffer.len() - 1] == b'e' {
        Request::from_bencode(buffer).map_err(|e| anyhow!("{}", e))
    } else {
        Err(anyhow!("keep reading"))
    }
}

pub fn json_to_duck(value: JsonValue) -> DuckValue {
    match value {
        JsonValue::Bool(b) => DuckValue::Boolean(b),
        JsonValue::Null => DuckValue::Null,
        JsonValue::Number(num) => {
            if let Some(f) = num.as_f64() {
                DuckValue::Double(f)
            } else if let Some(i) = num.as_i64() {
                DuckValue::BigInt(i)
            } else if let Some(ui) = num.as_u64() {
                DuckValue::UBigInt(ui)
            } else if let Some(hi) = num.as_i128() {
                DuckValue::HugeInt(hi)
            } else {
                eprintln!("failed to convert this number in json_to_duck: {:?}", num);
                DuckValue::Null
            }
        }
        JsonValue::String(s) => DuckValue::Text(s),
        // Handle any other variants as necessary
        v => {
            eprintln!("ERROR Unhandled json value in json_to_duck: {:?}", v);
            DuckValue::Null
        } // Or another appropriate default for unsupported types
    }
}

pub fn json_row_to_params(values: Vec<JsonValue>) -> impl AppenderParams {
    let duck_row: Vec<DuckValue> = values.into_iter().map(json_to_duck).collect();
    appender_params_from_iter(duck_row.into_iter())
}
