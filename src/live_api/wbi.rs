use md5::Digest;

const MIXIN_KEY_ENC_TAB: [usize; 64] = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49, 33, 9, 42, 19, 29,
    28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25,
    54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
];

pub fn get_mixin_key(raw: &str) -> String {
    let chars: Vec<char> = raw.chars().collect();
    MIXIN_KEY_ENC_TAB
        .iter()
        .filter_map(|&i| chars.get(i).copied())
        .take(32)
        .collect()
}

pub fn sign_wbi(params: &serde_json::Value, mixin_key: &str) -> String {
    let wts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let mut all = match params {
        serde_json::Value::Object(map) => map.clone(),
        _ => serde_json::Map::new(),
    };
    all.insert("wts".into(), serde_json::Value::Number(wts.into()));

    let mut keys: Vec<&String> = all.keys().collect();
    keys.sort();

    let query: String = keys
        .iter()
        .map(|k| {
            let v = stringify_wbi_param(&all[*k]);
            let v_cleaned: String = v
                .chars()
                .filter(|c| !matches!(c, '\'' | '!' | '(' | ')' | '*'))
                .collect();
            format!(
                "{}={}",
                urlencoding::encode(k),
                urlencoding::encode(&v_cleaned)
            )
        })
        .collect::<Vec<_>>()
        .join("&");

    let mut hasher = md5::Md5::new();
    hasher.update(query.as_bytes());
    hasher.update(mixin_key.as_bytes());
    let w_rid = hex::encode(hasher.finalize());

    format!("{query}&w_rid={w_rid}")
}

fn stringify_wbi_param(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixin_key_length() {
        let key = get_mixin_key(
            "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789",
        );
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn sign_wbi_produces_query() {
        let params = serde_json::json!({"id": 12345});
        let mixin_key = "0123456789abcdef0123456789abcdef";
        let result = sign_wbi(&params, mixin_key);
        assert!(result.contains("wts="));
        assert!(result.contains("w_rid="));
        assert!(result.contains("id=12345"));
    }

    #[test]
    fn sign_wbi_string_params_not_json_quoted() {
        let params = serde_json::json!({"id": 12345, "web_location": "444.8"});
        let mixin_key = "0123456789abcdef0123456789abcdef";
        let result = sign_wbi(&params, mixin_key);
        assert!(result.contains("web_location=444.8"));
        assert!(!result.contains("web_location=%22444.8%22"));
    }
}
