use serde::{Deserialize, Serialize};

pub const PARSER_VERSION: u32 = 1;
const CMD_DANMU_MSG: &str = "DANMU_MSG";
const CMD_SEND_GIFT: &str = "SEND_GIFT";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DanmakuEvent {
    pub uid: u64,
    pub uname: String,
    pub message: String,
    pub timestamp: u64,
    pub command_type: String,
    pub parser_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GiftEvent {
    pub uid: u64,
    pub uname: String,
    pub gift_id: u64,
    pub gift_name: String,
    pub coin_type: String,
    pub total_coin: u64,
    pub num: u32,
    pub timestamp: u64,
    pub command_type: String,
    pub parser_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiveEvent {
    Danmaku(DanmakuEvent),
    Gift(GiftEvent),
    Unsupported { command: String },
    Malformed { command: String },
}

pub fn parse_event(message: &serde_json::Value) -> LiveEvent {
    let cmd = message.get("cmd").and_then(|v| v.as_str()).unwrap_or("");
    match cmd {
        CMD_DANMU_MSG => {
            parse_danmaku(message)
                .map(LiveEvent::Danmaku)
                .unwrap_or(LiveEvent::Malformed {
                    command: cmd.to_string(),
                })
        }
        CMD_SEND_GIFT => parse_gift(message)
            .map(LiveEvent::Gift)
            .unwrap_or(LiveEvent::Malformed {
                command: cmd.to_string(),
            }),
        _ => LiveEvent::Unsupported {
            command: cmd.to_string(),
        },
    }
}

pub fn parse_danmaku(message: &serde_json::Value) -> Option<DanmakuEvent> {
    let info = message.get("info")?.as_array()?;
    let message_text = info.get(1)?.as_str()?.to_string();
    let user_info = info.get(2)?.as_array()?;
    let uid = user_info.first()?.as_u64()?;
    let uname = user_info.get(1)?.as_str()?.to_string();
    let timestamp = info
        .first()
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.get(4))
        .and_then(|v| v.as_u64())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });

    Some(DanmakuEvent {
        uid,
        uname,
        message: message_text,
        timestamp,
        command_type: CMD_DANMU_MSG.to_string(),
        parser_version: PARSER_VERSION,
    })
}

pub fn parse_gift(message: &serde_json::Value) -> Option<GiftEvent> {
    #[derive(Deserialize)]
    #[allow(non_snake_case)]
    struct GiftData {
        #[serde(default)]
        uid: Option<u64>,
        #[serde(default)]
        uname: Option<String>,
        #[serde(default)]
        giftId: Option<u64>,
        #[serde(default)]
        giftName: Option<String>,
        #[serde(default)]
        coin_type: Option<String>,
        #[serde(default)]
        price: Option<u64>,
        #[serde(default)]
        num: Option<u32>,
        #[serde(default)]
        timestamp: Option<u64>,
    }

    #[derive(Deserialize)]
    struct GiftMessage {
        cmd: String,
        data: Option<GiftData>,
    }

    let msg: GiftMessage = serde_json::from_value(message.clone()).ok()?;
    if msg.cmd != CMD_SEND_GIFT {
        return None;
    }

    let d = msg.data?;
    let gift_name = d.giftName.filter(|s| !s.is_empty())?;
    let coin_type = normalize_coin_type(d.coin_type.as_deref()?)?;
    let uname = d.uname.filter(|s| !s.is_empty())?;

    Some(GiftEvent {
        uid: d.uid?,
        uname,
        gift_id: d.giftId?,
        gift_name,
        coin_type: coin_type.to_string(),
        total_coin: d.price?,
        num: d.num?,
        timestamp: d.timestamp?,
        command_type: CMD_SEND_GIFT.to_string(),
        parser_version: PARSER_VERSION,
    })
}

fn normalize_coin_type(coin_type: &str) -> Option<&'static str> {
    match coin_type {
        "gold" => Some("gold"),
        "silver" => Some("silver"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_danmaku_fixture() {
        let msg = serde_json::json!({
            "cmd": "DANMU_MSG",
            "info": [
                [0, 1, 25, 16777215, 1700000000],
                "hello world",
                [12345, "test_user", 0]
            ]
        });
        let event = parse_danmaku(&msg).unwrap();
        assert_eq!(event.uid, 12345);
        assert_eq!(event.uname, "test_user");
        assert_eq!(event.message, "hello world");
        assert_eq!(event.timestamp, 1700000000);
        assert_eq!(event.command_type, "DANMU_MSG");
        assert_eq!(event.parser_version, PARSER_VERSION);
    }

    #[test]
    fn parse_gift_fixture() {
        let msg = serde_json::json!({
            "cmd": "SEND_GIFT",
            "data": {
                "uid": 456,
                "uname": "gift_user",
                "giftId": 123,
                "giftName": "test_gift",
                "coin_type": "gold",
                "price": 100,
                "num": 2,
                "timestamp": 1700000000
            }
        });
        let event = parse_gift(&msg).unwrap();
        assert_eq!(event.uid, 456);
        assert_eq!(event.uname, "gift_user");
        assert_eq!(event.gift_id, 123);
        assert_eq!(event.gift_name, "test_gift");
        assert_eq!(event.coin_type, "gold");
        assert_eq!(event.total_coin, 100);
        assert_eq!(event.num, 2);
        assert_eq!(event.command_type, "SEND_GIFT");
        assert_eq!(event.parser_version, PARSER_VERSION);
    }

    #[test]
    fn parse_gift_wrong_cmd() {
        let msg = serde_json::json!({"cmd": "OTHER", "data": {}});
        assert!(parse_gift(&msg).is_none());
    }

    #[test]
    fn parse_gift_no_data() {
        let msg = serde_json::json!({"cmd": "SEND_GIFT"});
        assert!(parse_gift(&msg).is_none());
    }

    #[test]
    fn parse_gift_rejects_partial_data() {
        let msg = serde_json::json!({
            "cmd": "SEND_GIFT",
            "data": {
                "giftId": 123,
                "giftName": "test"
            }
        });
        assert!(parse_gift(&msg).is_none());
    }

    #[test]
    fn parse_gift_rejects_unknown_coin_type() {
        let msg = serde_json::json!({
            "cmd": "SEND_GIFT",
            "data": {
                "uid": 456,
                "uname": "gift_user",
                "giftId": 123,
                "giftName": "test_gift",
                "coin_type": "points",
                "price": 100,
                "num": 2,
                "timestamp": 1700000000
            }
        });
        assert!(parse_gift(&msg).is_none());
    }

    #[test]
    fn malformed_json_does_not_panic() {
        let msg = serde_json::json!("not an object");
        let _ = parse_event(&msg);

        let msg = serde_json::json!(null);
        let _ = parse_event(&msg);

        let msg = serde_json::json!(42);
        let _ = parse_event(&msg);
    }

    #[test]
    fn unsupported_command_classified() {
        let msg = serde_json::json!({"cmd": "UNKNOWN_CMD"});
        match parse_event(&msg) {
            LiveEvent::Unsupported { command } => assert_eq!(command, "UNKNOWN_CMD"),
            _ => panic!("expected Unsupported"),
        }
    }

    #[test]
    fn missing_cmd_classified() {
        let msg = serde_json::json!({"data": {}});
        match parse_event(&msg) {
            LiveEvent::Unsupported { command } => assert_eq!(command, ""),
            _ => panic!("expected Unsupported"),
        }
    }

    #[test]
    fn known_cmd_bad_data_returns_malformed() {
        let msg = serde_json::json!({"cmd": "DANMU_MSG", "info": "not_an_array"});
        match parse_event(&msg) {
            LiveEvent::Malformed { command } => assert_eq!(command, "DANMU_MSG"),
            other => panic!("expected Malformed, got {other:?}"),
        }

        let msg = serde_json::json!({"cmd": "SEND_GIFT", "data": {"uid": 1}});
        match parse_event(&msg) {
            LiveEvent::Malformed { command } => assert_eq!(command, "SEND_GIFT"),
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn unknown_cmd_returns_unsupported() {
        let msg = serde_json::json!({"cmd": "INTERACT_WORD"});
        match parse_event(&msg) {
            LiveEvent::Unsupported { command } => assert_eq!(command, "INTERACT_WORD"),
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
}
