use super::wbi;
use super::{LiveApi, LiveApiError, LiveAuth, LiveEndpoint};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct LiveApiClient {
    http: reqwest::Client,
    api_semaphore: Arc<Semaphore>,
}

impl Default for LiveApiClient {
    fn default() -> Self {
        Self::new(10)
    }
}

impl LiveApiClient {
    pub fn new(api_concurrency_limit: usize) -> Self {
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(HTTP_TIMEOUT)
            .build()
            .expect("client build");
        let api_semaphore = Arc::new(Semaphore::new(api_concurrency_limit));
        Self {
            http,
            api_semaphore,
        }
    }

    async fn fetch_buvid3(&self) -> Result<String, LiveApiError> {
        #[derive(serde::Deserialize)]
        struct SpiData {
            b_3: String,
        }

        let resp: BiliResponse<SpiData> = self
            .http
            .get("https://api.bilibili.com/x/frontend/finger/spi")
            .send()
            .await
            .map_err(|e| LiveApiError::Network(e.to_string()))?
            .json()
            .await
            .map_err(|e| LiveApiError::Parse(e.to_string()))?;

        Ok(resp.data.map(|d| d.b_3).unwrap_or_default())
    }
}

#[derive(serde::Deserialize)]
struct BiliResponse<T> {
    code: i64,
    #[serde(default)]
    message: Option<String>,
    data: Option<T>,
}

#[derive(serde::Deserialize)]
struct RoomInitData {
    room_id: u64,
}

#[derive(serde::Deserialize)]
struct NavData {
    wbi_img: WbiImg,
    #[serde(default)]
    mid: Option<u64>,
}

#[derive(serde::Deserialize)]
struct WbiImg {
    img_url: String,
    sub_url: String,
}

#[derive(serde::Deserialize)]
struct DanmuInfoData {
    token: String,
    host_list: Vec<HostEntry>,
}

#[derive(serde::Deserialize)]
struct HostEntry {
    host: String,
    #[serde(default)]
    wss_port: Option<u16>,
}

impl LiveApi for LiveApiClient {
    async fn resolve_room_id(&self, room_id: u64) -> Result<u64, LiveApiError> {
        let url = format!("https://api.live.bilibili.com/room/v1/Room/mobileRoomInit?id={room_id}");
        let resp: BiliResponse<RoomInitData> = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| LiveApiError::Network(e.to_string()))?
            .json()
            .await
            .map_err(|e| LiveApiError::Parse(e.to_string()))?;

        if resp.code != 0 {
            return Err(LiveApiError::Api {
                code: resp.code,
                message: resp.message.unwrap_or_default(),
            });
        }

        Ok(resp.data.map(|d| d.room_id).unwrap_or(room_id))
    }

    async fn fetch_live_auth(&self, room_id: u64) -> Result<LiveAuth, LiveApiError> {
        let _permit = self
            .api_semaphore
            .acquire()
            .await
            .map_err(|_| LiveApiError::Network("api limiter closed".into()))?;
        let long_room_id = self.resolve_room_id(room_id).await?;

        let nav: BiliResponse<NavData> = self
            .http
            .get("https://api.bilibili.com/x/web-interface/nav")
            .send()
            .await
            .map_err(|e| LiveApiError::Network(e.to_string()))?
            .json()
            .await
            .map_err(|e| LiveApiError::Parse(e.to_string()))?;

        let nav_data = nav
            .data
            .ok_or(LiveApiError::Auth("nav data missing".into()))?;
        let uid = nav_data.mid.filter(|uid| *uid > 0);

        let img_key = nav_data
            .wbi_img
            .img_url
            .rsplit('/')
            .next()
            .and_then(|s| s.split('.').next())
            .unwrap_or("");
        let sub_key = nav_data
            .wbi_img
            .sub_url
            .rsplit('/')
            .next()
            .and_then(|s| s.split('.').next())
            .unwrap_or("");
        let mixin_key = wbi::get_mixin_key(&format!("{img_key}{sub_key}"));

        let signed = wbi::sign_wbi(
            &serde_json::json!({
                "id": long_room_id,
                "type": 0,
                "web_location": "444.8"
            }),
            &mixin_key,
        );
        let url =
            format!("https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo?{signed}");
        let danmu: BiliResponse<DanmuInfoData> = self
            .http
            .get(&url)
            .header("Referer", "https://live.bilibili.com/")
            .send()
            .await
            .map_err(|e| LiveApiError::Network(e.to_string()))?
            .json()
            .await
            .map_err(|e| LiveApiError::Parse(e.to_string()))?;

        if danmu.code != 0 {
            return Err(LiveApiError::Api {
                code: danmu.code,
                message: danmu.message.unwrap_or_default(),
            });
        }

        let danmu_data = danmu
            .data
            .ok_or(LiveApiError::Auth("danmuInfo data missing".into()))?;

        let endpoints: Vec<LiveEndpoint> = danmu_data
            .host_list
            .iter()
            .map(|h| LiveEndpoint {
                host: h.host.clone(),
                port: h.wss_port.unwrap_or(443),
            })
            .collect();

        let buvid3 = self.fetch_buvid3().await?;

        Ok(LiveAuth {
            token: danmu_data.token,
            endpoints,
            room_id: long_room_id,
            uid,
            buvid3,
        })
    }
}
