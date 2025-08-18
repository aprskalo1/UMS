from googleapiclient.discovery import build
from tenacity import retry, wait_exponential_jitter, stop_after_attempt
from typing import List, Dict, Any, Optional
from config import YT_API_KEY


class YouTubeClient:
    def __init__(self):
        if not YT_API_KEY:
            raise RuntimeError("YT_API_KEY not set")
        self.yt = build("youtube", "v3", developerKey=YT_API_KEY, cache_discovery=False)

    @retry(wait=wait_exponential_jitter(1, 8), stop=stop_after_attempt(5))
    def search_playlists(self, q: str, region_code: Optional[str], max_results: int) -> List[Dict[str, Any]]:
        req = self.yt.search().list(
            part="snippet",
            q=q,
            type="playlist",
            regionCode=region_code,
            maxResults=max_results,
            safeSearch="none",
        )
        return req.execute().get("items", [])

    @retry(wait=wait_exponential_jitter(1, 8), stop=stop_after_attempt(5))
    def playlist_items(self, playlist_id: str) -> List[Dict[str, Any]]:
        out = []
        page_token = None
        while True:
            req = self.yt.playlistItems().list(
                part="contentDetails,snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=page_token,
            )
            res = req.execute()
            out.extend(res.get("items", []))
            page_token = res.get("nextPageToken")
            if not page_token:
                break
        return out

    @retry(wait=wait_exponential_jitter(1, 8), stop=stop_after_attempt(5))
    def videos_metadata(self, video_ids: List[str]) -> List[Dict[str, Any]]:
        out = []
        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i:i + 50]
            req = self.yt.videos().list(
                part="snippet,contentDetails,statistics,status",
                id=",".join(chunk),
                maxResults=50,
            )
            res = req.execute()
            out.extend(res.get("items", []))
        return out
