import requests
from datetime import datetime
import pandas as pd
import isodate

YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

def fetch_youtube_search_results(api_key: str, query: str, max_results: int = 25):
    """
    Call the search endpoint to get basic video/snippet info.
    Returns the raw 'items' list from the API.
    """
    params = {
        "key": api_key,
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": max_results,
    }

    resp = requests.get(YOUTUBE_SEARCH_URL, params=params)
    resp.raise_for_status()
    data = resp.json()

    return data.get("items", [])


def fetch_youtube_video_stats(api_key: str, video_ids: list[str]):
    """
    Call the videos endpoint to get statistics (views, likes, comments, etc.)
    for a list of video IDs. Returns a dict: {video_id: stats_dict}.
    """
    if not video_ids:
        return {}

    stats_by_id: dict[str, dict] = {}

    # YouTube allows up to 50 IDs per request
    chunk_size = 50
    for i in range(0, len(video_ids), chunk_size):
        chunk = video_ids[i : i + chunk_size]
        params = {
            "key": api_key,
            "part": "statistics,contentDetails",
            "id": ",".join(chunk),
        }

        resp = requests.get(YOUTUBE_VIDEOS_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("items", []):
            vid = item.get("id")
            stats = item.get("statistics", {})
            stats_by_id[vid] = item

    return stats_by_id


def build_videos_with_stats(items: list[dict], stats_by_id: dict[str, dict]):
    """
    Merge snippet info from search with stats + contentDetails (duration)
    from videos endpoint into a flat list of dicts ready for a DataFrame.
    """
    rows = []

    for item in items:
        vid = item.get("id", {}).get("videoId")
        snippet = item.get("snippet", {}) or {}

        # stats_by_id[vid] contains the full item from the videos endpoint
        stats_entry = stats_by_id.get(vid) or {}
        statistics = stats_entry.get("statistics", {}) or {}
        content_details = stats_entry.get("contentDetails", {}) or {}

        rows.append(
            {
                "video_id": vid,
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "published_at": snippet.get("publishedAt"),
                "channel_title": snippet.get("channelTitle"),
                "channel_id": snippet.get("channelId"),

                # statistics (strings from API)
                "view_count": statistics.get("viewCount"),
                "like_count": statistics.get("likeCount"),
                "favorite_count": statistics.get("favoriteCount"),
                "comment_count": statistics.get("commentCount"),

                # new field from contentDetails
                "duration": content_details.get("duration"),   # e.g. "PT15M23S"
            }
        )

    return rows


def transform_youtube_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic transformations: timestamps, text columns, numeric stats.
    """

    # parse timestamp
    if "published_at" in df.columns:
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

    # ensure text columns are strings
    text_cols = ["title", "description", "channel_title", "channel_id", "video_id"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # convert stats columns to numeric (nullable integers)
    stat_cols = ["view_count", "like_count", "favorite_count", "comment_count"]
    for col in stat_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # parse ISO 8601 duration into seconds
    if "duration" in df.columns:
        df["duration_seconds"] = (
            df["duration"]
            .apply(lambda x: isodate.parse_duration(x).total_seconds() if pd.notnull(x) else None)
            .astype("Int64")
        )

    return df