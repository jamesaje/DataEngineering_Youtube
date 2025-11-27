import pandas as pd

from etls.youtube_etl import (
    fetch_youtube_search_results, 
    fetch_youtube_video_stats,
    build_videos_with_stats,
    transform_youtube_df,
)
from utils.constants import YOUTUBE_API_KEY, OUTPUT_PATH


def youtube_pipeline(
    file_name: str,
    query: str,
    max_results: int = 25,
    **context,
) -> str:
    """
    Extract YouTube videos for a search query, enrich with statistics,
    transform, save as CSV, and return the file path.
    """

    print(f"🎥 Starting YouTube pipeline for query='{query}', max_results={max_results}")

    # 1) Search for videos
    items = fetch_youtube_search_results(YOUTUBE_API_KEY, query=query, max_results=max_results)
    print(f"Fetched {len(items)} search results from YouTube.")

    # 2) Collect video IDs
    video_ids = [
        item.get("id", {}).get("videoId")
        for item in items
        if item.get("id", {}).get("videoId")
    ]
    print(f"Extracted {len(video_ids)} video IDs.")

    # 3) Fetch statistics for those IDs
    stats_by_id = fetch_youtube_video_stats(YOUTUBE_API_KEY, video_ids)
    print(f"Fetched statistics for {len(stats_by_id)} videos.")

    # 4) Merge snippet + statistics into rows
    rows = build_videos_with_stats(items, stats_by_id)

    # 5) Build DataFrame and transform
    df = pd.DataFrame(rows)
    df = transform_youtube_df(df)

    # 6) Save to CSV
    file_path = f"{OUTPUT_PATH}/{file_name}.csv"
    df.to_csv(file_path, index=False)
    print(f"✅ Saved enriched YouTube data to {file_path}")

    # 7) Return path for downstream tasks (e.g., S3 upload)
    return file_path