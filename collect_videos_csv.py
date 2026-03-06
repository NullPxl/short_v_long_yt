"""
Collect longitudinal YouTube video stats (views, likes, comments) into CSV.

Uses YouTube API. Supports multiple channels. Only collects videos (not shorts)
posted after a specified datetime. Polls every 30 seconds.
"""

import argparse
import csv
import datetime as dt
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence
from keys import API_KEY as GOOGLE_API_KEY

from googleapiclient.discovery import build

CHANNEL_ID = "UCWJ2lWNubArHWmf3FIHbfcQ"
SINCE_DATETIME = "2026-03-06T00:00:00Z"

ISO8601_DURATION_RE = re.compile(
    r"^PT"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+)S)?$"
)

# Videos with duration <= 60 seconds are shorts; we exclude them
SHORTS_MAX_DURATION_SECONDS = 60

CSV_COLUMNS = [
    "captured_at",
    "video_id",
    "channel_id",
    "title",
    "published_at",
    "view_count",
    "like_count",
    "comment_count",
]


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def parse_iso8601_duration_seconds(duration: str) -> int:
    match = ISO8601_DURATION_RE.match(duration)
    if not match:
        raise ValueError(f"Unsupported ISO-8601 duration: {duration}")

    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    return hours * 3600 + minutes * 60 + seconds


def parse_datetime(s: str) -> dt.datetime:
    """Parse ISO8601 datetime string to timezone-aware datetime."""
    parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


@dataclass
class VideoRecord:
    video_id: str
    channel_id: str
    title: str
    published_at: str
    duration_seconds: int
    view_count: int
    like_count: Optional[int]
    comment_count: Optional[int]

    @property
    def is_short(self) -> bool:
        return self.duration_seconds <= SHORTS_MAX_DURATION_SECONDS


class CsvStore:
    def __init__(self, csv_path: str) -> None:
        self.csv_path = csv_path
        self._ensure_file()

    def _ensure_file(self) -> None:
        if os.path.exists(self.csv_path):
            return
        os.makedirs(os.path.dirname(self.csv_path) or ".", exist_ok=True)
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()

    def append_rows(self, rows: Sequence[Dict[str, str]]) -> int:
        count = 0
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            for row in rows:
                writer.writerow(row)
                count += 1
        return count


class YouTubeVideoCollector:
    def __init__(self, api_key: str, output_csv: str) -> None:
        self.api_key = api_key
        self.youtube = build("youtube", "v3", developerKey=api_key)
        self.store = CsvStore(output_csv)
        self._uploads_playlist_cache: Dict[str, str] = {}

    def get_uploads_playlist_id(self, channel_id: str) -> str:
        if channel_id in self._uploads_playlist_cache:
            return self._uploads_playlist_cache[channel_id]

        resp = (
            self.youtube.channels()
            .list(part="contentDetails", id=channel_id, maxResults=1)
            .execute()
        )
        items = resp.get("items", [])
        if not items:
            raise RuntimeError(f"Channel not found: {channel_id}")

        playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
        self._uploads_playlist_cache[channel_id] = playlist_id
        return playlist_id

    def discover_videos_since(
        self,
        channel_id: str,
        since: dt.datetime,
        max_pages: int = 10,
    ) -> List[VideoRecord]:
        """Discover videos posted after `since` for the given channel. Excludes shorts."""
        playlist_id = self.get_uploads_playlist_id(channel_id)
        video_ids: List[str] = []
        next_page_token: Optional[str] = None

        seen_old = False
        for _ in range(max_pages):
            req = self.youtube.playlistItems().list(
                part="contentDetails,snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            )
            resp = req.execute()
            items = resp.get("items", [])

            for item in items:
                details = item.get("contentDetails", {})
                snippet = item.get("snippet", {})
                video_id = details.get("videoId")
                published_str = snippet.get("publishedAt", "")

                if not video_id or not published_str:
                    continue

                try:
                    published = parse_datetime(published_str)
                except ValueError:
                    continue

                if published < since:
                    # Playlist is newest-first; stop paginating
                    seen_old = True
                    break

                video_ids.append(video_id)

            if seen_old:
                break
            next_page_token = resp.get("nextPageToken")
            if not next_page_token:
                break

        if not video_ids:
            return []

        return self.fetch_video_records(video_ids, channel_id)

    def fetch_video_records(
        self,
        video_ids: Sequence[str],
        channel_id: str,
    ) -> List[VideoRecord]:
        records: List[VideoRecord] = []
        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i : i + 50]
            resp = (
                self.youtube.videos()
                .list(
                    part="snippet,contentDetails,statistics",
                    id=",".join(chunk),
                    maxResults=50,
                )
                .execute()
            )

            for item in resp.get("items", []):
                stats = item.get("statistics", {})
                snippet = item.get("snippet", {})
                content_details = item.get("contentDetails", {})

                duration_str = content_details.get("duration", "PT0S")
                try:
                    duration_s = parse_iso8601_duration_seconds(duration_str)
                except ValueError:
                    duration_s = 0

                rec = VideoRecord(
                    video_id=item["id"],
                    channel_id=channel_id,
                    title=snippet.get("title", ""),
                    published_at=snippet.get("publishedAt", ""),
                    duration_seconds=duration_s,
                    view_count=int(stats.get("viewCount", 0)),
                    like_count=int(stats["likeCount"]) if "likeCount" in stats else None,
                    comment_count=int(stats["commentCount"]) if "commentCount" in stats else None,
                )
                # Exclude shorts
                if not rec.is_short:
                    records.append(rec)

        return records

    def collect_once(
        self,
        channel_ids: List[str],
        since: dt.datetime,
    ) -> Dict[str, int]:
        all_records: List[VideoRecord] = []
        for channel_id in channel_ids:
            records = self.discover_videos_since(channel_id, since)
            all_records.extend(records)

        # Dedupe by video_id (in case a video appears in multiple channels)
        seen: Dict[str, VideoRecord] = {}
        for rec in all_records:
            if rec.video_id not in seen:
                seen[rec.video_id] = rec

        captured_at = utc_now_iso()
        rows = [
            {
                "captured_at": captured_at,
                "video_id": r.video_id,
                "channel_id": r.channel_id,
                "title": r.title,
                "published_at": r.published_at,
                "view_count": str(r.view_count),
                "like_count": "" if r.like_count is None else str(r.like_count),
                "comment_count": "" if r.comment_count is None else str(r.comment_count),
            }
            for r in seen.values()
        ]
        count = self.store.append_rows(rows)
        return {"videos": len(seen), "rows_written": count}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect YouTube video stats (views/likes/comments) into CSV. "
        "Checks for new videos after a specified datetime. Polls every 30 seconds."
    )
    parser.add_argument(
        "--api-key",
        default=GOOGLE_API_KEY,
        help="YouTube API key (or set YOUTUBE_API_KEY)",
    )
    parser.add_argument(
        "--channels",
        nargs="+",
        default=[CHANNEL_ID],
        help="Channel IDs to monitor (space-separated)",
    )
    parser.add_argument(
        "--since",
        default=SINCE_DATETIME,
        help="Only collect videos published after this datetime (ISO8601, e.g. 2025-03-01T00:00:00Z)",
    )
    parser.add_argument(
        "--output",
        default="video_stats.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=30,
        help="Seconds between collection runs",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (no continuous polling)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.api_key:
        raise SystemExit("Missing API key. Provide --api-key or set YOUTUBE_API_KEY.")

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    try:
        since = parse_datetime(args.since)
    except ValueError as e:
        raise SystemExit(f"Invalid --since datetime: {e}") from e

    collector = YouTubeVideoCollector(api_key=args.api_key, output_csv=args.output)

    if args.once:
        stats = collector.collect_once(args.channels, since)
        logging.info("One-time collection complete: %s", stats)
        return

    logging.info(
        "Starting collector: channels=%s, since=%s, poll=%ss, output=%s",
        args.channels,
        args.since,
        args.poll_seconds,
        args.output,
    )
    while True:
        started = time.time()
        try:
            stats = collector.collect_once(args.channels, since)
            logging.info("Collection tick: %s", stats)
        except Exception:
            logging.exception("Collection tick failed")

        elapsed = time.time() - started
        sleep_s = max(1, args.poll_seconds - int(elapsed))
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
    time.sleep(100000)
