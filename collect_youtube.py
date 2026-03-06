import argparse
import csv
import datetime as dt
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

from googleapiclient.discovery import build

try:
    from keys import GOOGLE_API_KEY as LOCAL_GOOGLE_API_KEY
except ImportError:
    LOCAL_GOOGLE_API_KEY = None


ISO8601_DURATION_RE = re.compile(
    r"^PT"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+)S)?$"
)

VIDEO_COLUMNS = [
    "video_id",
    "channel_id",
    "title",
    "published_at",
    "duration_seconds",
    "is_short",
    "first_seen_at",
    "last_seen_at",
]

SNAPSHOT_COLUMNS = [
    "captured_at",
    "video_id",
    "channel_id",
    "title",
    "published_at",
    "duration_seconds",
    "is_short",
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


@dataclass
class VideoRecord:
    video_id: str
    title: str
    published_at: str
    duration_seconds: int
    is_short: bool
    view_count: int
    like_count: Optional[int]
    comment_count: Optional[int]


class CsvStore:
    def __init__(self, output_dir: str) -> None:
        self.output_dir = output_dir
        self.videos_csv_path = os.path.join(output_dir, "videos.csv")
        self.snapshots_csv_path = os.path.join(output_dir, "snapshots.csv")
        self._ensure_files()

    def _ensure_files(self) -> None:
        os.makedirs(self.output_dir, exist_ok=True)
        self._ensure_csv(self.videos_csv_path, VIDEO_COLUMNS)
        self._ensure_csv(self.snapshots_csv_path, SNAPSHOT_COLUMNS)

    @staticmethod
    def _ensure_csv(path: str, columns: Sequence[str]) -> None:
        if os.path.exists(path):
            return
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()

    def load_videos_index(self) -> Dict[str, Dict[str, str]]:
        index: Dict[str, Dict[str, str]] = {}
        with open(self.videos_csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                video_id = row.get("video_id")
                if video_id:
                    index[video_id] = row
        return index

    def tracked_video_ids(self, channel_id: str, include_shorts: bool) -> List[str]:
        video_ids: List[str] = []
        with open(self.videos_csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("channel_id") != channel_id:
                    continue
                if not include_shorts and row.get("is_short") == "1":
                    continue
                if row.get("video_id"):
                    video_ids.append(row["video_id"])
        return video_ids

    def write_videos_index(self, index: Dict[str, Dict[str, str]]) -> None:
        rows = sorted(index.values(), key=lambda r: (r["channel_id"], r["published_at"], r["video_id"]))
        with open(self.videos_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=VIDEO_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

    def append_snapshots(self, rows: Iterable[Dict[str, str]]) -> int:
        count = 0
        with open(self.snapshots_csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SNAPSHOT_COLUMNS)
            for row in rows:
                writer.writerow(row)
                count += 1
        return count


class YouTubeCollector:
    def __init__(
        self,
        api_key: str,
        channel_id: str,
        output_dir: str,
        include_shorts: bool = False,
        discover_pages: int = 2,
    ) -> None:
        self.api_key = api_key
        self.channel_id = channel_id
        self.include_shorts = include_shorts
        self.discover_pages = max(1, discover_pages)

        self.youtube = build("youtube", "v3", developerKey=self.api_key)
        self.store = CsvStore(output_dir)
        self._uploads_playlist_id: Optional[str] = None

    def get_uploads_playlist_id(self) -> str:
        if self._uploads_playlist_id:
            return self._uploads_playlist_id

        resp = (
            self.youtube.channels()
            .list(part="contentDetails", id=self.channel_id, maxResults=1)
            .execute()
        )
        items = resp.get("items", [])
        if not items:
            raise RuntimeError(f"Channel not found: {self.channel_id}")

        self._uploads_playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
        return self._uploads_playlist_id

    def discover_recent_upload_ids(self, uploads_playlist_id: str) -> List[str]:
        video_ids: List[str] = []
        next_page_token: Optional[str] = None

        for _ in range(self.discover_pages):
            req = self.youtube.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            )
            resp = req.execute()
            items = resp.get("items", [])
            video_ids.extend(
                item.get("contentDetails", {}).get("videoId")
                for item in items
                if item.get("contentDetails", {}).get("videoId")
            )

            next_page_token = resp.get("nextPageToken")
            if not next_page_token:
                break

        seen = set()
        deduped = []
        for vid in video_ids:
            if vid not in seen:
                seen.add(vid)
                deduped.append(vid)
        return deduped

    def fetch_video_records(self, video_ids: Sequence[str]) -> List[VideoRecord]:
        if not video_ids:
            return []

        records: List[VideoRecord] = []
        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i : i + 50]
            resp = (
                self.youtube.videos()
                .list(part="snippet,contentDetails,statistics", id=",".join(chunk), maxResults=50)
                .execute()
            )

            for item in resp.get("items", []):
                stats = item.get("statistics", {})
                snippet = item.get("snippet", {})
                content_details = item.get("contentDetails", {})

                duration_s = parse_iso8601_duration_seconds(content_details.get("duration", "PT0S"))
                records.append(
                    VideoRecord(
                        video_id=item["id"],
                        title=snippet.get("title", ""),
                        published_at=snippet.get("publishedAt", ""),
                        duration_seconds=duration_s,
                        is_short=duration_s <= 60,
                        view_count=int(stats.get("viewCount", 0)),
                        like_count=int(stats["likeCount"]) if "likeCount" in stats else None,
                        comment_count=int(stats["commentCount"]) if "commentCount" in stats else None,
                    )
                )

        return records

    def collect_once(self) -> Dict[str, int]:
        uploads_playlist_id = self.get_uploads_playlist_id()
        discovered_ids = self.discover_recent_upload_ids(uploads_playlist_id)
        tracked_ids = self.store.tracked_video_ids(self.channel_id, self.include_shorts)

        all_ids: List[str] = []
        seen = set()
        for vid in discovered_ids + tracked_ids:
            if vid not in seen:
                seen.add(vid)
                all_ids.append(vid)

        records = self.fetch_video_records(all_ids)
        captured_at = utc_now_iso()

        videos_index = self.store.load_videos_index()
        snapshot_rows: List[Dict[str, str]] = []

        for rec in records:
            if rec.is_short and not self.include_shorts:
                continue

            previous = videos_index.get(rec.video_id)
            first_seen_at = previous["first_seen_at"] if previous else captured_at
            videos_index[rec.video_id] = {
                "video_id": rec.video_id,
                "channel_id": self.channel_id,
                "title": rec.title,
                "published_at": rec.published_at,
                "duration_seconds": str(rec.duration_seconds),
                "is_short": "1" if rec.is_short else "0",
                "first_seen_at": first_seen_at,
                "last_seen_at": captured_at,
            }

            snapshot_rows.append(
                {
                    "captured_at": captured_at,
                    "video_id": rec.video_id,
                    "channel_id": self.channel_id,
                    "title": rec.title,
                    "published_at": rec.published_at,
                    "duration_seconds": str(rec.duration_seconds),
                    "is_short": "1" if rec.is_short else "0",
                    "view_count": str(rec.view_count),
                    "like_count": "" if rec.like_count is None else str(rec.like_count),
                    "comment_count": "" if rec.comment_count is None else str(rec.comment_count),
                }
            )

        self.store.write_videos_index(videos_index)
        snapshot_count = self.store.append_snapshots(snapshot_rows)

        return {
            "discovered_ids": len(discovered_ids),
            "tracked_ids": len(tracked_ids),
            "queried_ids": len(all_ids),
            "snapshots_written": snapshot_count,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect longitudinal YouTube stats (views/likes/comments) for channel uploads into CSV files."
    )
    parser.add_argument(
        "--api-key",
        default=LOCAL_GOOGLE_API_KEY or os.getenv("YOUTUBE_API_KEY"),
        required=False,
    )
    parser.add_argument("--channel-id", required=True)
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--poll-seconds", type=int, default=300)
    parser.add_argument("--discover-pages", type=int, default=2)
    parser.add_argument("--include-shorts", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.api_key:
        raise SystemExit(
            "Missing API key. Set GOOGLE_API_KEY in keys.py, provide --api-key, or set YOUTUBE_API_KEY."
        )

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    collector = YouTubeCollector(
        api_key=args.api_key,
        channel_id=args.channel_id,
        output_dir=args.output_dir,
        include_shorts=args.include_shorts,
        discover_pages=args.discover_pages,
    )

    if args.once:
        stats = collector.collect_once()
        logging.info("One-time collection complete: %s", stats)
        return

    logging.info(
        "Starting collector for channel=%s, poll=%ss, include_shorts=%s, output_dir=%s",
        args.channel_id,
        args.poll_seconds,
        args.include_shorts,
        args.output_dir,
    )
    while True:
        started = time.time()
        try:
            stats = collector.collect_once()
            logging.info("Collection tick: %s", stats)
        except Exception:
            logging.exception("Collection tick failed")

        elapsed = time.time() - started
        sleep_s = max(1, args.poll_seconds - int(elapsed))
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
