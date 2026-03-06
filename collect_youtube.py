import argparse
import datetime as dt
import logging
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

from googleapiclient.discovery import build


ISO8601_DURATION_RE = re.compile(
    r"^PT"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+)S)?$"
)


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


class YouTubeCollector:
    def __init__(
        self,
        api_key: str,
        channel_id: str,
        db_path: str,
        include_shorts: bool = False,
        discover_pages: int = 2,
    ) -> None:
        self.api_key = api_key
        self.channel_id = channel_id
        self.db_path = db_path
        self.include_shorts = include_shorts
        self.discover_pages = max(1, discover_pages)

        self.youtube = build("youtube", "v3", developerKey=self.api_key)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS channels (
                channel_id TEXT PRIMARY KEY,
                uploads_playlist_id TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                title TEXT NOT NULL,
                published_at TEXT NOT NULL,
                duration_seconds INTEGER NOT NULL,
                is_short INTEGER NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                FOREIGN KEY(channel_id) REFERENCES channels(channel_id)
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                view_count INTEGER NOT NULL,
                like_count INTEGER,
                comment_count INTEGER,
                FOREIGN KEY(video_id) REFERENCES videos(video_id)
            );

            CREATE INDEX IF NOT EXISTS idx_videos_channel ON videos(channel_id);
            CREATE INDEX IF NOT EXISTS idx_snapshots_video_time ON snapshots(video_id, captured_at);
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def get_uploads_playlist_id(self) -> str:
        row = self.conn.execute(
            "SELECT uploads_playlist_id FROM channels WHERE channel_id = ?",
            (self.channel_id,),
        ).fetchone()
        if row:
            return row["uploads_playlist_id"]

        resp = (
            self.youtube.channels()
            .list(part="contentDetails", id=self.channel_id, maxResults=1)
            .execute()
        )
        items = resp.get("items", [])
        if not items:
            raise RuntimeError(f"Channel not found: {self.channel_id}")

        uploads_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
        self.conn.execute(
            """
            INSERT INTO channels (channel_id, uploads_playlist_id, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET uploads_playlist_id = excluded.uploads_playlist_id
            """,
            (self.channel_id, uploads_id, utc_now_iso()),
        )
        self.conn.commit()
        return uploads_id

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

        # Preserve order while deduplicating.
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
                is_short = duration_s <= 60

                rec = VideoRecord(
                    video_id=item["id"],
                    title=snippet.get("title", ""),
                    published_at=snippet.get("publishedAt", ""),
                    duration_seconds=duration_s,
                    is_short=is_short,
                    view_count=int(stats.get("viewCount", 0)),
                    like_count=int(stats["likeCount"]) if "likeCount" in stats else None,
                    comment_count=int(stats["commentCount"]) if "commentCount" in stats else None,
                )
                records.append(rec)

        return records

    def upsert_videos_and_snapshot(self, records: Iterable[VideoRecord]) -> int:
        now = utc_now_iso()
        snap_count = 0

        for rec in records:
            if rec.is_short and not self.include_shorts:
                continue

            self.conn.execute(
                """
                INSERT INTO videos (
                    video_id, channel_id, title, published_at, duration_seconds, is_short, first_seen_at, last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    title = excluded.title,
                    published_at = excluded.published_at,
                    duration_seconds = excluded.duration_seconds,
                    is_short = excluded.is_short,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    rec.video_id,
                    self.channel_id,
                    rec.title,
                    rec.published_at,
                    rec.duration_seconds,
                    int(rec.is_short),
                    now,
                    now,
                ),
            )

            self.conn.execute(
                """
                INSERT INTO snapshots (video_id, captured_at, view_count, like_count, comment_count)
                VALUES (?, ?, ?, ?, ?)
                """,
                (rec.video_id, now, rec.view_count, rec.like_count, rec.comment_count),
            )
            snap_count += 1

        self.conn.commit()
        return snap_count

    def get_tracked_video_ids(self) -> List[str]:
        rows = self.conn.execute(
            """
            SELECT video_id
            FROM videos
            WHERE channel_id = ?
              AND (is_short = 0 OR ? = 1)
            """,
            (self.channel_id, int(self.include_shorts)),
        ).fetchall()
        return [r["video_id"] for r in rows]

    def collect_once(self) -> Dict[str, int]:
        uploads_playlist_id = self.get_uploads_playlist_id()
        discovered_ids = self.discover_recent_upload_ids(uploads_playlist_id)
        discovered_set = set(discovered_ids)

        discovered_records = self.fetch_video_records(discovered_ids)
        snap_discovered = self.upsert_videos_and_snapshot(discovered_records)

        tracked_ids = [vid for vid in self.get_tracked_video_ids() if vid not in discovered_set]
        tracked_records = self.fetch_video_records(tracked_ids)
        snap_tracked = self.upsert_videos_and_snapshot(tracked_records)

        return {
            "discovered_ids": len(discovered_ids),
            "discovered_snapshots": snap_discovered,
            "tracked_ids": len(tracked_ids),
            "tracked_snapshots": snap_tracked,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect longitudinal YouTube stats (views/likes/comments) for channel uploads."
    )
    parser.add_argument("--api-key", default=os.getenv("YOUTUBE_API_KEY"), required=False)
    parser.add_argument("--channel-id", required=True)
    parser.add_argument("--db-path", default="data/youtube_stats.db")
    parser.add_argument("--poll-seconds", type=int, default=300)
    parser.add_argument("--discover-pages", type=int, default=2)
    parser.add_argument("--include-shorts", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.api_key:
        raise SystemExit("Missing API key. Provide --api-key or set YOUTUBE_API_KEY.")

    os.makedirs(os.path.dirname(args.db_path) or ".", exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    collector = YouTubeCollector(
        api_key=args.api_key,
        channel_id=args.channel_id,
        db_path=args.db_path,
        include_shorts=args.include_shorts,
        discover_pages=args.discover_pages,
    )

    try:
        if args.once:
            stats = collector.collect_once()
            logging.info("One-time collection complete: %s", stats)
            return

        logging.info(
            "Starting collector for channel=%s, poll=%ss, include_shorts=%s",
            args.channel_id,
            args.poll_seconds,
            args.include_shorts,
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
    finally:
        collector.close()


if __name__ == "__main__":
    main()
