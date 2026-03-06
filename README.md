# short_v_long_yt

Collect longitudinal YouTube performance metrics over time.

Current scope: **regular videos first** (views, likes, comments). Shorts support is scaffolded and can be enabled later.

## What this does

- Detects recent uploads from a channel's uploads playlist.
- Captures a timestamped snapshot of stats.
- Stores snapshots in SQLite so you can analyze growth over time.
- Polls continuously (or run once).

## Setup

1. Create and activate a Python environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a YouTube Data API v3 key.
4. Export your API key:

```bash
# PowerShell
$env:YOUTUBE_API_KEY="your_api_key"
```

## Run (videos only)

```bash
python collect_youtube.py --channel-id UC_x5XG1OV2P6uZZ5FSM9Ttw --poll-seconds 60
```

Recommended for early-post-upload tracking: run with low polling interval (e.g. 30-120 seconds).

One-time run:

```bash
python collect_youtube.py --channel-id UC_x5XG1OV2P6uZZ5FSM9Ttw --once
```

## Shorts later

By default, videos with duration <= 60s are excluded.

To include shorts now (optional):

```bash
python collect_youtube.py --channel-id UC_x5XG1OV2P6uZZ5FSM9Ttw --include-shorts
```

## Storage

SQLite file default: `data/youtube_stats.db`

Tables:

- `channels`
- `videos`
- `snapshots`

Example query: latest time-series for one video

```sql
SELECT captured_at, view_count, like_count, comment_count
FROM snapshots
WHERE video_id = 'VIDEO_ID_HERE'
ORDER BY captured_at;
```

## Notes

- API quota usage scales with polling frequency and number of tracked videos.
- Keep `--discover-pages` small unless you need deep backfill.
