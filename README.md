# short_v_long_yt

Collect longitudinal YouTube performance metrics over time.

Current scope: regular videos first (views, likes, comments). Shorts support is scaffolded and can be enabled later.

## What this does

- Detects recent uploads from a channel's uploads playlist.
- Captures timestamped snapshots of video stats.
- Stores everything in plain CSV files.
- Polls continuously (or runs once).

## Setup

1. Create and activate a Python environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a YouTube Data API v3 key.
4. Export your API key:

```powershell
$env:YOUTUBE_API_KEY="your_api_key"
```

## Run (videos only)

```bash
python collect_youtube.py --channel-id UC_x5XG1OV2P6uZZ5FSM9Ttw --poll-seconds 60 --output-dir data
```

Recommended for early-post-upload tracking: use a low poll interval (30-120 seconds).

One-time run:

```bash
python collect_youtube.py --channel-id UC_x5XG1OV2P6uZZ5FSM9Ttw --once --output-dir data
```

## Shorts later

By default, videos with duration <= 60 seconds are excluded.

To include shorts now (optional):

```bash
python collect_youtube.py --channel-id UC_x5XG1OV2P6uZZ5FSM9Ttw --include-shorts --output-dir data
```

## CSV Output Schema

Default output directory: `data/`

Files written:

- `videos.csv`: one row per known video (latest metadata + first/last seen timestamps).
- `snapshots.csv`: append-only time series (one row per capture timestamp per video).

### `videos.csv` columns

- `video_id`: YouTube video ID.
- `channel_id`: YouTube channel ID.
- `title`: latest observed title.
- `published_at`: YouTube publish timestamp (ISO-8601 UTC string).
- `duration_seconds`: parsed duration in seconds.
- `is_short`: `1` if duration <= 60 seconds, else `0`.
- `first_seen_at`: first time this collector observed the video (ISO-8601 UTC string).
- `last_seen_at`: most recent observation time (ISO-8601 UTC string).

### `snapshots.csv` columns

- `captured_at`: timestamp when stats were captured (ISO-8601 UTC string).
- `video_id`: YouTube video ID.
- `channel_id`: YouTube channel ID.
- `title`: title at capture time.
- `published_at`: YouTube publish timestamp (ISO-8601 UTC string).
- `duration_seconds`: parsed duration in seconds at capture time.
- `is_short`: `1` if duration <= 60 seconds, else `0`.
- `view_count`: integer view count at capture time.
- `like_count`: integer like count at capture time, blank when hidden/unavailable.
- `comment_count`: integer comment count at capture time, blank when hidden/unavailable.

## Notes

- API quota usage scales with poll frequency and number of tracked videos.
- Keep `--discover-pages` small unless you want deeper upload discovery/backfill.
