# short_v_long_yt

Collect longitudinal YouTube performance metrics over time.

Current scope: collect both long-form videos and Shorts with separate CSV outputs.

## What this does

- Detects recent uploads from a channel's uploads playlist.
- Captures timestamped snapshots of stats.
- Stores outputs in separate CSV files for long-form videos vs Shorts.
- Polls continuously (or runs once).

## Setup

1. Create and activate a Python environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set your API key in `keys.py` as `GOOGLE_API_KEY`.

## Run

```bash
python collect_youtube.py --channel-id UC_x5XG1OV2P6uZZ5FSM9Ttw --poll-seconds 60 --output-dir data
```

One-time run:

```bash
python collect_youtube.py --channel-id UC_x5XG1OV2P6uZZ5FSM9Ttw --once --output-dir data
```

## CSV Output Schema

Default output directory: `data/`

Files written:

- `videos.csv`: one row per long-form video (duration > 60s).
- `video_snapshots.csv`: append-only time series for long-form videos.
- `shorts.csv`: one row per Short (duration <= 60s).
- `short_snapshots.csv`: append-only time series for Shorts.

### Index CSV columns (`videos.csv`, `shorts.csv`)

- `video_id`: YouTube video ID.
- `channel_id`: YouTube channel ID.
- `title`: latest observed title.
- `published_at`: YouTube publish timestamp (ISO-8601 UTC string).
- `duration_seconds`: parsed duration in seconds.
- `first_seen_at`: first time this collector observed the item (ISO-8601 UTC string).
- `last_seen_at`: most recent observation time (ISO-8601 UTC string).

### Snapshot CSV columns (`video_snapshots.csv`, `short_snapshots.csv`)

- `captured_at`: timestamp when stats were captured (ISO-8601 UTC string).
- `video_id`: YouTube video ID.
- `channel_id`: YouTube channel ID.
- `title`: title at capture time.
- `published_at`: YouTube publish timestamp (ISO-8601 UTC string).
- `duration_seconds`: parsed duration in seconds at capture time.
- `view_count`: integer view count at capture time.
- `like_count`: integer like count at capture time, blank when hidden/unavailable.
- `comment_count`: integer comment count at capture time, blank when hidden/unavailable.

## Notes

- API quota usage scales with poll frequency and number of tracked uploads.
- Keep `--discover-pages` small unless you want deeper upload discovery/backfill.
