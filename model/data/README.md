# Training data artifacts

This directory holds **data artifacts only** — manifests, label exports, segmentation outputs,
and inference results. Fetch scripts and API clients have been moved to a sibling directory.

## Directory layout

```
model/
├── data/                      — data artifacts (this directory)
│   ├── inference_manifests/   — per-model inference results merged with ground truth
│   ├── manifests/             — batch manifests consumed by transcription notebooks
│   ├── label_studio_exports/  — Label Studio annotation exports
│   └── README.md              — this file
└── data_sources/              — fetch scripts and API clients
    ├── broadcastify/          — Broadcastify API client + archive-URL fetch scripts
    ├── echo/                  — Watch Duty Echo S3 scanner
    └── fire_notifications/    — FireNotifications API client + archive fetch scripts
```

## Data sources

Fetch code for each source now lives under `model/data_sources/`. Run scripts from within
each subdirectory (sibling imports rely on being co-located):

```bash
cd model/data_sources/broadcastify && python get_all_feeds.py
cd model/data_sources/echo && python s3_file_scanner.py
cd model/data_sources/fire_notifications && python fetch_fn_archives_day.py
```

### Watch Duty Echo recordings

https://echo-recordings.s3.us-east-1.amazonaws.com/

Considerations:

- Some files are empty or only noise.
- Some files are stereo mixed, with multiple radio streams on R and L channels. Avoid these.
- Some audio files have [CTCSS](https://en.wikipedia.org/wiki/Squelch#CTCSS) or
  [DCS](https://en.wikipedia.org/wiki/Squelch#DCS) data in them.

See [../data_sources/echo/](../data_sources/echo/README.md)

### FireNotifications recordings

https://player.textmefires.info/audioplay/folder_play.html (auth required)

Considerations:

- Small audio samples (approx. 1 file per transmission)
- Generally high quality
- Does also archive Watch Duty Echo audio, but easy to filter out
- Archived mp3s are behind HTTP auth, so must be copied to use for labeling

See [../data_sources/fire_notifications/](../data_sources/fire_notifications/README.md)

### Broadcastify recordings

https://broadcastify.com/
https://api.bcfy.io/ (auth required)

Considerations:

- Some streams archive dead air (ie, 30m mp3s with no audio content)
- Some streams trim dead air out
- Most streams are mono mp3
- Hardware, channels, vary a lot

See [../data_sources/broadcastify/](../data_sources/broadcastify/README.md)
