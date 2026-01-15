# Training data

Documentation and scripts to select and process training data.

## Possible sources for unlabeled data

- Watch Duty Echo recordings
- FireNotifications recordings
- Broadcastify recordings

### Watch Duty Echo recordings

https://echo-recordings.s3.us-east-1.amazonaws.com/

Considerations:

- Some files are empty or only noise.
- Some files are stereo mixed, with multiple radio streams on R and L channels. Avoid these.
- Some audio files have [CTCSS](https://en.wikipedia.org/wiki/Squelch#CTCSS) or [DCS](https://en.wikipedia.org/wiki/Squelch#DCS) data in them.

See [echo/](echo/README.md)

### FireNotifications recordings

https://player.textmefires.info/audioplay/folder_play.html (auth required)

Considerations:

- Small audio samples (approx. 1 file per transmission)
- Generally high quality
- Does also archive Watch Duty Echo audio, but easy to filter out
- Archived mp3s are behind HTTP auth, so must be copied to use for labeling

See [fire_notifications/](fire_notifications/)

### Broadcastify recordings

https://broadcastify.com/
https://api.bcfy.io/ (auth required)

Considerations:

- Some streams archive dead air (ie, 30m mp3s with no audio content)
- Some streams trim dead air out
- Most streams are mono mp3
- Hardware, channels, vary a lot

See [broadcastify/](broadcastify/).
