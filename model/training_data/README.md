# Training data

Documentation and scripts to select and process training data.

## Possible sources for unlabeled data

- Watch Duty Echo recordings
- FireNotifications recordings
- Broadcastify recordings

### Watch Duty Echo recordings

https://echo-recordings.s3.us-east-1.amazonaws.com/

[selection/echo/README.md](selection/echo/README.md)

Considerations:

- Some files are empty or only noise.
- Some files are stereo mixed, with multiple radio streams on R and L channels. Avoid these.
- Some audio files have [CTCSS](https://en.wikipedia.org/wiki/Squelch#CTCSS) or [DCS](https://en.wikipedia.org/wiki/Squelch#DCS) data in them.

See [echo/README.md](echo/README.md) for more.

### FireNotifications recordings

TBD

### Broadcastify recordings

https://broadcastify.com/

TBD
