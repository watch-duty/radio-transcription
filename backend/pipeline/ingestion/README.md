# To use for Google Cloud Run

Run `gcloud run deploy --source .` with source code (3 files - Dockerfile, requirements, and main.py).

In the new Cloud Run service, add Env variables and re-deploy.

Send a POST JSON with a `feed_id` like:

`curl -X POST <address> -H "Content-Type: application/json" -d '{"feed_id": 46274}'`

Note that the subprocess doesn't end cleanly and might continue to write Pub/Sub messages (WIP). Re-deploy to end this as a workaround.