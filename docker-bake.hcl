target "audio-ingestion" {
  cache-from = ["type=gha,scope=integration-tests-audio-ingestion"]
  cache-to   = ["type=gha,mode=max,scope=integration-tests-audio-ingestion"]
}

target "normalization" {
  cache-from = ["type=gha,scope=integration-tests-normalization"]
  cache-to   = ["type=gha,mode=max,scope=integration-tests-normalization"]
}

target "segmentation" {
  cache-from = ["type=gha,scope=integration-tests-segmentation"]
  cache-to   = ["type=gha,mode=max,scope=integration-tests-segmentation"]
}

target "transcription" {
  cache-from = ["type=gha,scope=integration-tests-transcription"]
  cache-to   = ["type=gha,mode=max,scope=integration-tests-transcription"]
}

target "rules-evaluation" {
  cache-from = ["type=gha,scope=integration-tests-rules-evaluation"]
  cache-to   = ["type=gha,mode=max,scope=integration-tests-rules-evaluation"]
}

target "notification" {
  cache-from = ["type=gha,scope=integration-tests-notification"]
  cache-to   = ["type=gha,mode=max,scope=integration-tests-notification"]
}

target "rules-management" {
  cache-from = ["type=gha,scope=integration-tests-rules-management"]
  cache-to   = ["type=gha,mode=max,scope=integration-tests-rules-management"]
}

target "transcripts-api" {
  cache-from = ["type=gha,scope=integration-tests-transcripts-api"]
  cache-to   = ["type=gha,mode=max,scope=integration-tests-transcripts-api"]
}

target "feeds-api" {
  cache-from = ["type=gha,scope=integration-tests-feeds-api"]
  cache-to   = ["type=gha,mode=max,scope=integration-tests-feeds-api"]
}

target "audio-segments-api" {
  cache-from = ["type=gha,scope=integration-tests-audio-segments-api"]
  cache-to   = ["type=gha,mode=max,scope=integration-tests-audio-segments-api"]
}

target "frontend-api" {
  cache-from = ["type=gha,scope=integration-tests-frontend-api"]
  cache-to   = ["type=gha,mode=max,scope=integration-tests-frontend-api"]
}
