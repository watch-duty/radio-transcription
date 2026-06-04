target "audio-ingestion" {
  cache-from = ["type=gha,scope=audio-ingestion"]
  cache-to   = ["type=gha,mode=max,scope=audio-ingestion"]
}

target "normalization" {
  cache-from = ["type=gha,scope=normalization"]
  cache-to   = ["type=gha,mode=max,scope=normalization"]
}

target "transcription" {
  cache-from = ["type=gha,scope=transcription"]
  cache-to   = ["type=gha,mode=max,scope=transcription"]
}

target "rules-evaluation" {
  cache-from = ["type=gha,scope=rules-evaluation"]
  cache-to   = ["type=gha,mode=max,scope=rules-evaluation"]
}

target "notification" {
  cache-from = ["type=gha,scope=notification"]
  cache-to   = ["type=gha,mode=max,scope=notification"]
}

target "rules-management" {
  cache-from = ["type=gha,scope=rules-management"]
  cache-to   = ["type=gha,mode=max,scope=rules-management"]
}

target "transcripts-api" {
  cache-from = ["type=gha,scope=transcripts-api"]
  cache-to   = ["type=gha,mode=max,scope=transcripts-api"]
}

target "feeds-api" {
  cache-from = ["type=gha,scope=feeds-api"]
  cache-to   = ["type=gha,mode=max,scope=feeds-api"]
}

target "audio-segments-api" {
  cache-from = ["type=gha,scope=audio-segments-api"]
  cache-to   = ["type=gha,mode=max,scope=audio-segments-api"]
}

target "frontend-api" {
  cache-from = ["type=gha,scope=frontend-api"]
  cache-to   = ["type=gha,mode=max,scope=frontend-api"]
}
