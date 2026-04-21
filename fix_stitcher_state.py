with open('backend/pipeline/transcription/stitcher_state.py', 'r') as f:
    content = f.read()

content = content.replace('(len(chunk_data.audio) // 16)', '(len(chunk_data.audio) // (chunk_data.sample_rate // 1000))')
content = content.replace('append_end * 16', 'append_end * (chunk_data.sample_rate // 1000)')
content = content.replace('append_start * 16', 'append_start * (chunk_data.sample_rate // 1000)')

with open('backend/pipeline/transcription/stitcher_state.py', 'w') as f:
    f.write(content)
