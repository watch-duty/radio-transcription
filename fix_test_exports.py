import os
import re

def fix_file(path, dummy_bytes):
    with open(path, 'r') as f:
        content = f.read()
    
    # Find the function that creates dummy audio
    # and replace its body with return dummy_bytes
    
    # Let's just replace the specific lines:
    lines = content.split('\n')
    new_lines = []
    skip = False
    for line in lines:
        if "audio.export(buf, format=\"mp3\")" in line or "segment.export(buf, format=\"ipod\")" in line:
            new_lines.append(f"    return {dummy_bytes}")
            skip = True
            continue
        if skip and "return buf.getvalue()" in line:
            skip = False
            continue
        if skip and line.strip() == "":
             skip = False
        if not skip:
            new_lines.append(line)
            
    with open(path, 'w') as f:
        f.write('\n'.join(new_lines))

fix_file('backend/pipeline/ingestion/collectors/echo/tests/test_echo_collector_integration.py', 'b"dummy mp3 audio"')
fix_file('backend/pipeline/ingestion/collectors/echo/tests/test_main.py', 'b"dummy mp3 audio"')
fix_file('backend/pipeline/ingestion/collectors/tests/test_openmhz_collector_integration.py', 'b"dummy m4a audio"')

