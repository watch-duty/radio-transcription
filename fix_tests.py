import os
import re

test_dir = 'backend/pipeline/transcription/tests'
for root, _, files in os.walk(test_dir):
    for file in files:
        if file.endswith('.py'):
            path = os.path.join(root, file)
            with open(path, 'r') as f:
                content = f.read()
            content = re.sub(r'from pydub import AudioSegment\n?', 'import numpy as np\n', content)
            content = re.sub(r'from pydub\.generators import Sine\n?', '', content)
            # AudioSegment.silent(duration=X) -> np.zeros(int((X) * 16), dtype=np.int16)
            content = re.sub(r'AudioSegment\.silent\(\s*duration=([^\)]+)\s*\)', r'np.zeros(int((\1) * 16), dtype=np.int16)', content)
            content = content.replace('AudioSegment', 'np.ndarray')
            with open(path, 'w') as f:
                f.write(content)
