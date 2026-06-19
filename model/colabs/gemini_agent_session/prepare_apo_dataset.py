import json
import subprocess
import os

# Define the train and validation splits to compile
datasets = [
    {
        "name": "TRAINING SPLIT",
        "gcs_input": "gs://wd-transcription-data/segmented_audio/broadcastify/calls/training_audio_masked_v2/batch_manifest.jsonl",
        "gcs_output": "gs://wd-transcription-data/segmented_audio/broadcastify/calls/training_audio_masked_v2/apo_dataset.jsonl",
        "local_temp_input": "train_manifest_temp.jsonl",
        "local_temp_output": "train_apo_dataset.jsonl",
    },
    {
        "name": "VALIDATION (EVAL) SPLIT",
        "gcs_input": "gs://wd-transcription-data/segmented_audio/broadcastify/calls/eval_audio_masked_v2/batch_manifest.jsonl",
        "gcs_output": "gs://wd-transcription-data/segmented_audio/broadcastify/calls/eval_audio_masked_v2/apo_dataset.jsonl",
        "local_temp_input": "eval_manifest_temp.jsonl",
        "local_temp_output": "eval_apo_dataset.jsonl",
    },
]

print("=== MULTI-SPLIT VERTEX AI PROMPT OPTIMIZER COMPILER ===")

for ds in datasets:
    name = ds["name"]
    gcs_input = ds["gcs_input"]
    gcs_output = ds["gcs_output"]
    local_input = ds["local_temp_input"]
    local_output = ds["local_temp_output"]

    print(f"\n--------------------------------------------------")
    print(f"🔄 Processing {name}...")
    print(f"--------------------------------------------------")

    # 1. Download manifest from GCS
    print(f"1. Downloading from GCS: {gcs_input}...")
    try:
        subprocess.run(["gsutil", "cp", gcs_input, local_input], check=True)
        print("   ✅ Download successful!")
    except Exception as e:
        print(f"   ❌ Failed to download manifest: {e}")
        continue

    # 2. Reformat to Prompt Optimizer schema
    print(f"2. Reformatting entries into Prompt Optimizer schema...")
    compiled_count = 0
    try:
        with (
            open(local_input, "r", encoding="utf-8") as infile,
            open(local_output, "w", encoding="utf-8") as outfile,
        ):
            for line in infile:
                line = line.strip()
                if not line:
                    continue

                entry = json.loads(line)

                # Extract GCS audio URI (input) and ground truth text (output)
                audio_uri = entry.get("audio_filepath")
                ground_truth_text = entry.get("text")

                # Skip entries that don't have speech or are missing text
                if not audio_uri or not ground_truth_text:
                    continue

                # Reformat to the exact schema expected by Vertex AI Prompt Optimizer
                # 🌿 ALIGNED: Changing the ground-truth key from 'output_text' to 'target'!
                apo_entry = {
                    "input_text": audio_uri,
                    "target": ground_truth_text,
                }

                outfile.write(json.dumps(apo_entry) + "\n")
                compiled_count += 1

        print(
            f"   ✅ Successfully compiled {compiled_count} segments into {local_output}!"
        )
    except Exception as e:
        print(f"   ❌ Failed to compile dataset: {e}")
        if os.path.exists(local_input):
            os.remove(local_input)
        continue

    # 3. Upload compiled dataset back to GCS
    print(f"3. Uploading back to GCS: {gcs_output}...")
    try:
        subprocess.run(["gsutil", "cp", local_output, gcs_output], check=True)
        print(f"   ✅ Upload successful! {name} is 100% ready!")
    except Exception as e:
        print(f"   ❌ Failed to upload compiled dataset: {e}")
    finally:
        # Clean up local temporary files
        if os.path.exists(local_input):
            os.remove(local_input)
        if os.path.exists(local_output):
            os.remove(local_output)

print("\n=== ALL SPLITS COMPILED AND UPLOADED SUCCESSFULLY! ===")
