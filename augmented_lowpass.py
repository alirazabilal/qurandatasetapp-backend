import subprocess
import pandas as pd
import os

# Folders
filtered_folder = "filteredforwhisper"
augmented_folder = "augmented_lowpass"
os.makedirs(augmented_folder, exist_ok=True)

# CSV path
csv_path = "recorded_ayats_wav_with_Augmentation.csv"

print("=== Starting Data Augmentation (Low-Pass Filter) ===\n")

if not os.path.exists(csv_path):
    print(f"❌ ERROR: CSV file '{csv_path}' not found!")
    exit()

df = pd.read_csv(csv_path)
print(f"✅ Loaded CSV: {len(df)} rows\n")
print(f"CSV Columns: {list(df.columns)}")
print("-" * 50)

df['original_audio'] = ""
df['augmented_audio_lowpass'] = ""

success_count = 0
failed_count = 0
not_found_count = 0
deleted_count = 0

for idx, row in df.iterrows():
    filename = str(row['Recording Name'])
    if not filename.endswith(".wav"):
        continue

    input_path = os.path.join(filtered_folder, filename)
    if not os.path.exists(input_path):
        print(f"⚠️  {filename} not found")
        not_found_count += 1
        continue

    # Check duration
    duration_cmd = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", input_path
    ]
    result = subprocess.run(duration_cmd, capture_output=True, text=True)
    try:
        duration = float(result.stdout.strip())
    except:
        duration = 0.0
    if duration > 30:
        print(f"🗑️  {filename} skipped (duration {duration:.2f}s > 30s)")
        deleted_count += 1
        continue

    df.at[idx, "original_audio"] = input_path

    try:
        # Random cutoff between 3000–6000 Hz
        import random
        cutoff = random.randint(3000, 6000)

        output_name = filename.replace(".wav", "_lowpass.wav")
        output_path = os.path.join(augmented_folder, output_name)

        # FFmpeg low-pass filter
        cmd = [
            "ffmpeg", "-i", input_path,
            "-af", f"lowpass=f={cutoff}",
            output_path, "-y", "-loglevel", "error"
        ]
        subprocess.run(cmd, check=True, capture_output=True)

        df.at[idx, "augmented_audio_lowpass"] = output_path
        success_count += 1
        print(f"✅ {filename} → {output_name} (cutoff={cutoff}Hz)")

    except subprocess.CalledProcessError as e:
        print(f"❌ {filename} FFmpeg error: {e}")
        failed_count += 1
    except Exception as e:
        print(f"❌ {filename} failed: {str(e)}")
        failed_count += 1

output_csv = "recorded_ayats_wav_Augmantation.csv"
df.to_csv(output_csv, index=False)

print("\n" + "="*50)
print("=== AUGMENTATION COMPLETE (LOWPASS) ===")
print("="*50)
print(f"✅ Successfully augmented: {success_count}")
print(f"❌ Failed: {failed_count}")
print(f"⚠️  Not found: {not_found_count}")
print(f"🗑️  Skipped (>30s): {deleted_count}")
print(f"\n💾 CSV saved as: {output_csv}")
print(f"📁 Output folder: {augmented_folder}")
