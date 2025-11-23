import subprocess
import pandas as pd
import os
import numpy as np
import soundfile as sf

# Folders
filtered_folder = "filteredforwhisper"
augmented_folder = "augmented_noise"
os.makedirs(augmented_folder, exist_ok=True)

# CSV path
csv_path = "recorded_ayats_wav_volume.csv"

print("=== Starting Data Augmentation (Add Background Noise) ===\n")

# Check CSV
if not os.path.exists(csv_path):
    print(f"❌ ERROR: CSV file '{csv_path}' not found!")
    exit()

# Load CSV
df = pd.read_csv(csv_path)
print(f"✅ Loaded CSV: {len(df)} rows\n")
print(f"CSV Columns: {list(df.columns)}")
print("-" * 50)

df['original_audio'] = ""
df['augmented_audio_noise'] = ""

success_count = 0
failed_count = 0
not_found_count = 0
deleted_count = 0

# Loop through audios
for idx, row in df.iterrows():
    filename = str(row['Recording Name'])
    if not filename.endswith(".wav"):
        continue

    input_path = os.path.join(filtered_folder, filename)
    if not os.path.exists(input_path):
        not_found_count += 1
        print(f"⚠️  {filename} not found")
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
        # Load audio using soundfile
        data, sr = sf.read(input_path)
        # Add light Gaussian noise (1-3% of signal power)
        noise_amp = 0.02 * np.random.uniform(0.5, 1.0) * np.amax(data)
        noisy_data = data + noise_amp * np.random.normal(size=data.shape)
        
        # Clip to valid range [-1, 1]
        noisy_data = np.clip(noisy_data, -1.0, 1.0)

        # Save
        output_name = filename.replace(".wav", "_noise.wav")
        output_path = os.path.join(augmented_folder, output_name)
        sf.write(output_path, noisy_data, sr)

        df.at[idx, "augmented_audio_noise"] = output_path
        success_count += 1
        print(f"✅ {filename} → {output_name} (noise added)")
    except Exception as e:
        print(f"❌ {filename} failed: {e}")
        failed_count += 1

# Save CSV
output_csv = "recorded_ayats_wav_with_Augmentation.csv"
df.to_csv(output_csv, index=False)

print("\n" + "="*50)
print("=== AUGMENTATION COMPLETE (NOISE) ===")
print("="*50)
print(f"✅ Successfully augmented: {success_count}")
print(f"❌ Failed: {failed_count}")
print(f"⚠️  Not found: {not_found_count}")
print(f"🗑️  Skipped (duration >30s): {deleted_count}")
print(f"\n💾 CSV saved as: {output_csv}")
print(f"📁 Output folder: {augmented_folder}")
