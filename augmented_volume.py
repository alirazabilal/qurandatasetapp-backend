import subprocess
import pandas as pd
import os
import random

# Folders
filtered_folder = "filteredforwhisper"   # Original .wav files
augmented_folder = "augmented_volume"   # Volume-augmented files
os.makedirs(augmented_folder, exist_ok=True)

# CSV path
csv_path = "recorded_ayats_wav_reverb.csv"

print("=== Starting Data Augmentation (Volume Gain / Attenuation) ===\n")

# Check if CSV exists
if not os.path.exists(csv_path):
    print(f"❌ ERROR: CSV file '{csv_path}' not found!")
    exit()

# Load CSV
df = pd.read_csv(csv_path)
print(f"✅ Loaded CSV: {len(df)} rows\n")
print(f"CSV Columns: {list(df.columns)}")
print("-" * 50)

# Add new columns for original and augmented paths
df['original_audio'] = ""
df['augmented_audio_volume'] = ""

success_count = 0
failed_count = 0
not_found_count = 0
deleted_count = 0

# Process each row in CSV
for idx, row in df.iterrows():
    filename = str(row['Recording Name'])
    
    if not filename.endswith('.wav'):
        print(f"⚠️  Skipping {filename} - Not a .wav file")
        continue
    
    input_path = os.path.join(filtered_folder, filename)
    
    # Check if original file exists
    if not os.path.exists(input_path):
        print(f"⚠️  {filename} - File not found in {filtered_folder}")
        not_found_count += 1
        continue

    # Check duration (skip >30s)
    duration_cmd = [
        "ffprobe", "-v", "error", "-show_entries",
        "format=duration", "-of",
        "default=noprint_wrappers=1:nokey=1", input_path
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

    # Set original audio path
    df.at[idx, 'original_audio'] = input_path

    try:
        # Create output file name
        output_name = filename.replace('.wav', '_volume.wav')
        output_path = os.path.join(augmented_folder, output_name)
        
        # Random gain between -3dB and +3dB
        gain_db = round(random.uniform(-3.0, 3.0), 2)
        
        # FFmpeg command to change volume
        cmd = [
            'ffmpeg', '-i', input_path,
            '-filter:a', f'volume={gain_db}dB',
            output_path,
            '-y',
            '-loglevel', 'error'
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        
        # Set augmented audio path
        df.at[idx, 'augmented_audio_volume'] = output_path
        
        success_count += 1
        print(f"✅ {filename} → {output_name} (Gain {gain_db:+.2f} dB)")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ {filename} FFmpeg error:")
        print(f"   Return code: {e.returncode}")
        if e.stderr:
            print(f"   Error: {e.stderr.decode()}")
        failed_count += 1
    except Exception as e:
        print(f"❌ {filename} failed: {str(e)}")
        failed_count += 1

# Save new CSV
output_csv = "recorded_ayats_wav_volume.csv"
df.to_csv(output_csv, index=False)

print("\n" + "="*50)
print("=== AUGMENTATION COMPLETE (VOLUME GAIN) ===")
print("="*50)
print(f"✅ Successfully augmented: {success_count}")
print(f"❌ Failed: {failed_count}")
print(f"⚠️  Files not found: {not_found_count}")
print(f"🗑️  Skipped (duration >30s): {deleted_count}")
print(f"📊 Total CSV rows: {len(df)}")
print(f"\n💾 Updated CSV saved as: '{output_csv}'")
print(f"📁 Augmented audio files in: '{augmented_folder}' folder")
print("="*50)
print("\n📝 New columns added:")
print("   - 'original_audio': Path to original audio")
print("   - 'augmented_audio_volume': Path to volume-adjusted audio")
