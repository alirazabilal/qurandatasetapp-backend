import subprocess
import pandas as pd
import os

# Folders
filtered_folder = "filteredforwhisper"   # Original .wav files
augmented_folder = "augmented_2x"       # 2x speed files
os.makedirs(augmented_folder, exist_ok=True)

# CSV path
csv_path = "recorded_ayats_wav.csv"

print("=== Starting Data Augmentation (2x Speed) ===\n")

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
df['augmented_audio_2x'] = ""

success_count = 0
failed_count = 0
not_found_count = 0

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
    
    # Set original audio path
    df.at[idx, 'original_audio'] = input_path
    
    try:
        # Create augmented file with 2x speed
        output_name = filename.replace('.wav', '_2x.wav')
        output_path = os.path.join(augmented_folder, output_name)
        
        # FFmpeg command for 2x speed
        cmd = [
            'ffmpeg', '-i', input_path,
            '-filter:a', 'atempo=2.0',
            output_path,
            '-y',
            '-loglevel', 'error'
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        
        # Set augmented audio path
        df.at[idx, 'augmented_audio_2x'] = output_path
        
        success_count += 1
        print(f"✅ {filename} → {output_name}")
        
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
output_csv = "recorded_ayats_wav_augmented.csv"
df.to_csv(output_csv, index=False)

print("\n" + "="*50)
print("=== AUGMENTATION COMPLETE ===")
print("="*50)
print(f"✅ Successfully augmented: {success_count}")
print(f"❌ Failed: {failed_count}")
print(f"⚠️  Files not found: {not_found_count}")
print(f"📊 Total CSV rows: {len(df)}")
print(f"\n💾 Updated CSV saved as: '{output_csv}'")
print(f"📁 Augmented audio files in: '{augmented_folder}' folder")
print("="*50)
print("\n📝 New columns added:")
print("   - 'original_audio': Path to original speed audio")
print("   - 'augmented_audio_2x': Path to 2x speed audio")