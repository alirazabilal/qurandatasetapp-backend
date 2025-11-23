import subprocess
import os
import shutil

# Folders
input_folder = "webaudios"
wav_folder = "wavaudios"
filtered_folder = "filteredforwhisper"

os.makedirs(wav_folder, exist_ok=True)
os.makedirs(filtered_folder, exist_ok=True)

threshold_sec = 30  # 30 seconds

print("=== Starting Conversion: webm → wav ===\n")

failed_files = []
kept_count = 0
skipped_count = 0

for filename in os.listdir(input_folder):
    if filename.endswith(".webm"):
        input_path = os.path.join(input_folder, filename)
        output_name = filename.rsplit(".", 1)[0] + ".wav"
        wav_path = os.path.join(wav_folder, output_name)
        
        try:
            # Convert using FFmpeg directly
            cmd = [
                'ffmpeg', '-i', input_path,
                '-acodec', 'pcm_s16le',
                '-ar', '16000',
                '-ac', '1',
                wav_path,
                '-y',
                '-loglevel', 'error'
            ]
            subprocess.run(cmd, check=True, capture_output=True)
            
            # Get duration using FFprobe
            duration_cmd = [
                'ffprobe', '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                wav_path
            ]
            result = subprocess.run(duration_cmd, capture_output=True, text=True)
            duration_sec = float(result.stdout.strip())
            
            # Filter: Keep only if <= 30s
            if duration_sec <= threshold_sec:
                filtered_path = os.path.join(filtered_folder, output_name)
                shutil.copy(wav_path, filtered_path)
                print(f"✅ {output_name} | {duration_sec:.1f}s | KEPT")
                kept_count += 1
            else:
                print(f"⏭️  {output_name} | {duration_sec:.1f}s | SKIPPED (too long)")
                skipped_count += 1
                
        except Exception as e:
            print(f"❌ {filename} | ERROR: {str(e)}")
            failed_files.append(filename)

# Summary
print("\n" + "="*50)
print("=== CONVERSION & FILTERING COMPLETE ===")
print("="*50)
print(f"✅ Files kept (≤30s): {kept_count}")
print(f"⏭️  Files skipped (>30s): {skipped_count}")
if failed_files:
    print(f"❌ Failed conversions: {len(failed_files)}")
    for f in failed_files:
        print(f"   - {f}")
print(f"\nFiltered files saved in: '{filtered_folder}' folder")
print("="*50)