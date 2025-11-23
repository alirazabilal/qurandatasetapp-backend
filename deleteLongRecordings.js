const mongoose = require('mongoose');
const { S3Client, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { exec } = require('child_process');
const { promisify } = require('util');
const fs = require('fs').promises;
const path = require('path');
require('dotenv').config();

const execPromise = promisify(exec);

// S3 Client Setup
const s3 = new S3Client({
  region: process.env.B2_REGION,
  endpoint: process.env.B2_ENDPOINT,
  credentials: {
    accessKeyId: process.env.B2_KEY_ID,
    secretAccessKey: process.env.B2_APP_KEY,
  },
  forcePathStyle: false
});

const BUCKET = process.env.B2_BUCKET;

// MongoDB Connection
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/quran_recordings';

// Recording Schema
const recordingSchema = new mongoose.Schema({
  ayatIndex: { type: Number, required: true, unique: true },
  ayatText: { type: String, required: true },
  audioPath: { type: String, required: true },
  recordedAt: { type: Date, default: Date.now },
  recorderName: { type: String, required: true },
  recorderGender: { type: String, enum: ["Male", "Female"], required: true },
  isVerified: { type: Boolean, default: false }
});

const Recording = mongoose.model('Recording', recordingSchema);

// Function to get audio duration using ffprobe
async function getAudioDuration(audioPath) {
  const tempDir = './temp_audio';
  await fs.mkdir(tempDir, { recursive: true });
  const tempFilePath = path.join(tempDir, path.basename(audioPath));

  try {
    // Download from B2
    const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: audioPath });
    const data = await s3.send(getCmd);
    
    // Save to temp file
    const chunks = [];
    for await (const chunk of data.Body) {
      chunks.push(chunk);
    }
    await fs.writeFile(tempFilePath, Buffer.concat(chunks));

    // Use ffprobe to get duration
    const { stdout } = await execPromise(
      `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${tempFilePath}"`
    );
    
    const duration = parseFloat(stdout.trim());
    
    // Cleanup
    await fs.unlink(tempFilePath);
    
    return isNaN(duration) ? null : duration;
  } catch (error) {
    // Cleanup on error
    try {
      await fs.unlink(tempFilePath);
    } catch {}
    return null;
  }
}

async function findAndDeleteLongRecordings(deleteMode = false) {
  try {
    console.log('Connecting to MongoDB...');
    await mongoose.connect(MONGODB_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    console.log('Connected to MongoDB\n');

    // Check if ffprobe is available
    try {
      await execPromise('ffprobe -version');
      console.log('ffprobe found\n');
    } catch (error) {
      console.error('ERROR: ffprobe not found!');
      console.error('Please make sure FFmpeg is installed and in PATH\n');
      process.exit(1);
    }

    // Fetch all recordings
    console.log('Fetching all recordings...');
    const recordings = await Recording.find({});
    console.log(`Found ${recordings.length} total recordings\n`);

    const longRecordings = [];
    const failedChecks = [];
    let processed = 0;

    console.log('Checking audio durations using ffprobe...');
    console.log('This will take 10-20 minutes for 1132 files\n');

    for (const recording of recordings) {
      processed++;
      process.stdout.write(`\rProcessing: ${processed}/${recordings.length} | Long (>30s): ${longRecordings.length} | Failed: ${failedChecks.length}`);

      const duration = await getAudioDuration(recording.audioPath);
      
      if (duration === null) {
        failedChecks.push({
          ayatIndex: recording.ayatIndex,
          audioPath: recording.audioPath
        });
      } else if (duration > 30) {
        longRecordings.push({
          ayatIndex: recording.ayatIndex,
          ayatText: recording.ayatText.substring(0, 50) + '...',
          audioPath: recording.audioPath,
          duration: Math.round(duration * 10) / 10,
          recorderName: recording.recorderName,
          recordedAt: recording.recordedAt,
          _id: recording._id
        });
      }
    }

    console.log('\n\n' + '='.repeat(80));
    console.log('SUMMARY');
    console.log('='.repeat(80));
    console.log(`Total recordings checked: ${recordings.length}`);
    console.log(`Recordings > 30 seconds: ${longRecordings.length}`);
    console.log(`Failed to check: ${failedChecks.length}`);
    console.log('='.repeat(80) + '\n');

    if (failedChecks.length > 0) {
      console.log('FAILED CHECKS (could not determine duration):');
      failedChecks.forEach((rec, index) => {
        console.log(`   ${index + 1}. Ayat #${rec.ayatIndex} - ${rec.audioPath}`);
      });
      console.log('\n');
    }

    if (longRecordings.length === 0) {
      console.log('No recordings found that exceed 30 seconds!');
      await cleanup();
      return;
    }

    // Display list
    console.log('LIST OF RECORDINGS > 30 SECONDS:\n');
    longRecordings.forEach((rec, index) => {
      console.log(`${index + 1}. Ayat #${rec.ayatIndex} - ${rec.duration}s`);
      console.log(`   Text: ${rec.ayatText}`);
      console.log(`   Recorder: ${rec.recorderName}`);
      console.log(`   File: ${rec.audioPath}`);
      console.log(`   Recorded: ${rec.recordedAt.toLocaleString()}`);
      console.log('');
    });

    // Delete if in delete mode
    if (deleteMode) {
      console.log('DELETE MODE: Deleting all long recordings...\n');
      
      let deleted = 0;
      let deleteFailed = 0;
      
      for (const rec of longRecordings) {
        try {
          // Delete from B2
          await s3.send(new DeleteObjectCommand({ 
            Bucket: BUCKET, 
            Key: rec.audioPath 
          }));
          
          // Delete from MongoDB
          await Recording.deleteOne({ _id: rec._id });
          
          deleted++;
          console.log(`Deleted Ayat #${rec.ayatIndex} (${rec.duration}s)`);
        } catch (error) {
          deleteFailed++;
          console.error(`Failed to delete Ayat #${rec.ayatIndex}:`, error.message);
        }
      }

      console.log(`\n${'='.repeat(80)}`);
      console.log(`Successfully deleted: ${deleted}/${longRecordings.length}`);
      if (deleteFailed > 0) {
        console.log(`Failed to delete: ${deleteFailed}`);
      }
      console.log('='.repeat(80));
    } else {
      console.log('DRY RUN MODE: No recordings were deleted');
      console.log('To delete these recordings, run with DELETE_MODE=true\n');
      console.log('Command: set DELETE_MODE=true && node deleteLongRecordings.js');
    }

    await cleanup();
  } catch (error) {
    console.error('\nError:', error);
    await cleanup();
    process.exit(1);
  }
}

async function cleanup() {
  // Cleanup temp directory
  await fs.rmdir('./temp_audio', { recursive: true }).catch(() => {});
  await mongoose.connection.close();
  console.log('\nScript completed successfully');
}

// Run script
const deleteMode = process.env.DELETE_MODE === 'true';
findAndDeleteLongRecordings(deleteMode);