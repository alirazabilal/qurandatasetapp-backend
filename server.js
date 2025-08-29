// server.js
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;
const app = express();
const PORT = process.env.PORT || 5000;

require('dotenv').config();

// Middleware
app.use(cors());
app.use(express.json());
app.use('/uploads', express.static('uploads'));

// Create uploads directory if it doesn't exist
const createUploadsDir = async () => {
  try {
    await fs.access('uploads');
  } catch {
    await fs.mkdir('uploads');
  }
};
createUploadsDir();

// MongoDB connection - Replace with your connection string
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/quran_recordings';

mongoose.connect(MONGODB_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log('Connected to MongoDB'))
.catch(err => console.error('MongoDB connection error:', err));

// // Hardcoded CSV data - Quran Ayats (example data)
// const ayatsCSV = [
//   "بِسْمِ اللَّهِ الرَّحْمَٰنِ الرَّحِيمِ",
//   "الْحَمْدُ لِلَّهِ رَبِّ الْعَالَمِينَ",
//   "الرَّحْمَٰنِ الرَّحِيمِ",
//   "مَالِكِ يَوْمِ الدِّينِ",
//   "إِيَّاكَ نَعْبُدُ وَإِيَّاكَ نَسْتَعِينُ",
//   "اهْدِنَا الصِّرَاطَ الْمُسْتَقِيمَ",
//   "صِرَاطَ الَّذِينَ أَنْعَمْتَ عَلَيْهِمْ غَيْرِ الْمَغْضُوبِ عَلَيْهِمْ وَلَا الضَّالِّينَ",
//   "قُلْ هُوَ اللَّهُ أَحَدٌ",
//   "اللَّهُ الصَّمَدُ",
//   "لَمْ يَلِدْ وَلَمْ يُولَدْ",
//   "وَلَمْ يَكُن لَّهُ كُفُوًا أَحَدٌ",
//   "قُلْ أَعُوذُ بِرَبِّ الْفَلَقِ",
//   "مِن شَرِّ مَا خَلَقَ",
//   "وَمِن شَرِّ غَاسِقٍ إِذَا وَقَبَ",
//   "وَمِن شَرِّ النَّفَّاثَاتِ فِي الْعُقَدِ",
//   "وَمِن شَرِّ حَاسِدٍ إِذَا حَسَدَ",
//   "قُلْ أَعُوذُ بِرَبِّ النَّاسِ",
//   "مَلِكِ النَّاسِ",
//   "إِلَٰهِ النَّاسِ",
//   "مِن شَرِّ الْوَسْوَاسِ الْخَنَّاسِ"
// ];

// // Convert CSV to array of objects
// const ayats = ayatsCSV.map((text, index) => ({
//   index,
//   text: text.trim()
// }));



const csv = require('csv-parser');
const fsSync = require('fs'); // for createReadStream

let ayats = []; // will hold Quran ayats

// Load ayats from CSV file
const loadAyatsFromCSV = async () => {
  return new Promise((resolve, reject) => {
    const results = [];
    fsSync.createReadStream(path.join(__dirname, 'quran.csv'))
      .pipe(csv({ headers: false })) // no header row
      .on('data', (row) => {
        // row[0] contains the ayat text if single column
        const text = Object.values(row)[0]?.trim();
        if (text) {
          results.push(text);
        }
      })
      .on('end', () => {
        console.log(`Loaded ${results.length} ayats from CSV`);
        resolve(results);
      })
      .on('error', reject);
  });
};
// Load CSV before server starts
loadAyatsFromCSV().then((ayatsCSV) => {
  ayats = ayatsCSV.map((text, index) => ({
    index,
    text
  }));

  console.log(`Total ayats loaded: ${ayats.length}`);
});

// MongoDB Schema
const recordingSchema = new mongoose.Schema({
  ayatIndex: {
    type: Number,
    required: true,
    unique: true
  },
  ayatText: {
    type: String,
    required: true
  },
  audioPath: {
    type: String,
    required: true
  },
  recordedAt: {
    type: Date,
    default: Date.now
  },
  recorderName: {
    type: String,
    required: true,
  }
});


const Recording = mongoose.model('Recording', recordingSchema);

// Multer configuration for file upload
// Multer configuration for file upload
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'uploads/');
  },
  filename: (req, file, cb) => {
    // Generate a temporary filename first, we'll rename it later
    const tempName = `temp_${Date.now()}_${Math.random().toString(36).substring(7)}.webm`;
    cb(null, tempName);
  }
});

const upload = multer({ 
  storage,
  limits: {
    fileSize: 10 * 1024 * 1024 // 10MB limit
  }
});

// Routes

// Get next unrecorded ayat
app.get('/api/ayats/next', async (req, res) => {
  try {
    // Get all recorded ayat indices
    const recordedAyats = await Recording.find({}, 'ayatIndex');
    const recordedIndices = recordedAyats.map(r => r.ayatIndex);
    
    // Find first unrecorded ayat
    const nextAyat = ayats.find(ayat => !recordedIndices.includes(ayat.index));
    
    res.json({
      ayat: nextAyat || null,
      recordedCount: recordedIndices.length,
      totalAyats: ayats.length
    });
  } catch (error) {
    console.error('Error fetching next ayat:', error);
    res.status(500).json({ error: 'Failed to fetch next ayat' });
  }
});

// Get all ayats status
app.get('/api/ayats/status', async (req, res) => {
  try {
    const recordings = await Recording.find({}, 'ayatIndex');
    const recordedIndices = recordings.map(r => r.ayatIndex);
    
    const ayatStatus = ayats.map(ayat => ({
      ...ayat,
      isRecorded: recordedIndices.includes(ayat.index)
    }));
    
    res.json({
      ayats: ayatStatus,
      recordedCount: recordedIndices.length,
      totalCount: ayats.length
    });
  } catch (error) {
    console.error('Error fetching ayats status:', error);
    res.status(500).json({ error: 'Failed to fetch ayats status' });
  }
});



// Save recording - UPDATED VERSION
app.post('/api/recordings/save', upload.single('audio'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No audio file provided' });
    }

    const { ayatIndex, ayatText, recorderName } = req.body;

    if (!recorderName) {
      return res.status(400).json({ error: 'Recorder name is required' });
    }

    if (!ayatIndex) {
      return res.status(400).json({ error: 'Ayat index is required' });
    }

    // ✅ check if this ayat was already recorded
    const existingRecording = await Recording.findOne({ ayatIndex: parseInt(ayatIndex) });

    if (existingRecording) {
      // Delete the uploaded file since we're not using it
      await fs.unlink(req.file.path);
      return res.status(400).json({ error: 'This ayat is already recorded.' });
    }

    // ✅ Rename the file with proper ayat index
    const properFileName = `ayat_${parseInt(ayatIndex) + 1}_${Date.now()}.webm`;
    const oldPath = req.file.path;
    //const newPath = path.join('uploads', properFileName);
    let newPath = path.join('uploads', properFileName);
    // Always normalize to forward slashes
    newPath = newPath.replace(/\\/g, "\\");
    // Rename the file
    await fs.rename(oldPath, newPath);

    // ✅ save new recording with the proper file path
    const recording = new Recording({
      ayatIndex: parseInt(ayatIndex),
      ayatText,
      audioPath: newPath,
      recorderName
    });

    await recording.save();

    res.json({
      message: 'Recording saved successfully',
      recording
    });
  } catch (error) {
    console.error('Error saving recording:', error);

    // Clean up the uploaded file in case of error
    if (req.file) {
      try {
        await fs.unlink(req.file.path);
      } catch (err) {
        console.error('Error deleting file:', err);
      }
    }

    res.status(500).json({ error: 'Failed to save recording' });
  }
});


// Get all recordings
app.get('/api/recordings', async (req, res) => {
  try {
    const recordings = await Recording.find().sort('ayatIndex');
    res.json(recordings);
  } catch (error) {
    console.error('Error fetching recordings:', error);
    res.status(500).json({ error: 'Failed to fetch recordings' });
  }
});

// Get single recording by ayat index
app.get('/api/recordings/:index', async (req, res) => {
  try {
    const recording = await Recording.findOne({ ayatIndex: parseInt(req.params.index) });
    
    if (!recording) {
      return res.status(404).json({ error: 'Recording not found' });
    }
    
    res.json(recording);
  } catch (error) {
    console.error('Error fetching recording:', error);
    res.status(500).json({ error: 'Failed to fetch recording' });
  }
});

// Delete recording
app.delete('/api/recordings/:index', async (req, res) => {
  try {
    const recording = await Recording.findOne({ ayatIndex: parseInt(req.params.index) });
    
    if (!recording) {
      return res.status(404).json({ error: 'Recording not found' });
    }
    
    // Delete audio file
    try {
      await fs.unlink(recording.audioPath);
    } catch (err) {
      console.error('Error deleting file:', err);
    }
    
    await recording.deleteOne();
    
    res.json({ message: 'Recording deleted successfully' });
  } catch (error) {
    console.error('Error deleting recording:', error);
    res.status(500).json({ error: 'Failed to delete recording' });
  }
});

// Health check
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'OK',
    totalAyats: ayats.length,
    mongoConnected: mongoose.connection.readyState === 1
  });
});












const jwt = require("jsonwebtoken");

// Hardcoded admin password
const ADMIN_PASSWORD = "2025";
const ADMIN_SECRET = "supersecretkey"; // change to env var ideally

// Admin login route
app.post("/api/admin/login", (req, res) => {
  const { password } = req.body;
  if (password === ADMIN_PASSWORD) {
    const token = jwt.sign({ role: "admin" }, ADMIN_SECRET, { expiresIn: "2h" });
    return res.json({ success: true, token });
  }
  return res.status(401).json({ success: false, error: "Invalid password" });
});

// Middleware to check admin auth
const adminAuth = (req, res, next) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: "No token" });

  const token = authHeader.split(" ")[1];
  try {
    const decoded = jwt.verify(token, ADMIN_SECRET);
    if (decoded.role !== "admin") throw new Error("Not admin");
    next();
  } catch (err) {
    return res.status(401).json({ error: "Unauthorized" });
  }
};













// Admin API: get all ayats with recording info
app.get('/api/admin/ayats',adminAuth, async (req, res) => {
  try {
    const recordings = await Recording.find({});
    const recordedMap = new Map(recordings.map(r => [r.ayatIndex, r]));
    console.log(recordedMap)
    const ayatsWithRecordings = ayats.map(ayat => {
      const rec = recordedMap.get(ayat.index);
      return {
        ...ayat,
        isRecorded: !!rec,
        audioUrl: rec ? `http://localhost:${PORT}/${rec.audioPath.replace(/\\/g, "/")}` : null,
        audioPath: rec ? `${rec.audioPath}` : null,
        recordedAt: rec ? rec.recordedAt : null,
        recorderName: rec? rec.recorderName: null,

      };
    });

    res.json(ayatsWithRecordings);
  } catch (error) {
    console.error("Error fetching admin ayats:", error);
    res.status(500).json({ error: "Failed to fetch ayats" });
  }
});


const archiver = require("archiver");


// ✅ Route to download all audios in a zip
app.get("/api/download-audios", (req, res) => {
  const uploadDir = path.join(__dirname, "uploads");
  res.setHeader("Content-Disposition", "attachment; filename=audios.zip");
  res.setHeader("Content-Type", "application/zip");

  const archive = archiver("zip", { zlib: { level: 9 } });
  archive.pipe(res);

  // Add all files from uploads folder
  archive.directory(uploadDir, false);

  archive.finalize();
});






// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  console.log(`Total ayats loaded: ${ayats.length}`);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received. Closing HTTP server...');
  await mongoose.connection.close();
  process.exit(0);
});
