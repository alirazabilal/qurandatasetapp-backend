const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;
const fsSync = require('fs');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const archiver = require('archiver');
const XLSX = require('xlsx'); // ✅ NEW: Added for Excel file reading

const app = express();
const PORT = process.env.PORT || 5000;
const USER_SECRET = process.env.JWT_SECRET || 'usersecretkey';

require('dotenv').config();

const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

// S3 / Backblaze B2 client (S3-compatible)
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
const PRESIGN_EXPIRY = parseInt(process.env.B2_SIGN_URL_EXPIRY || '3600', 10);

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

// MongoDB connection
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/quran_recordings';

mongoose.connect(MONGODB_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
  .then(() => console.log('Connected to MongoDB'))
  .catch(err => console.error('MongoDB connection error:', err));

// User Schema
const userSchema = new mongoose.Schema({
  name: { type: String, required: true, unique: true, trim: true },
  password: { type: String, required: true },
  gender: { type: String, enum: ["Male", "Female"], required: true }
});

const User = mongoose.model('User', userSchema);

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

// Multer memory storage (we upload to B2 from memory, no disk)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 } // 15MB
});

let ayats = []; // ✅ NEW: Will store complete ayat data with surah info

// ✅ NEW: Load ayats from Excel file
const loadAyatsFromExcel = async () => {
  try {
    console.log('🔍 Looking for Excel file...');
    const filePath = path.join(__dirname, 'data', 'Kaggle - The Quran Dataset.xlsx');
    // Check if file exists
    if (!fsSync.existsSync(filePath)) {
      throw new Error(`Excel file not found at: ${filePath}`);
    }

    console.log('📁 Excel file found, reading...');
    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    console.log('📋 Sheet name:', sheetName);

    const worksheet = workbook.Sheets[sheetName];
    const jsonData = XLSX.utils.sheet_to_json(worksheet);

    console.log(`📊 Loaded ${jsonData.length} rows from Excel`);

    // Debug: Show first row to see column structure
    if (jsonData.length > 0) {
      console.log('🔎 First row columns:', Object.keys(jsonData[0]));
      console.log('🔎 First row data:', jsonData[0]);
    }

    // Map the data to our format using exact column names
    const formattedAyats = jsonData.map((row, index) => {
      const ayat = {
        index: index, // 0-based index for our system
        text: row.uthmani_script || row['uthmani_script'] || '', // Default to Uthmani by default
        uthmani_script: row.uthmani_script || row['uthmani_script'] || '',
        indopak_script: row.indopak_script || row['indopak_script'] || '',

        surahNameAr: row.surah_name_ar || row['surah_name_ar'] || '',
        surahNameEn: row.surah_name_en || row['surah_name_en'] || '',
        surahNo: row.surah_no || row['surah_no'] || 0,
        ayahNoInSurah: row.ayah_no_surah || row['ayah_no_surah'] || 0,
        ayahNoQuran: row.ayah_no_quran || row['ayah_no_quran'] || 0,
        juzNo: row.juz_no || row['juz_no'] || 0,
        rukoNo: row.ruko_no || row['ruko_no'] || 0
      };

      // Debug: Log first few ayats
      if (index < 3) {
        console.log(`🔎 Ayat ${index}:`, ayat);
      }

      return ayat;
    });

    console.log(`✅ Successfully formatted ${formattedAyats.length} ayats`);
    return formattedAyats;
  } catch (error) {
    console.error('❌ Error loading Excel file:', error);
    console.error('📁 Make sure "Kaggle - The Quran Dataset.xlsx" is in the server root directory');

    // Fallback: Return empty array to prevent crashes
    return [];
  }
};

// Hardcoded admin credentials
const ADMIN_PASSWORD = "2025";
const ADMIN_SECRET = "supersecretkey";

// User auth middleware
const userAuth = (req, res, next) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'No token provided' });

  const token = authHeader.split(' ')[1];
  try {
    const decoded = jwt.verify(token, USER_SECRET);
    req.user = decoded;
    next();
  } catch (err) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
};

// Define routes after Excel is loaded
loadAyatsFromExcel().then((ayatsData) => {
  ayats = ayatsData;
  console.log(`Total ayats loaded: ${ayats.length}`);
});

// User Registration
app.post('/api/users/register', async (req, res) => {
  try {
    const { name, password, gender } = req.body;

    if (!name || typeof name !== 'string' || name.trim() === '') {
      return res.status(400).json({ error: 'A valid name is required' });
    }

    if (!password || typeof password !== 'string' || password.trim() === '') {
      return res.status(400).json({ error: 'A valid password is required' });
    }

    if (!gender || !["Male", "Female"].includes(gender)) {
      return res.status(400).json({ error: 'Gender must be Male or Female' });
    }

    const existingUser = await User.findOne({ name: name.trim() });
    if (existingUser) {
      return res.status(400).json({ error: 'Name already taken' });
    }

    const hashedPassword = await bcrypt.hash(password.trim(), 10);
    const user = new User({
      name: name.trim(),
      password: hashedPassword,
      gender
    });

    await user.save();

    const token = jwt.sign({ name: name.trim(), gender }, USER_SECRET, { expiresIn: '2h' });

    res.json({
      message: 'User registered successfully',
      token,
      gender
    });
  } catch (error) {
    console.error('Error registering user:', error);
    res.status(500).json({ error: 'Failed to register user' });
  }
});

// User Login
app.post('/api/users/login', async (req, res) => {
  try {
    const { name, password } = req.body;

    if (!name || !password) {
      return res.status(400).json({ error: 'Name and password are required' });
    }

    const user = await User.findOne({ name: name.trim() });
    if (!user) return res.status(401).json({ error: 'Invalid credentials' });

    const isMatch = await bcrypt.compare(password.trim(), user.password);
    if (!isMatch) return res.status(401).json({ error: 'Invalid credentials' });

    const token = jwt.sign({ name: user.name, gender: user.gender }, USER_SECRET, { expiresIn: '2h' });

    res.json({
      message: 'Login successful',
      token,
      gender: user.gender
    });
  } catch (error) {
    console.error('Error logging in:', error);
    res.status(500).json({ error: 'Failed to log in' });
  }
});




app.get('/api/ayats/next', userAuth, async (req, res) => {
  try {
    console.log('🔍 API /api/ayats/next called');

    const recordedAyats = await Recording.find({}, 'ayatIndex');
    const recordedIndices = recordedAyats.map(r => r.ayatIndex);
    const nextAyat = ayats.find(ayat => !recordedIndices.includes(ayat.index));

    if (!nextAyat) {
      return res.json({
        ayat: null,
        recordedCount: recordedIndices.length,
        totalAyats: ayats.length
      });
    }

    // ✅ Ensure both scripts are included in the response
    const formattedAyat = {
      ...nextAyat,
      uthmani_script: nextAyat.uthmani_script || '',
      indopak_script: nextAyat.indopak_script || '',
      text: nextAyat.uthmani_script || nextAyat.text || ''
    };

    console.log('✅ Sending ayat with scripts:', {
      uthmani_sample: formattedAyat.uthmani_script?.slice(0, 20),
      indopak_sample: formattedAyat.indopak_script?.slice(0, 20)
    });

    res.json({
      ayat: formattedAyat,
      recordedCount: recordedIndices.length,
      totalAyats: ayats.length
    });
  } catch (error) {
    console.error('❌ Error fetching next ayat:', error);
    res.status(500).json({ error: 'Failed to fetch next ayat' });
  }
});


app.get('/api/ayats/next-after/:index', userAuth, async (req, res) => {
  try {
    console.log('🔍 API /api/ayats/next-after called with index:', req.params.index);

    const currentIndex = parseInt(req.params.index);
    if (isNaN(currentIndex) || currentIndex < -1 || currentIndex >= ayats.length) {
      return res.status(400).json({ error: 'Invalid current index' });
    }

    const recordedAyats = await Recording.find({}, 'ayatIndex');
    const recordedSet = new Set(recordedAyats.map(r => r.ayatIndex));

    let nextAyat = null;
    for (let i = currentIndex + 1; i < ayats.length; i++) {
      if (!recordedSet.has(i)) {
        nextAyat = ayats[i];
        break;
      }
    }

    if (!nextAyat) {
      return res.json({
        ayat: null,
        recordedCount: recordedSet.size,
        totalAyats: ayats.length
      });
    }

    // ✅ Include both scripts in response
    const formattedAyat = {
      ...nextAyat,
      uthmani_script: nextAyat.uthmani_script || '',
      indopak_script: nextAyat.indopak_script || '',
      text: nextAyat.uthmani_script || nextAyat.text || ''
    };

    console.log('✅ Sending next-after ayat with scripts:', {
      uthmani_sample: formattedAyat.uthmani_script?.slice(0, 20),
      indopak_sample: formattedAyat.indopak_script?.slice(0, 20)
    });
    console.log("🧩 Sample ayat from memory:", ayats[0]);
    console.log("🧩 Next ayat found:", nextAyat);

    res.json({
      ayat: formattedAyat,
      recordedCount: recordedSet.size,
      totalAyats: ayats.length
    });
  } catch (error) {
    console.error('❌ Error fetching next ayat after index:', error);
    res.status(500).json({ error: 'Failed to fetch next ayat' });
  }
});


// Get all ayats status (secured)
app.get('/api/ayats/status', userAuth, async (req, res) => {
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



app.post('/api/recordings/save', userAuth, upload.single('audio'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No audio file provided' });

    const { ayatIndex, ayatText } = req.body;
    const recorderName = req.user?.name;
    const recorderGender = req.user?.gender;

    if (!recorderName || !recorderGender) return res.status(401).json({ error: 'User not logged in properly' });

    const existing = await Recording.findOne({ ayatIndex: parseInt(ayatIndex) });
    if (existing) return res.status(400).json({ error: 'This ayat is already recorded.' });

    const ext = req.file.mimetype.includes('wav') ? 'wav' : req.file.mimetype.includes('mpeg') ? 'mp3' : 'webm';
    const objectKey = `ayat_${parseInt(ayatIndex) + 1}_${Date.now()}.${ext}`;

    await s3.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: objectKey,
      Body: req.file.buffer,
      ContentType: req.file.mimetype,
    }));

    const recording = new Recording({
      ayatIndex: parseInt(ayatIndex),
      ayatText,
      audioPath: objectKey,
      recorderName,
      recorderGender
    });

    await recording.save();

    res.json({
      message: 'Recording saved successfully',
      recording
    });
  } catch (err) {
    console.error('Error saving recording:', err);
    res.status(500).json({ error: 'Failed to save recording' });
  }
});

// Get all recordings (secured)
app.get('/api/recordings', userAuth, async (req, res) => {
  try {
    const recordings = await Recording.find().sort('ayatIndex');
    res.json(recordings);
  } catch (error) {
    console.error('Error fetching recordings:', error);
    res.status(500).json({ error: 'Failed to fetch recordings' });
  }
});

// Get single recording by ayat index (secured)
app.get('/api/recordings/:index', userAuth, async (req, res) => {
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

// ✅ NEW: Get ayats by Surah (secured)
app.get('/api/surah/:surahNum', userAuth, async (req, res) => {
  try {
    const surahNum = parseInt(req.params.surahNum);
    const surahAyats = ayats.filter(ayat => ayat.surahNo === surahNum);

    if (surahAyats.length === 0) {
      return res.status(404).json({ error: "Surah not found" });
    }

    res.json({
      surah: surahAyats[0].surahNameEn,
      surahAr: surahAyats[0].surahNameAr,
      ayats: surahAyats
    });
  } catch (error) {
    console.error('Error fetching surah ayats:', error);
    res.status(500).json({ error: 'Failed to fetch surah ayats' });
  }
});

// ✅ NEW: Get ayats by Para/Juz (secured)
app.get('/api/para/:paraNum', userAuth, async (req, res) => {
  try {
    const paraNum = parseInt(req.params.paraNum);
    const paraAyats = ayats.filter(ayat => ayat.juzNo === paraNum);

    if (paraAyats.length === 0) {
      return res.status(404).json({ error: "Para not found" });
    }

    res.json({
      para: paraNum,
      ayats: paraAyats
    });
  } catch (error) {
    console.error('Error fetching para ayats:', error);
    res.status(500).json({ error: 'Failed to fetch para ayats' });
  }
});

// ✅ NEW: Get all Surahs list (secured)
app.get('/api/surahs', userAuth, async (req, res) => {
  try {
    const surahs = [];
    const seen = new Set();

    ayats.forEach(ayat => {
      if (!seen.has(ayat.surahNo)) {
        surahs.push({
          surahNo: ayat.surahNo,
          surahNameEn: ayat.surahNameEn,
          surahNameAr: ayat.surahNameAr
        });
        seen.add(ayat.surahNo);
      }
    });

    surahs.sort((a, b) => a.surahNo - b.surahNo);
    res.json(surahs);
  } catch (error) {
    console.error('Error fetching surahs:', error);
    res.status(500).json({ error: 'Failed to fetch surahs' });
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

// Admin auth middleware
const adminAuth = (req, res, next) => {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: 'No token' });

  const token = authHeader.split(' ')[1];
  try {
    const decoded = jwt.verify(token, ADMIN_SECRET);
    if (decoded.role !== 'admin') throw new Error('Not admin');
    next();
  } catch (err) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
};

// Delete recording (delete from B2 + DB)
app.delete('/api/recordings/:index', adminAuth, async (req, res) => {
  try {
    const idx = parseInt(req.params.index);
    const rec = await Recording.findOne({ ayatIndex: idx });
    if (!rec) return res.status(404).json({ error: 'Recording not found' });

    // delete from B2
    await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: rec.audioPath }));

    // delete db
    await rec.deleteOne();

    res.json({ message: 'Recording deleted successfully' });
  } catch (err) {
    console.error('Error deleting recording:', err);
    res.status(500).json({ error: 'Failed to delete recording' });
  }
});

// Toggle verification
app.patch('/api/recordings/:index', adminAuth, async (req, res) => {
  try {
    const idx = parseInt(req.params.index);
    const rec = await Recording.findOne({ ayatIndex: idx });
    if (!rec) return res.status(404).json({ error: 'Recording not found' });

    rec.isVerified = !rec.isVerified;
    await rec.save();

    res.json({ message: 'Verification updated', verified: rec.isVerified });
  } catch (err) {
    console.error('Error verifying recording:', err);
    res.status(500).json({ error: 'Failed to update verification' });
  }
});

// Admin login
app.post('/api/admin/login', (req, res) => {
  const { password } = req.body;
  if (password === ADMIN_PASSWORD) {
    const token = jwt.sign({ role: 'admin' }, ADMIN_SECRET, { expiresIn: '2h' });
    return res.json({ success: true, token });
  }
  return res.status(401).json({ success: false, error: 'Invalid password' });
});

//====================================================================================
// Add these routes to your server.js file after the existing routes

// Memorization Recording Schema (add this after the Recording schema)
const memorizationSchema = new mongoose.Schema({
  ayatIndex: { type: Number, required: true },
  ayatText: { type: String, required: true },
  audioPath: { type: String, required: true },
  recordedAt: { type: Date, default: Date.now },
  recorderName: { type: String, required: true },
  recorderGender: { type: String, enum: ["Male", "Female"], required: true },
  isVerified: { type: Boolean, default: false }
});

// Create compound index to allow multiple recordings per user per ayat
memorizationSchema.index({ ayatIndex: 1, recorderName: 1 });

const MemorizationRecording = mongoose.model('MemorizationRecording', memorizationSchema);

// Get next unrecorded ayat from Para 30 for current user
app.get('/api/memorization/next', userAuth, async (req, res) => {
  try {
    const userName = req.user?.name;
    if (!userName) return res.status(401).json({ error: 'User not authenticated' });

    // Filter ayats for Para 30 (juzNo === 30)
    const para30Ayats = ayats.filter(ayat => ayat.juzNo === 30);

    if (para30Ayats.length === 0) {
      return res.status(404).json({ error: 'Para 30 data not found' });
    }

    // Get user's recorded ayats from Para 30
    const userRecordings = await MemorizationRecording.find(
      { recorderName: userName },
      'ayatIndex'
    );
    const recordedIndices = [...new Set(userRecordings.map(r => r.ayatIndex))];

    // Find next unrecorded ayat
    const nextAyat = para30Ayats.find(ayat => !recordedIndices.includes(ayat.index));

    if (!nextAyat) {
      return res.json({
        ayat: null,
        userRecorded: recordedIndices.length,
        totalAyats: para30Ayats.length
      });
    }

    const formattedAyat = {
      ...nextAyat,
      uthmani_script: nextAyat.uthmani_script || '',
      indopak_script: nextAyat.indopak_script || '',
      text: nextAyat.uthmani_script || nextAyat.text || ''
    };

    res.json({
      ayat: formattedAyat,
      userRecorded: recordedIndices.length,
      totalAyats: para30Ayats.length
    });
  } catch (error) {
    console.error('Error fetching next memorization ayat:', error);
    res.status(500).json({ error: 'Failed to fetch next ayat' });
  }
});

// Save memorization recording
app.post('/api/memorization/save', userAuth, upload.single('audio'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No audio file provided' });

    const { ayatIndex, ayatText } = req.body;
    const recorderName = req.user?.name;
    const recorderGender = req.user?.gender;

    if (!recorderName || !recorderGender) {
      return res.status(401).json({ error: 'User not logged in properly' });
    }

    const ayatIdx = parseInt(ayatIndex);
    
    // Verify ayat is from Para 30
    const ayat = ayats.find(a => a.index === ayatIdx);
    if (!ayat || ayat.juzNo !== 30) {
      return res.status(400).json({ error: 'Invalid ayat or not from Para 30' });
    }

    // Upload to B2/S3
    const ext = req.file.mimetype.includes('wav') ? 'wav' 
                : req.file.mimetype.includes('mpeg') ? 'mp3' 
                : 'webm';
    const objectKey = `memorization/${recorderName}_ayat_${ayatIdx + 1}_${Date.now()}.${ext}`;

    await s3.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: objectKey,
      Body: req.file.buffer,
      ContentType: req.file.mimetype,
    }));

    // Save to database (allows multiple recordings)
    const recording = new MemorizationRecording({
      ayatIndex: ayatIdx,
      ayatText,
      audioPath: objectKey,
      recorderName,
      recorderGender
    });

    await recording.save();

    res.json({
      message: 'Recording saved successfully',
      recording
    });
  } catch (err) {
    console.error('Error saving memorization recording:', err);
    res.status(500).json({ error: 'Failed to save recording' });
  }
});

// Get user's memorization history
app.get('/api/memorization/history', userAuth, async (req, res) => {
  try {
    const userName = req.user?.name;
    if (!userName) return res.status(401).json({ error: 'User not authenticated' });

    const recordings = await MemorizationRecording.find({ recorderName: userName })
      .sort({ recordedAt: -1 });

    res.json({ recordings });
  } catch (error) {
    console.error('Error fetching memorization history:', error);
    res.status(500).json({ error: 'Failed to fetch history' });
  }
});

// Delete user's own memorization recording
app.delete('/api/memorization/:id', userAuth, async (req, res) => {
  try {
    const userName = req.user?.name;
    const recordingId = req.params.id;

    const recording = await MemorizationRecording.findOne({
      _id: recordingId,
      recorderName: userName
    });

    if (!recording) {
      return res.status(404).json({ error: 'Recording not found or not owned by user' });
    }

    // Delete from B2
    await s3.send(new DeleteObjectCommand({ 
      Bucket: BUCKET, 
      Key: recording.audioPath 
    }));

    // Delete from database
    await recording.deleteOne();

    res.json({ message: 'Recording deleted successfully' });
  } catch (err) {
    console.error('Error deleting memorization recording:', err);
    res.status(500).json({ error: 'Failed to delete recording' });
  }
});

// Get all users' memorization progress (for admin or statistics)
app.get('/api/memorization/all-progress', userAuth, async (req, res) => {
  try {
    const recordings = await MemorizationRecording.find({}, 'recorderName ayatIndex');
    
    const progressByUser = {};
    recordings.forEach(rec => {
      if (!progressByUser[rec.recorderName]) {
        progressByUser[rec.recorderName] = new Set();
      }
      progressByUser[rec.recorderName].add(rec.ayatIndex);
    });

    const para30Count = ayats.filter(a => a.juzNo === 30).length;
    
    const progress = Object.entries(progressByUser).map(([name, indices]) => ({
      name,
      recorded: indices.size,
      total: para30Count,
      percentage: ((indices.size / para30Count) * 100).toFixed(1)
    }));

    res.json({ progress });
  } catch (error) {
    console.error('Error fetching all progress:', error);
    res.status(500).json({ error: 'Failed to fetch progress' });
  }
});


//==================CSV Me Ayat Text Column==================
// Get ALL ayats without pagination (for CSV export)
app.get('/api/admin/ayats/all', adminAuth, async (req, res) => {
  try {
    const recordings = await Recording.find({});
    const recordedMap = new Map(recordings.map(r => [r.ayatIndex, r]));

    const items = await Promise.all(ayats.map(async (ayat) => {
      const rec = recordedMap.get(ayat.index);
      if (!rec) {
        return {
          ...ayat,
          isRecorded: false,
          audioUrl: null,
          recorderName: null,
          recorderGender: null,
          isVerified: false
        };
      }

      return {
        ...ayat,
        isRecorded: true,
        audioPath: rec.audioPath,
        recordedAt: rec.recordedAt,
        recorderName: rec.recorderName,
        recorderGender: rec.recorderGender,
        isVerified: rec.isVerified
      };
    }));

    res.json({ data: items });
  } catch (err) {
    console.error("Error fetching all ayats:", err);
    res.status(500).json({ error: "Failed to fetch ayats" });
  }
});

// Get ALL memorization recordings without pagination (for CSV export)
app.get('/api/admin/memorization/all', adminAuth, async (req, res) => {
  try {
    const recordings = await MemorizationRecording.find({})
      .sort({ recordedAt: -1 });

    res.json({ recordings });
  } catch (err) {
    console.error("Error fetching all memorization recordings:", err);
    res.status(500).json({ error: "Failed to fetch recordings" });
  }
});
//===========================================================


//===========================================================
// Admin: get memorization with pagination
app.get('/api/admin/memorization', adminAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 200;
    const skip = (page - 1) * limit;

    const totalRecordings = await MemorizationRecording.countDocuments();
    const recordings = await MemorizationRecording.find({})
      .sort({ recordedAt: -1 })
      .skip(skip)
      .limit(limit);

    const recordingsWithUrls = await Promise.all(recordings.map(async (rec) => {
      const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: rec.audioPath });
      const signedUrl = await getSignedUrl(s3, getCmd, { expiresIn: PRESIGN_EXPIRY });

      return {
        _id: rec._id,
        ayatIndex: rec.ayatIndex,
        ayatText: rec.ayatText,
        audioUrl: signedUrl,
        audioPath: rec.audioPath,
        recordedAt: rec.recordedAt,
        recorderName: rec.recorderName,
        recorderGender: rec.recorderGender,
        isVerified: rec.isVerified
      };
    }));

    res.json({
      recordings: recordingsWithUrls,
      pagination: {
        currentPage: page,
        totalPages: Math.ceil(totalRecordings / limit),
        totalItems: totalRecordings,
        itemsPerPage: limit
      }
    });
  } catch (err) {
    console.error("Error fetching admin memorization recordings:", err);
    res.status(500).json({ error: "Failed to fetch recordings" });
  }
});

// Delete memorization recording (admin only)
app.delete('/api/admin/memorization/:id', adminAuth, async (req, res) => {
  try {
    const recordingId = req.params.id;
    const recording = await MemorizationRecording.findById(recordingId);

    if (!recording) {
      return res.status(404).json({ error: 'Recording not found' });
    }

    await s3.send(new DeleteObjectCommand({ 
      Bucket: BUCKET, 
      Key: recording.audioPath 
    }));

    await recording.deleteOne();

    res.json({ message: 'Recording deleted successfully' });
  } catch (err) {
    console.error('Error deleting memorization recording:', err);
    res.status(500).json({ error: 'Failed to delete recording' });
  }
});


// Toggle verification for memorization recording (admin only)
app.patch('/api/admin/memorization/verify/:id', adminAuth, async (req, res) => {
  try {
    const recordingId = req.params.id;
    const recording = await MemorizationRecording.findById(recordingId);

    if (!recording) {
      return res.status(404).json({ error: 'Recording not found' });
    }

    // Toggle verification status
    recording.isVerified = !recording.isVerified;
    await recording.save();

    res.json({ 
      message: 'Verification updated successfully',
      isVerified: recording.isVerified 
    });
  } catch (err) {
    console.error('Error updating memorization verification:', err);
    res.status(500).json({ error: 'Failed to update verification' });
  }
});

// Replace your existing CSV export route with this in server.js

// Export memorization recordings as CSV (Fixed with query token support)
// Add this route to your server.js (after other admin routes)

// Export memorization recordings as CSV
app.get('/api/admin/memorization/export-csv', adminAuth, async (req, res) => {
  try {
    const recordings = await MemorizationRecording.find({}).sort({ ayatIndex: 1, recorderName: 1 });

    // CSV Header
    let csv = 'Ayat_Index,Ayat_Number,Surah_Name,Para,Recorder_Name,Gender,Audio_Filename,Recorded_Date\n';

    // Add each recording as a row
    for (const rec of recordings) {
      const ayat = ayats.find(a => a.index === rec.ayatIndex);
      
      // Create unique filename with user name
      const timestamp = new Date(rec.recordedAt).getTime();
      const uniqueFilename = `para30_ayat${rec.ayatIndex + 1}_${rec.recorderName}_${rec.recorderGender}_${timestamp}.webm`;
      
      const row = [
        rec.ayatIndex,
        rec.ayatIndex + 1, // Ayat number (1-based)
        ayat ? `"${ayat.surahNameEn} (${ayat.surahNameAr})"` : 'Unknown',
        ayat ? ayat.juzNo : 30,
        `"${rec.recorderName}"`,
        rec.recorderGender,
        uniqueFilename,
        new Date(rec.recordedAt).toISOString()
      ].join(',');
      
      csv += row + '\n';
    }

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=memorization_para30_recordings.csv');
    res.send(csv);
  } catch (err) {
    console.error('Error exporting memorization CSV:', err);
    res.status(500).json({ error: 'Failed to export CSV' });
  }
});

// Download memorization recordings with unique filenames (UPDATED)
app.get('/api/download-memorization-audios', async (req, res) => {
  try {
    const recordings = await MemorizationRecording.find({}).sort({ recorderName: 1, ayatIndex: 1 });

    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', 'attachment; filename=memorization_para30_recordings.zip');

    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(res);

    for (const rec of recordings) {
      if (!rec.audioPath) continue;

      try {
        const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: rec.audioPath });
        const data = await s3.send(getCmd);
        
        // Create unique filename with user name
        const timestamp = new Date(rec.recordedAt).getTime();
        const folderName = `${rec.recorderName}_${rec.recorderGender}`;
        const uniqueFilename = `para30_ayat${rec.ayatIndex + 1}_${rec.recorderName}_${rec.recorderGender}_${timestamp}.webm`;
        const filePath = `${folderName}/${uniqueFilename}`;
        
        archive.append(data.Body, { name: filePath });
      } catch (err) {
        if (err.Code === "NoSuchKey") {
          console.warn(`⚠ Skipping missing file in B2: ${rec.audioPath}`);
          continue;
        } else {
          console.error(`Error fetching ${rec.audioPath}:`, err);
        }
      }
    }

    await archive.finalize();
  } catch (err) {
    console.error('Error building memorization zip:', err);
    res.status(500).json({ error: 'Failed to build zip' });
  }
});
// Download memorization recordings with unique filenames
app.get('/api/download-memorization-audios', async (req, res) => {
  try {
    const recordings = await MemorizationRecording.find({}).sort({ recorderName: 1, ayatIndex: 1 });

    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', 'attachment; filename=memorization_para30_recordings.zip');

    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(res);

    for (const rec of recordings) {
      if (!rec.audioPath) continue;

      try {
        const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: rec.audioPath });
        const data = await s3.send(getCmd);
        
        const timestamp = new Date(rec.recordedAt).getTime();
        const folderName = `${rec.recorderName}_${rec.recorderGender}`;
        const uniqueFilename = `para30_ayat${rec.ayatIndex + 1}_${rec.recorderName}_${rec.recorderGender}_${timestamp}.webm`;
        const filePath = `${folderName}/${uniqueFilename}`;
        
        archive.append(data.Body, { name: filePath });
      } catch (err) {
        if (err.Code === "NoSuchKey") {
          console.warn(`⚠ Skipping missing file in B2: ${rec.audioPath}`);
          continue;
        } else {
          console.error(`Error fetching ${rec.audioPath}:`, err);
        }
      }
    }

    await archive.finalize();
  } catch (err) {
    console.error('Error building memorization zip:', err);
    res.status(500).json({ error: 'Failed to build zip' });
  }
});


//======================================================================================

// Add this route to your server.js (after memorization routes)

// Get next group of unrecorded ayats from Para 30 for bulk recording (Surah-wise)
app.get('/api/bulk-recording/next', userAuth, async (req, res) => {
  try {
    const userName = req.user?.name;
    if (!userName) return res.status(401).json({ error: 'User not authenticated' });

    // Filter ayats for Para 30 (juzNo === 30)
    const para30Ayats = ayats.filter(ayat => ayat.juzNo === 30);

    if (para30Ayats.length === 0) {
      return res.status(404).json({ error: 'Para 30 data not found' });
    }

    // Get user's recorded ayats from Para 30
    const userRecordings = await MemorizationRecording.find(
      { recorderName: userName },
      'ayatIndex'
    );
    const recordedIndices = [...new Set(userRecordings.map(r => r.ayatIndex))];

    // Find unrecorded ayats
    const unrecordedAyats = para30Ayats.filter(ayat => !recordedIndices.includes(ayat.index));

    if (unrecordedAyats.length === 0) {
      return res.json({
        ayats: [],
        userRecorded: recordedIndices.length,
        totalAyats: para30Ayats.length
      });
    }

    // Get all unique surahs in Para 30 from unrecorded ayats
    const unrecordedSurahs = [...new Set(unrecordedAyats.map(a => a.surahNo))].sort((a, b) => a - b);
    
    if (unrecordedSurahs.length === 0) {
      return res.json({
        ayats: [],
        userRecorded: recordedIndices.length,
        totalAyats: para30Ayats.length
      });
    }

    let groupSurahs = [];
    let nextGroup = [];

    // Find next group based on first unrecorded surah
    const firstSurah = unrecordedSurahs[0];

    // Strategy:
    // Surah 78-93: 1 at a time
    // Surah 94-104: 2 at a time
    // Surah 105-114: 5 at a time

    if (firstSurah <= 93) {
      // Single surah (78-93)
      groupSurahs = [firstSurah];
    } else if (firstSurah <= 104) {
      // Pairs (94-104)
      groupSurahs = [firstSurah];
      if (unrecordedSurahs.includes(firstSurah + 1) && firstSurah + 1 <= 104) {
        groupSurahs.push(firstSurah + 1);
      }
    } else {
      // Groups of 5 (105-114)
      groupSurahs = [firstSurah];
      for (let i = 1; i < 5; i++) {
        if (unrecordedSurahs.includes(firstSurah + i) && firstSurah + i <= 114) {
          groupSurahs.push(firstSurah + i);
        }
      }
    }

    // Get all ayats for selected surahs
    nextGroup = unrecordedAyats.filter(ayat => groupSurahs.includes(ayat.surahNo));

    // Sort by ayat index
    nextGroup.sort((a, b) => a.index - b.index);

    // Format ayats with both scripts
    const formattedAyats = nextGroup.map(ayat => ({
      ...ayat,
      uthmani_script: ayat.uthmani_script || '',
      indopak_script: ayat.indopak_script || '',
      text: ayat.uthmani_script || ayat.text || ''
    }));

    res.json({
      ayats: formattedAyats,
      userRecorded: recordedIndices.length,
      totalAyats: para30Ayats.length,
      currentSurahs: groupSurahs,
      groupType: groupSurahs.length === 1 ? 'single' : 
                 groupSurahs.length === 2 ? 'pair' : 
                 groupSurahs.length >= 5 ? 'five' : 'multiple'
    });
  } catch (error) {
    console.error('Error fetching bulk recording ayats:', error);
    res.status(500).json({ error: 'Failed to fetch ayats' });
  }
});

//==============================================================================================



// Admin: get ayats with pagination
app.get('/api/admin/ayats', adminAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 200;
    const skip = (page - 1) * limit;

    const recordings = await Recording.find({});
    const recordedMap = new Map(recordings.map(r => [r.ayatIndex, r]));

    // Get paginated ayats
    const paginatedAyats = ayats.slice(skip, skip + limit);

    const items = await Promise.all(paginatedAyats.map(async (ayat) => {
      const rec = recordedMap.get(ayat.index);
      if (!rec) {
        return {
          ...ayat,
          isRecorded: false,
          audioUrl: null,
          recorderName: null,
          recorderGender: null
        };
      }

      const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: rec.audioPath });
      const signedUrl = await getSignedUrl(s3, getCmd, { expiresIn: PRESIGN_EXPIRY });

      return {
        ...ayat,
        isRecorded: true,
        audioUrl: signedUrl,
        audioPath: rec.audioPath,
        recordedAt: rec.recordedAt,
        recorderName: rec.recorderName,
        recorderGender: rec.recorderGender,
        isVerified: rec.isVerified
      };
    }));

    res.json({
      data: items,
      pagination: {
        currentPage: page,
        totalPages: Math.ceil(ayats.length / limit),
        totalItems: ayats.length,
        itemsPerPage: limit
      }
    });
  } catch (err) {
    console.error("Error fetching admin ayats:", err);
    res.status(500).json({ error: "Failed to fetch ayats" });
  }
});


// Download all audios as zip (stream from B2)
app.get('/api/download-audios', async (req, res) => {
  try {
    const recordings = await Recording.find({});

    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', 'attachment; filename=audios.zip');

    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(res);

    for (const rec of recordings) {
      if (!rec.audioPath) continue;

      try {
        const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: rec.audioPath });
        const data = await s3.send(getCmd);
        archive.append(data.Body, { name: rec.audioPath });
      } catch (err) {
        if (err.Code === "NoSuchKey") {
          console.warn(`⚠ Skipping missing file in B2: ${rec.audioPath}`);
          continue;
        } else {
          console.error(`Error fetching ${rec.audioPath}:`, err);
        }
      }
    }

    await archive.finalize();
  } catch (err) {
    console.error('Error building zip:', err);
    res.status(500).json({ error: 'Failed to build zip' });
  }
});


//========================================



//=====================================================================================
// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received. Closing HTTP server...');
  await mongoose.connection.close();
  process.exit(0);
});