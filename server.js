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
const XLSX = require('xlsx');

const app = express();
const PORT = process.env.PORT || 5000;
const USER_SECRET = process.env.JWT_SECRET || 'usersecretkey';

require('dotenv').config();

const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

// S3 / Backblaze B2 client
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

// Create uploads directory
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

// Schemas
const userSchema = new mongoose.Schema({
  name: { type: String, required: true, unique: true, trim: true },
  password: { type: String, required: true },
  gender: { type: String, enum: ["Male", "Female"], required: true }
});

const recordingSchema = new mongoose.Schema({
  ayatIndex: { type: Number, required: true, unique: true },
  ayatText: { type: String, required: true },
  audioPath: { type: String, required: true },
  recordedAt: { type: Date, default: Date.now },
  recorderName: { type: String, required: true },
  recorderGender: { type: String, enum: ["Male", "Female"], required: true },
  isVerified: { type: Boolean, default: false }
});

const memorizationSchema = new mongoose.Schema({
  ayatIndex: { type: Number, required: true },
  ayatText: { type: String, required: true },
  audioPath: { type: String, required: true },
  recordedAt: { type: Date, default: Date.now },
  recorderName: { type: String, required: true },
  recorderGender: { type: String, enum: ["Male", "Female"], required: true },
  isVerified: { type: Boolean, default: false }
});

memorizationSchema.index({ ayatIndex: 1, recorderName: 1 });

const User = mongoose.model('User', userSchema);
const Recording = mongoose.model('Recording', recordingSchema);
const MemorizationRecording = mongoose.model('MemorizationRecording', memorizationSchema);

// Multer
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 }
});

let ayats = [];

// Load ayats from Excel
const loadAyatsFromExcel = async () => {
  try {
    console.log('🔍 Looking for Excel file...');
    const filePath = path.join(__dirname, 'data', 'Kaggle - The Quran Dataset.xlsx');
    
    if (!fsSync.existsSync(filePath)) {
      throw new Error(`Excel file not found at: ${filePath}`);
    }

    console.log('📁 Excel file found, reading...');
    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const jsonData = XLSX.utils.sheet_to_json(worksheet);

    console.log(`📊 Loaded ${jsonData.length} rows from Excel`);

    const formattedAyats = jsonData.map((row, index) => ({
      index: index,
      text: row.uthmani_script || row['uthmani_script'] || '',
      uthmani_script: row.uthmani_script || row['uthmani_script'] || '',
      indopak_script: row.indopak_script || row['indopak_script'] || '',
      surahNameAr: row.surah_name_ar || row['surah_name_ar'] || '',
      surahNameEn: row.surah_name_en || row['surah_name_en'] || '',
      surahNo: row.surah_no || row['surah_no'] || 0,
      ayahNoInSurah: row.ayah_no_surah || row['ayah_no_surah'] || 0,
      ayahNoQuran: row.ayah_no_quran || row['ayah_no_quran'] || 0,
      juzNo: row.juz_no || row['juz_no'] || 0,
      rukoNo: row.ruko_no || row['ruko_no'] || 0
    }));

    console.log(`✅ Successfully formatted ${formattedAyats.length} ayats`);
    return formattedAyats;
  } catch (error) {
    console.error('❌ Error loading Excel file:', error);
    return [];
  }
};

// Admin credentials
const ADMIN_PASSWORD = "2025";
const ADMIN_SECRET = "supersecretkey";

// Auth middleware
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

// Load ayats
loadAyatsFromExcel().then((ayatsData) => {
  ayats = ayatsData;
  console.log(`Total ayats loaded: ${ayats.length}`);
});

// User routes
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

// Ayat routes
app.get('/api/ayats/next', userAuth, async (req, res) => {
  try {
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

    const formattedAyat = {
      ...nextAyat,
      uthmani_script: nextAyat.uthmani_script || '',
      indopak_script: nextAyat.indopak_script || '',
      text: nextAyat.uthmani_script || nextAyat.text || ''
    };

    res.json({
      ayat: formattedAyat,
      recordedCount: recordedIndices.length,
      totalAyats: ayats.length
    });
  } catch (error) {
    console.error('Error fetching next ayat:', error);
    res.status(500).json({ error: 'Failed to fetch next ayat' });
  }
});

app.get('/api/ayats/next-after/:index', userAuth, async (req, res) => {
  try {
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

    const formattedAyat = {
      ...nextAyat,
      uthmani_script: nextAyat.uthmani_script || '',
      indopak_script: nextAyat.indopak_script || '',
      text: nextAyat.uthmani_script || nextAyat.text || ''
    };

    res.json({
      ayat: formattedAyat,
      recordedCount: recordedSet.size,
      totalAyats: ayats.length
    });
  } catch (error) {
    console.error('Error fetching next ayat after index:', error);
    res.status(500).json({ error: 'Failed to fetch next ayat' });
  }
});

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

// Recording routes
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

app.get('/api/recordings', userAuth, async (req, res) => {
  try {
    const recordings = await Recording.find().sort('ayatIndex');
    res.json(recordings);
  } catch (error) {
    console.error('Error fetching recordings:', error);
    res.status(500).json({ error: 'Failed to fetch recordings' });
  }
});

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

// Surah routes
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

// Memorization routes
app.get('/api/memorization/next', userAuth, async (req, res) => {
  try {
    const userName = req.user?.name;
    if (!userName) return res.status(401).json({ error: 'User not authenticated' });

    const para30Ayats = ayats.filter(ayat => ayat.juzNo === 30);

    if (para30Ayats.length === 0) {
      return res.status(404).json({ error: 'Para 30 data not found' });
    }

    const userRecordings = await MemorizationRecording.find(
      { recorderName: userName },
      'ayatIndex'
    );
    const recordedIndices = [...new Set(userRecordings.map(r => r.ayatIndex))];

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
    
    const ayat = ayats.find(a => a.index === ayatIdx);
    if (!ayat || ayat.juzNo !== 30) {
      return res.status(400).json({ error: 'Invalid ayat or not from Para 30' });
    }

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

// Bulk recording
app.get('/api/bulk-recording/next', userAuth, async (req, res) => {
  try {
    const userName = req.user?.name;
    if (!userName) return res.status(401).json({ error: 'User not authenticated' });

    const para30Ayats = ayats.filter(ayat => ayat.juzNo === 30);

    if (para30Ayats.length === 0) {
      return res.status(404).json({ error: 'Para 30 data not found' });
    }

    const userRecordings = await MemorizationRecording.find(
      { recorderName: userName },
      'ayatIndex'
    );
    const recordedIndices = [...new Set(userRecordings.map(r => r.ayatIndex))];

    const unrecordedAyats = para30Ayats.filter(ayat => !recordedIndices.includes(ayat.index));

    if (unrecordedAyats.length === 0) {
      return res.json({
        ayats: [],
        userRecorded: recordedIndices.length,
        totalAyats: para30Ayats.length
      });
    }

    const unrecordedSurahs = [...new Set(unrecordedAyats.map(a => a.surahNo))].sort((a, b) => a - b);
    
    if (unrecordedSurahs.length === 0) {
      return res.json({
        ayats: [],
        userRecorded: recordedIndices.length,
        totalAyats: para30Ayats.length
      });
    }

    let groupSurahs = [];
    const firstSurah = unrecordedSurahs[0];

    if (firstSurah <= 93) {
      groupSurahs = [firstSurah];
    } else if (firstSurah <= 104) {
      groupSurahs = [firstSurah];
      if (unrecordedSurahs.includes(firstSurah + 1) && firstSurah + 1 <= 104) {
        groupSurahs.push(firstSurah + 1);
      }
    } else {
      groupSurahs = [firstSurah];
      for (let i = 1; i < 5; i++) {
        if (unrecordedSurahs.includes(firstSurah + i) && firstSurah + i <= 114) {
          groupSurahs.push(firstSurah + i);
        }
      }
    }

    const nextGroup = unrecordedAyats.filter(ayat => groupSurahs.includes(ayat.surahNo));
    nextGroup.sort((a, b) => a.index - b.index);

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

// Admin routes
app.post('/api/admin/login', (req, res) => {
  const { password } = req.body;
  if (password === ADMIN_PASSWORD) {
    const token = jwt.sign({ role: 'admin' }, ADMIN_SECRET, { expiresIn: '2h' });
    return res.json({ success: true, token });
  }
  return res.status(401).json({ success: false, error: 'Invalid password' });
});

app.get('/api/admin/ayats', adminAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 200;
    const skip = (page - 1) * limit;

    const recordings = await Recording.find({});
    const recordedMap = new Map(recordings.map(r => [r.ayatIndex, r]));

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

app.delete('/api/recordings/:index', adminAuth, async (req, res) => {
  try {
    const idx = parseInt(req.params.index);
    const rec = await Recording.findOne({ ayatIndex: idx });
    if (!rec) return res.status(404).json({ error: 'Recording not found' });

    await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: rec.audioPath }));
    await rec.deleteOne();

    res.json({ message: 'Recording deleted successfully' });
  } catch (err) {
    console.error('Error deleting recording:', err);
    res.status(500).json({ error: 'Failed to delete recording' });
  }
});

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

app.patch('/api/admin/memorization/verify/:id', adminAuth, async (req, res) => {
  try {
    const recordingId = req.params.id;
    const recording = await MemorizationRecording.findById(recordingId);

    if (!recording) {
      return res.status(404).json({ error: 'Recording not found' });
    }

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

// CSV Export
app.get('/api/admin/memorization/export-csv', adminAuth, async (req, res) => {
  try {
    const recordings = await MemorizationRecording.find({}).sort({ ayatIndex: 1, recorderName: 1 });

    let csv = 'Ayat_Index,Ayat_Number,Surah_Name,Para,Recorder_Name,Gender,Audio_Filename,Recorded_Date\n';

    for (const rec of recordings) {
      const ayat = ayats.find(a => a.index === rec.ayatIndex);
      
      const timestamp = new Date(rec.recordedAt).getTime();
      const uniqueFilename = `para30_ayat${rec.ayatIndex + 1}_${rec.recorderName}_${rec.recorderGender}_${timestamp}.webm`;
      
      const row = [
        rec.ayatIndex,
        rec.ayatIndex + 1,
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

// REPLACE these two routes in your server.js

// Download recordings in chunks - FIXED VERSION with Readable Stream
app.get('/api/download-audios', async (req, res) => {
  try {
    const start = parseInt(req.query.start);
    const end = parseInt(req.query.end);
    
    // Validate parameters
    if (!start || !end || start < 1 || end < start) {
      return res.status(400).json({ error: 'Invalid start/end parameters' });
    }
    
    console.log(`📦 ZIP Download request: ${start} to ${end}`);
    
    // Get ALL recordings first, then slice
    const allRecordings = await Recording.find({})
      .sort({ ayatIndex: 1 });
    
    console.log(`📊 Total recordings in DB: ${allRecordings.length}`);
    
    // Slice to get chunk (arrays are 0-indexed, so start-1)
    const recordings = allRecordings.slice(start - 1, end);
    
    console.log(`✅ Chunk size: ${recordings.length} (from ${start} to ${end})`);

    if (recordings.length === 0) {
      return res.status(404).json({ error: 'No recordings found in this range' });
    }

    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename=recorder_audios_${start}_to_${end}.zip`);

    const archive = archiver('zip', { 
      zlib: { level: 9 }
    });
    
    // Handle errors properly
    archive.on('error', (err) => {
      console.error('❌ Archive error:', err);
      throw err;
    });

    // Pipe archive to response
    archive.pipe(res);

    let fileCount = 0;
    for (const rec of recordings) {
      if (!rec.audioPath) continue;

      try {
        console.log(`📥 Fetching ${fileCount + 1}/${recordings.length}: ${rec.audioPath}`);
        const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: rec.audioPath });
        const { Body } = await s3.send(getCmd);
        
        const filename = `ayat_${rec.ayatIndex + 1}_${rec.recorderName}_${rec.recorderGender}.webm`;
        
        // Append stream directly without converting to buffer
        archive.append(Body, { name: filename });
        fileCount++;
        console.log(`✅ Added ${fileCount}: ${filename}`);
      } catch (err) {
        if (err.Code === "NoSuchKey" || err.name === "NoSuchKey") {
          console.warn(`⚠️ Skipping missing file: ${rec.audioPath}`);
          continue;
        } else {
          console.error(`❌ Error fetching ${rec.audioPath}:`, err);
          throw err;
        }
      }
    }

    console.log(`🔄 Finalizing archive with ${fileCount} files...`);
    
    // Finalize the archive
    await archive.finalize();
    
    console.log('✅ Archive finalized successfully');
    
  } catch (err) {
    console.error('❌ Error building zip:', err);
    if (!res.headersSent) {
      res.status(500).json({ error: 'Failed to build zip: ' + err.message });
    }
  }
});

// Download memorization recordings in chunks - FIXED VERSION with Readable Stream
app.get('/api/download-memorization-audios', async (req, res) => {
  try {
    const start = parseInt(req.query.start);
    const end = parseInt(req.query.end);
    
    // Validate parameters
    if (!start || !end || start < 1 || end < start) {
      return res.status(400).json({ error: 'Invalid start/end parameters' });
    }
    
    console.log(`📦 Memorization ZIP Download request: ${start} to ${end}`);
    
    // Get ALL recordings first, then slice
    const allRecordings = await MemorizationRecording.find({})
      .sort({ recorderName: 1, ayatIndex: 1 });
    
    console.log(`📊 Total recordings in DB: ${allRecordings.length}`);
    
    // Slice to get chunk (arrays are 0-indexed, so start-1)
    const recordings = allRecordings.slice(start - 1, end);
    
    console.log(`✅ Chunk size: ${recordings.length} (from ${start} to ${end})`);

    if (recordings.length === 0) {
      return res.status(404).json({ error: 'No recordings found in this range' });
    }

    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename=para30_recordings_${start}_to_${end}.zip`);

    const archive = archiver('zip', { 
      zlib: { level: 9 }
    });
    
    // Handle errors properly
    archive.on('error', (err) => {
      console.error('❌ Archive error:', err);
      throw err;
    });

    // Pipe archive to response
    archive.pipe(res);

    let fileCount = 0;
    for (const rec of recordings) {
      if (!rec.audioPath) continue;

      try {
        console.log(`📥 Fetching ${fileCount + 1}/${recordings.length}: ${rec.audioPath}`);
        const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: rec.audioPath });
        const { Body } = await s3.send(getCmd);
        
        const timestamp = new Date(rec.recordedAt).getTime();
        const folderName = `${rec.recorderName}_${rec.recorderGender}`;
        const filename = `para30_ayat${rec.ayatIndex + 1}_${rec.recorderName}_${rec.recorderGender}_${timestamp}.webm`;
        const filePath = `${folderName}/${filename}`;
        
        // Append stream directly without converting to buffer
        archive.append(Body, { name: filePath });
        fileCount++;
        console.log(`✅ Added ${fileCount}: ${filePath}`);
      } catch (err) {
        if (err.Code === "NoSuchKey" || err.name === "NoSuchKey") {
          console.warn(`⚠️ Skipping missing file: ${rec.audioPath}`);
          continue;
        } else {
          console.error(`❌ Error fetching ${rec.audioPath}:`, err);
          throw err;
        }
      }
    }

    console.log(`🔄 Finalizing archive with ${fileCount} files...`);
    
    // Finalize the archive
    await archive.finalize();
    
    console.log('✅ Archive finalized successfully');
    
  } catch (err) {
    console.error('❌ Error building memorization zip:', err);
    if (!res.headersSent) {
      res.status(500).json({ error: 'Failed to build zip: ' + err.message });
    }
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