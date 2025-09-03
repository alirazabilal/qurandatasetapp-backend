
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;
const csv = require('csv-parser');
const fsSync = require('fs');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const archiver = require('archiver');

const app = express();
const PORT = process.env.PORT || 5000;
const USER_SECRET = process.env.JWT_SECRET || 'usersecretkey';

require('dotenv').config();


const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');


// S3 / Backblaze B2 client (S3-compatible)
const s3 = new S3Client({
  region: process.env.B2_REGION,
  endpoint: process.env.B2_ENDPOINT, // e.g. https://s3.us-west-002.backblazeb2.com
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
  name: {
    type: String,
    required: true,
    unique: true,
    trim: true
  },
  password: {
    type: String,
    required: true
  },
  gender: {
    type: String,
    enum: ["Male", "Female"],
    required: true
  }
});
const User = mongoose.model('User', userSchema);

// Recording Schema
const recordingSchema = new mongoose.Schema({
  ayatIndex: { type: Number, required: true, unique: true },
  ayatText: { type: String, required: true },
  audioPath: { type: String, required: true }, 
  recordedAt: { type: Date, default: Date.now },
  recorderName: { type: String, required: true },
  recorderGender: { type: String, enum: ["Male", "Female"], required: true } // ✅ NEW
});
const Recording = mongoose.model('Recording', recordingSchema);


// Multer configuration for file upload
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'uploads/');
  },
  filename: (req, file, cb) => {
    const tempName = `temp_${Date.now()}_${Math.random().toString(36).substring(7)}.webm`;
    cb(null, tempName);
  }
});

// Multer memory storage (we upload to B2 from memory, no disk)
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 15 * 1024 * 1024 } }); // 15MB



let ayats = [];

// Load ayats from CSV file
const loadAyatsFromCSV = async () => {
  return new Promise((resolve, reject) => {
    const results = [];
    fsSync.createReadStream(path.join(__dirname, 'quran.csv'))
      .pipe(csv({ headers: false }))
      .on('data', (row) => {
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
    req.user = decoded; // Attach user data (name) to request
    next();
  } catch (err) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
};


// Define routes after CSV is loaded
loadAyatsFromCSV().then((ayatsCSV) => {
  ayats = ayatsCSV.map((text, index) => ({
    index,
    text
  }));
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
    const user = new User({ name: name.trim(), password: hashedPassword, gender });
    await user.save();

    const token = jwt.sign({ name: name.trim(), gender }, USER_SECRET, { expiresIn: '2h' });
    res.json({ message: 'User registered successfully', token, gender });
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
    res.json({ message: 'Login successful', token, gender: user.gender });
  } catch (error) {
    console.error('Error logging in:', error);
    res.status(500).json({ error: 'Failed to log in' });
  }
});


  // Get next unrecorded ayat (secured)
  app.get('/api/ayats/next', userAuth, async (req, res) => {
    try {
      const recordedAyats = await Recording.find({}, 'ayatIndex');
      const recordedIndices = recordedAyats.map(r => r.ayatIndex);
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

  // Get next unrecorded ayat after a given index (secured)
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

      res.json({
        ayat: nextAyat || null,
        recordedCount: recordedSet.size,
        totalAyats: ayats.length
      });
    } catch (error) {
      console.error('Error fetching next ayat after index:', error);
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
    const recorderGender = req.user?.gender; // ✅ from JWT

    if (!recorderName || !recorderGender) return res.status(401).json({ error: 'User not logged in properly' });

    const existing = await Recording.findOne({ ayatIndex: parseInt(ayatIndex) });
    if (existing) return res.status(400).json({ error: 'This ayat is already recorded.' });

    const ext = req.file.mimetype.includes('wav') ? 'wav'
              : req.file.mimetype.includes('mpeg') ? 'mp3'
              : 'webm';
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
    res.json({ message: 'Recording saved successfully', recording });
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







  // ------------------ Delete recording (delete from B2 + DB) ------------------
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

  
  // Admin login
  app.post('/api/admin/login', (req, res) => {
    const { password } = req.body;
    if (password === ADMIN_PASSWORD) {
      const token = jwt.sign({ role: 'admin' }, ADMIN_SECRET, { expiresIn: '2h' });
      return res.json({ success: true, token });
    }
    return res.status(401).json({ success: false, error: 'Invalid password' });
  });

  // ------------------ Admin: get ayats with presigned audio URL ------------------
// (assume you already have adminAuth middleware)
app.get('/api/admin/ayats', adminAuth, async (req, res) => {
  try {
    const recordings = await Recording.find({});
    const recordedMap = new Map(recordings.map(r => [r.ayatIndex, r]));

    const items = await Promise.all(ayats.map(async (ayat) => {
      const rec = recordedMap.get(ayat.index);
      if (!rec) {
        return { ...ayat, isRecorded: false, audioUrl: null, recorderName: null, recorderGender: null };
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
        recorderGender: rec.recorderGender   // ✅ new field
      };
    }));

    res.json(items);
  } catch (err) {
    console.error("Error fetching admin ayats:", err);
    res.status(500).json({ error: "Failed to fetch ayats" });
  }
});



// ------------------ Download all audios as zip (stream from B2) ------------------
app.get('/api/download-audios', async (req, res) => {
  try {
    const recordings = await Recording.find({});
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', 'attachment; filename=audios.zip');

    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(res);

    for (const rec of recordings) {
      if (!rec.audioPath) continue; // skip if no key stored

      try {
        const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: rec.audioPath });
        const data = await s3.send(getCmd);

        // ✅ use exact filename from DB/storage
        archive.append(data.Body, { name: rec.audioPath });

      } catch (err) {
        if (err.Code === "NoSuchKey") {
          console.warn(`⚠ Skipping missing file in B2: ${rec.audioPath}`);
          continue; // don’t crash, just skip
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
