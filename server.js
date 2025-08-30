
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
  }
});

const User = mongoose.model('User', userSchema);

// Recording Schema
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
    required: true
  }
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

const upload = multer({
  storage,
  limits: {
    fileSize: 10 * 1024 * 1024 // 10MB limit
  }
});

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

// Define routes after CSV is loaded
loadAyatsFromCSV().then((ayatsCSV) => {
  ayats = ayatsCSV.map((text, index) => ({
    index,
    text
  }));
  console.log(`Total ayats loaded: ${ayats.length}`);

  // User Registration
  app.post('/api/users/register', async (req, res) => {
    try {
      const { name, password } = req.body;
      console.log('Register attempt with:', { name, password: password ? '[provided]' : '[missing]' }); // Debug log

      if (!name || typeof name !== 'string' || name.trim() === '') {
        return res.status(400).json({ error: 'A valid name is required' });
      }
      if (!password || typeof password !== 'string' || password.trim() === '') {
        return res.status(400).json({ error: 'A valid password is required' });
      }

      const existingUser = await User.findOne({ name: name.trim() });
      if (existingUser) {
        return res.status(400).json({ error: 'Name already taken' });
      }

      const hashedPassword = await bcrypt.hash(password.trim(), 10);
      const user = new User({ name: name.trim(), password: hashedPassword });
      await user.save();

      const token = jwt.sign({ name: name.trim() }, USER_SECRET, { expiresIn: '2h' });
      res.json({ message: 'User registered successfully', token });
    } catch (error) {
      console.error('Error registering user:', error);
      res.status(500).json({ error: 'Failed to register user' });
    }
  });

  // User Login
  app.post('/api/users/login', async (req, res) => {
    try {
      const { name, password } = req.body;
      console.log('Login attempt with:', { name, password: password ? '[provided]' : '[missing]' }); // Debug log

      if (!name || typeof name !== 'string' || name.trim() === '') {
        return res.status(400).json({ error: 'Name is required' });
      }
      if (!password || typeof password !== 'string' || password.trim() === '') {
        return res.status(400).json({ error: 'Password is required' });
      }

      const user = await User.findOne({ name: name.trim() });
      if (!user) {
        return res.status(401).json({ error: 'Invalid credentials' });
      }

      const isMatch = await bcrypt.compare(password.trim(), user.password);
      if (!isMatch) {
        return res.status(401).json({ error: 'Invalid credentials' });
      }

      const token = jwt.sign({ name: user.name }, USER_SECRET, { expiresIn: '2h' });
      res.json({ message: 'Login successful', token });
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

  // Save recording (secured, uses user name from token)
  app.post('/api/recordings/save', userAuth, upload.single('audio'), async (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({ error: 'No audio file provided' });
      }

      const { ayatIndex, ayatText } = req.body;
      const recorderName = req.user.name; // Get name from JWT

      if (!ayatIndex) {
        return res.status(400).json({ error: 'Ayat index is required' });
      }

      const existingRecording = await Recording.findOne({ ayatIndex: parseInt(ayatIndex) });
      if (existingRecording) {
        await fs.unlink(req.file.path);
        return res.status(400).json({ error: 'This ayat is already recorded.' });
      }

      const properFileName = `ayat_${parseInt(ayatIndex) + 1}_${Date.now()}.webm`;
      const oldPath = req.file.path;
      let newPath = path.join('uploads', properFileName).replace(/\\/g, "/");

      await fs.rename(oldPath, newPath);

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

  // Delete recording (admin only)
  app.delete('/api/recordings/:index', adminAuth, async (req, res) => {
    try {
      const recording = await Recording.findOne({ ayatIndex: parseInt(req.params.index) });
      if (!recording) {
        return res.status(404).json({ error: 'Recording not found' });
      }
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

  // Admin login
  app.post('/api/admin/login', (req, res) => {
    const { password } = req.body;
    if (password === ADMIN_PASSWORD) {
      const token = jwt.sign({ role: 'admin' }, ADMIN_SECRET, { expiresIn: '2h' });
      return res.json({ success: true, token });
    }
    return res.status(401).json({ success: false, error: 'Invalid password' });
  });

  // Admin API: get all ayats with recording info
  app.get('/api/admin/ayats', adminAuth, async (req, res) => {
    try {
      const recordings = await Recording.find({});
      const recordedMap = new Map(recordings.map(r => [r.ayatIndex, r]));
      const ayatsWithRecordings = ayats.map(ayat => {
        const rec = recordedMap.get(ayat.index);
        return {
          ...ayat,
          isRecorded: !!rec,
          audioUrl: rec ? `https://qurandatasetapp-backend-1.onrender.com/${rec.audioPath.replace(/\\/g, "/")}` : null,
          audioPath: rec ? `${rec.audioPath}` : null,
          recordedAt: rec ? rec.recordedAt : null,
          recorderName: rec ? rec.recorderName : null,
        };
      });
      res.json(ayatsWithRecordings);
    } catch (error) {
      console.error('Error fetching admin ayats:', error);
      res.status(500).json({ error: 'Failed to fetch ayats' });
    }
  });

  // Download all audios in a zip (admin only)
  app.get('/api/download-audios', (req, res) => {
    const uploadDir = path.join(__dirname, 'Uploads');
    res.setHeader('Content-Disposition', 'attachment; filename=audios.zip');
    res.setHeader('Content-Type', 'application/zip');

    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(res);
    archive.directory(uploadDir, false);
    archive.finalize();
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
