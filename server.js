import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import { WebcastPushConnection } from 'tiktok-live-connector/legacy';
import { MongoClient } from 'mongodb';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = process.env.PORT || 10000;
const MONGODB_URI = process.env.MONGODB_URI || '';
const MONGODB_DB = process.env.MONGODB_DB || 'Top10';
const MONGODB_COLLECTION = process.env.MONGODB_COLLECTION || 'questions';

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: true, credentials: true }
});

app.use(express.static(__dirname));
app.get('/api/health', (_req, res) => {
  res.json({ ok: true, service: 'top10-tiktok-game', tiktokConnector: '2.5.0-legacy' });
});

let mongoClient = null;
let questionsCollection = null;
let localQuestions = [];

try {
  const p = path.join(__dirname, 'questions.json');
  if (fs.existsSync(p)) {
    localQuestions = JSON.parse(fs.readFileSync(p, 'utf8'));
  }
} catch (err) {
  console.error('[Questions] local load error:', err.message);
}

async function initMongo() {
  if (!MONGODB_URI) {
    console.log('[MongoDB] MONGODB_URI not set; using local questions.json fallback.');
    return;
  }
  try {
    mongoClient = new MongoClient(MONGODB_URI, {
      serverSelectionTimeoutMS: 10000,
      connectTimeoutMS: 10000
    });
    await mongoClient.connect();
    const db = mongoClient.db(MONGODB_DB);
    questionsCollection = db.collection(MONGODB_COLLECTION);

    const count = await questionsCollection.countDocuments();
    if (count === 0 && Array.isArray(localQuestions) && localQuestions.length) {
      await questionsCollection.insertMany(localQuestions.map(q => ({ ...q })));
      console.log(`[MongoDB] Seeded ${localQuestions.length} questions from questions.json.`);
    }
    console.log(`[MongoDB] Connected to ${MONGODB_DB}.${MONGODB_COLLECTION}`);
  } catch (err) {
    console.error('[MongoDB] connection error:', err.message);
    mongoClient = null;
    questionsCollection = null;
  }
}

app.get('/api/question/random', async (_req, res) => {
  try {
    if (questionsCollection) {
      const docs = await questionsCollection.aggregate([{ $sample: { size: 1 } }]).toArray();
      if (docs[0]) return res.json(docs[0]);
    }
    if (Array.isArray(localQuestions) && localQuestions.length) {
      return res.json(localQuestions[Math.floor(Math.random() * localQuestions.length)]);
    }
    return res.status(404).json({ error: 'No questions available' });
  } catch (err) {
    console.error('[Questions] random error:', err.message);
    return res.status(500).json({ error: 'Failed to load question' });
  }
});

const connections = new Map();

function cleanUsername(value) {
  return String(value || '').trim().replace(/^@+/, '').replace(/\s+/g, '');
}

function errorMessage(err) {
  if (!err) return 'خطأ غير معروف أثناء الاتصال.';
  if (typeof err === 'string') return err;
  const parts = [];
  if (err.message) parts.push(err.message);
  if (err.code !== undefined) parts.push(`code=${err.code}`);
  if (err.name && err.name !== 'Error') parts.push(err.name);
  return parts.join(' | ') || JSON.stringify(err);
}

function forwardChat(socket, data) {
  const user = data?.user || data?.userDetails || {};
  socket.emit('chat', {
    uniqueId: data?.uniqueId || user?.uniqueId || user?.unique_id || '',
    nickname: data?.nickname || user?.nickname || user?.displayName || '',
    profilePictureUrl:
      data?.profilePictureUrl ||
      user?.profilePictureUrl ||
      user?.profilePictureUrls?.[0] || '',
    comment: data?.comment || ''
  });
}

function forwardMember(socket, data) {
  const user = data?.user || data?.userDetails || {};
  socket.emit('tiktok-member-event', {
    uniqueId: data?.uniqueId || user?.uniqueId || user?.unique_id || '',
    nickname: data?.nickname || user?.nickname || user?.displayName || '',
    profilePictureUrl:
      data?.profilePictureUrl ||
      user?.profilePictureUrl ||
      user?.profilePictureUrls?.[0] || ''
  });
}

async function closeConnection(socketId) {
  const item = connections.get(socketId);
  if (!item) return;
  connections.delete(socketId);
  try {
    await item.connection.disconnect();
  } catch (_) {}
}

io.on('connection', (socket) => {
  console.log(`[Socket] connected ${socket.id}`);

  socket.on('connect-tiktok', async (payload = {}) => {
    const username = cleanUsername(payload.uniqueId);
    if (!username) {
      socket.emit('tiktok-error', { message: 'يرجى إدخال اسم مستخدم TikTok صحيح.' });
      return;
    }

    await closeConnection(socket.id);
    console.log(`[TikTok] attempting legacy connection to @${username} for ${socket.id}`);

    let connection;
    try {
      connection = new WebcastPushConnection(username, {
        processInitialData: true,
        enableExtendedGiftInfo: false,
        requestOptions: { timeout: 15000 },
        websocketOptions: { timeout: 20000 }
      });
    } catch (err) {
      console.error('[TikTok] constructor error:', err);
      socket.emit('tiktok-error', { message: errorMessage(err) });
      return;
    }

    connections.set(socket.id, { connection, username });
    let settled = false;
    let timer = null;

    const fail = (err) => {
      const message = errorMessage(err);
      console.error(`[TikTok] @${username} error: ${message}`);
      if (!settled) {
        settled = true;
        if (timer) clearTimeout(timer);
        socket.emit('tiktok-error', { message });
      }
    };

    connection.on('connected', (state) => {
      settled = true;
      if (timer) clearTimeout(timer);
      console.log(`[TikTok] CONNECTED @${username} roomId=${state?.roomId || 'unknown'}`);
      socket.emit('tiktok-connected', {
        uniqueId: username,
        roomId: state?.roomId || null
      });
    });

    connection.on('chat', (data) => forwardChat(socket, data));
    connection.on('member', (data) => forwardMember(socket, data));

    connection.on('error', fail);

    connection.on('disconnected', (data) => {
      console.log(`[TikTok] DISCONNECTED @${username}`, data || '');
      if (settled) {
        socket.emit('tiktok-error', { message: 'انقطع اتصال البث.' });
      }
    });

    connection.on('streamEnd', (data) => {
      console.log(`[TikTok] STREAM END @${username}`, data || '');
      socket.emit('tiktok-error', { message: 'انتهى البث المباشر.' });
    });

    timer = setTimeout(async () => {
      if (settled) return;
      settled = true;
      const message = 'انتهت مهلة الاتصال بتيكتوك بعد 25 ثانية. تأكد أن البث مباشر وأن اسم المستخدم صحيح.';
      console.error(`[TikTok] TIMEOUT @${username}`);
      socket.emit('tiktok-error', { message });
      try { await connection.disconnect(); } catch (_) {}
      connections.delete(socket.id);
    }, 25000);

    try {
      await connection.connect();
    } catch (err) {
      fail(err);
      try { await connection.disconnect(); } catch (_) {}
      connections.delete(socket.id);
    }
  });

  socket.on('disconnect', async () => {
    console.log(`[Socket] disconnected ${socket.id}`);
    await closeConnection(socket.id);
  });
});

await initMongo();

server.listen(PORT, '0.0.0.0', () => {
  console.log(`TOP10 server listening on port ${PORT}`);
  console.log('TikTok connector: tiktok-live-connector 2.5.0 / legacy WebcastPushConnection');
});
