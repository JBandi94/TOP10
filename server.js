import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { Server } from 'socket.io';
import pkg from 'tiktok-live-connector';
import { MongoClient } from 'mongodb';

const { WebcastPushConnection } = pkg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = process.env.PORT || 10000;

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: true, credentials: true } });

app.use(express.json());
app.use(express.static(__dirname));
app.get('/health', (_req, res) => res.json({ ok: true, version: 'TOP10-2.4.4' }));

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = process.env.MONGODB_DB || 'Top10';
const QUESTIONS_COLLECTION = process.env.MONGODB_COLLECTION || 'questions';
let mongoClient = null;
let questionsCollection = null;
let tiktokConnection = null;
let activeUniqueId = null;
let connectionGeneration = 0;
const usedQuestionIds = new Set();

function cleanTikTokId(value) {
  return String(value || '')
    .trim()
    .replace(/^https?:\/\/(www\.)?tiktok\.com\/@?/i, '')
    .split(/[/?#]/)[0]
    .replace(/^@+/, '')
    .trim();
}

function errorMessage(err) {
  const raw = err?.message || String(err || 'خطأ غير معروف');
  const lower = raw.toLowerCase();
  if (/not live|offline|user.?not.?found|user.?offline/.test(lower)) {
    return 'الحساب ليس في بث مباشر الآن أو أن TikTok لم يجعل البث متاحاً للاتصال.';
  }
  if (/room.?id|retrieve room|room_id/.test(lower)) {
    return `تعذر الحصول على رقم غرفة البث من TikTok. (${raw})`;
  }
  if (/timeout|timed out|etimedout/.test(lower)) {
    return `انتهت مهلة الاتصال بخوادم TikTok. (${raw})`;
  }
  if (/websocket|socket|upgrade/.test(lower)) {
    return `فشل اتصال WebSocket مع TikTok. (${raw})`;
  }
  return raw;
}

function normalizeQuestion(doc) {
  if (!doc) return null;
  const category = doc.category || doc.domain;
  const words = Array.isArray(doc.words) ? doc.words.slice(0, 10) : [];
  if (!category || words.length !== 10) return null;
  return { id: doc.id ?? String(doc._id ?? ''), category, domain: category, words };
}

function loadQuestionsFile() {
  const file = path.join(__dirname, 'questions.json');
  if (!fs.existsSync(file)) return [];
  try {
    const data = JSON.parse(fs.readFileSync(file, 'utf8'));
    return (Array.isArray(data) ? data : []).map(normalizeQuestion).filter(Boolean);
  } catch (err) {
    console.error('[Mongo] questions.json error:', err.message);
    return [];
  }
}

async function connectMongoDB() {
  if (!MONGODB_URI) {
    console.warn('[Mongo] MONGODB_URI غير موجود؛ سيتم استخدام questions.json.');
    return;
  }
  try {
    mongoClient = new MongoClient(MONGODB_URI, { serverSelectionTimeoutMS: 10000 });
    await mongoClient.connect();
    const db = mongoClient.db(DB_NAME);
    questionsCollection = db.collection(QUESTIONS_COLLECTION);
    const count = await questionsCollection.countDocuments();
    if (count === 0) {
      const questions = loadQuestionsFile();
      if (questions.length) {
        await questionsCollection.insertMany(
          questions.map(q => ({ ...q, seededAt: new Date() })),
          { ordered: false }
        );
      }
    }
    console.log(`[Mongo] connected. questions=${await questionsCollection.countDocuments()}`);
  } catch (err) {
    console.error('[Mongo] connection failed:', err.message);
    mongoClient = null;
    questionsCollection = null;
  }
}

async function getRandomQuestion() {
  if (questionsCollection) {
    let candidates = await questionsCollection.find({}).project({ _id: 0, id: 1, category: 1, domain: 1, words: 1 }).toArray();
    candidates = candidates.map(normalizeQuestion).filter(Boolean);
    let available = candidates.filter(q => !usedQuestionIds.has(String(q.id)));
    if (!available.length) {
      usedQuestionIds.clear();
      available = candidates;
    }
    if (available.length) {
      const selected = available[Math.floor(Math.random() * available.length)];
      usedQuestionIds.add(String(selected.id));
      return selected;
    }
  }

  const local = loadQuestionsFile();
  let available = local.filter(q => !usedQuestionIds.has(String(q.id)));
  if (!available.length) {
    usedQuestionIds.clear();
    available = local;
  }
  if (!available.length) return null;
  const selected = available[Math.floor(Math.random() * available.length)];
  usedQuestionIds.add(String(selected.id));
  return selected;
}

app.get('/api/question/random', async (_req, res) => {
  try {
    const question = await getRandomQuestion();
    if (!question) return res.status(404).json({ success: false, message: 'لا توجد مجالات صالحة.' });
    res.json(question);
  } catch (err) {
    console.error('[Question] error:', err);
    res.status(500).json({ success: false, message: 'تعذر جلب المجال.' });
  }
});

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    server: 'online',
    connector: 'tiktok-live-connector 2.4.4',
    mongodb: Boolean(questionsCollection),
    tiktok: Boolean(tiktokConnection),
    activeTikTok: activeUniqueId || null
  });
});

async function disconnectTikTok() {
  const old = tiktokConnection;
  tiktokConnection = null;
  activeUniqueId = null;
  if (old) {
    try { await old.disconnect(); } catch (_) {}
  }
}

function emitError(socket, message) {
  socket.emit('tiktok-error', { message });
  socket.emit('tiktok-status', { status: 'disconnected', message });
}

io.on('connection', socket => {
  console.log('[Socket.IO] client:', socket.id);

  socket.on('connect-tiktok', async (data = {}) => {
    const uniqueId = cleanTikTokId(data.uniqueId);
    if (!uniqueId) {
      emitError(socket, 'يرجى إدخال اسم مستخدم TikTok صحيح.');
      return;
    }

    const generation = ++connectionGeneration;
    await disconnectTikTok();

    socket.emit('tiktok-connecting', { uniqueId, message: 'جاري محاولة الاتصال بالبث...' });
    socket.emit('tiktok-status', { status: 'connecting', uniqueId, message: 'جاري محاولة الاتصال بالبث...' });

    let connection;
    try {
      // This is intentionally based on the supplied working 2.4.4 example:
      // legacy WebcastPushConnection + HTTP polling, with websocket upgrade disabled.
      connection = new WebcastPushConnection(uniqueId, {
        processInitialData: false,
        fetchRoomInfoOnConnect: true,
        enableExtendedGiftInfo: true,
        enableWebsocketUpgrade: false,
        requestPollingIntervalMs: 1500,
        requestOptions: { timeout: 10000 }
      });

      tiktokConnection = connection;
      activeUniqueId = uniqueId;
      let connected = false;

      connection.on('connected', state => {
        if (generation !== connectionGeneration || tiktokConnection !== connection) return;
        connected = true;
        socket.emit('tiktok-connected', {
          success: true,
          uniqueId,
          roomId: state?.roomId || null,
          message: `تم الاتصال بالبث بنجاح: @${uniqueId}`
        });
        socket.emit('tiktok-status', {
          status: 'connected',
          uniqueId,
          roomId: state?.roomId || null,
          message: `تم الاتصال بالبث بنجاح: @${uniqueId}`
        });
        console.log(`[TikTok] connected @${uniqueId} room=${state?.roomId || 'unknown'}`);
      });

      connection.on('chat', data => {
        if (generation !== connectionGeneration || tiktokConnection !== connection) return;
        io.emit('chat', {
          uniqueId: data?.uniqueId || '',
          nickname: data?.nickname || data?.uniqueId || 'مستخدم',
          comment: data?.comment || '',
          profilePictureUrl: data?.profilePictureUrl || ''
        });
      });

      connection.on('member', data => {
        if (generation !== connectionGeneration || tiktokConnection !== connection) return;
        io.emit('tiktok-member-event', {
          uniqueId: data?.uniqueId || '',
          nickname: data?.nickname || data?.uniqueId || 'مستخدم',
          profilePictureUrl: data?.profilePictureUrl || '',
          action: 'join'
        });
      });

      connection.on('streamEnd', () => {
        if (generation !== connectionGeneration) return;
        emitError(socket, 'انتهى البث المباشر.');
        if (tiktokConnection === connection) {
          tiktokConnection = null;
          activeUniqueId = null;
        }
      });

      connection.on('disconnected', () => {
        if (generation !== connectionGeneration) return;
        if (!connected) {
          emitError(socket, 'تم قطع الاتصال قبل اكتمال الاتصال بالبث.');
        } else {
          socket.emit('tiktok-status', { status: 'disconnected', message: 'تم قطع الاتصال بالبث المباشر.' });
        }
        if (tiktokConnection === connection) {
          tiktokConnection = null;
          activeUniqueId = null;
        }
      });

      connection.on('error', err => {
        if (generation !== connectionGeneration) return;
        const msg = errorMessage(err);
        console.error(`[TikTok] error @${uniqueId}:`, err);
        emitError(socket, msg);
        if (tiktokConnection === connection) {
          tiktokConnection = null;
          activeUniqueId = null;
        }
      });

      const timeout = setTimeout(async () => {
        if (generation !== connectionGeneration || connected) return;
        console.error(`[TikTok] timeout @${uniqueId}`);
        emitError(socket, 'انتهت مهلة الاتصال بالبث بعد 20 ثانية. تحقق من أن البث مفتوح واسم المستخدم صحيح.');
        if (tiktokConnection === connection) {
          tiktokConnection = null;
          activeUniqueId = null;
          try { await connection.disconnect(); } catch (_) {}
        }
      }, 20000);

      connection.connect()
        .then(state => {
          clearTimeout(timeout);
          if (generation !== connectionGeneration || tiktokConnection !== connection) return;
          if (!connected) {
            connected = true;
            socket.emit('tiktok-connected', {
              success: true,
              uniqueId,
              roomId: state?.roomId || null,
              message: `تم الاتصال بالبث بنجاح: @${uniqueId}`
            });
            socket.emit('tiktok-status', {
              status: 'connected',
              uniqueId,
              roomId: state?.roomId || null,
              message: `تم الاتصال بالبث بنجاح: @${uniqueId}`
            });
          }
        })
        .catch(err => {
          clearTimeout(timeout);
          if (generation !== connectionGeneration) return;
          console.error(`[TikTok] connect failed @${uniqueId}:`, err);
          if (tiktokConnection === connection) {
            tiktokConnection = null;
            activeUniqueId = null;
          }
          emitError(socket, errorMessage(err));
        });
    } catch (err) {
      console.error('[TikTok] constructor error:', err);
      emitError(socket, errorMessage(err));
    }
  });

  socket.on('disconnect-tiktok', async () => {
    ++connectionGeneration;
    await disconnectTikTok();
    socket.emit('tiktok-status', { status: 'disconnected', message: 'تم فصل الاتصال بالحساب.' });
  });

  socket.on('disconnect', () => console.log('[Socket.IO] disconnected:', socket.id));
});

async function start() {
  await connectMongoDB();
  server.listen(PORT, '0.0.0.0', () => console.log(`TOP10 server listening on ${PORT}`));
}

start().catch(err => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});

process.on('SIGTERM', async () => {
  try { await disconnectTikTok(); } catch (_) {}
  try { if (mongoClient) await mongoClient.close(); } catch (_) {}
  process.exit(0);
});
