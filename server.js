import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { Server } from 'socket.io';
import { TikTokLiveConnection, WebcastEvent } from 'tiktok-live-connector';
import { MongoClient } from 'mongodb';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = process.env.PORT || 10000;

const app = express();
const server = http.createServer(app);
const io = new Server(server, { 
  cors: { 
    origin: true, 
    credentials: true 
  },
  transports: ['websocket', 'polling']
});

app.use(express.json());
app.use(express.static(__dirname));

// Health check endpoint
app.get('/health', (_req, res) => res.json({ ok: true, version: 'TOP10-2.5.0' }));

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = process.env.MONGODB_DB || 'Top10';
const QUESTIONS_COLLECTION = process.env.MONGODB_COLLECTION || 'questions';

// State
let mongoClient = null;
let questionsCollection = null;
let tiktokConnection = null;
let activeUniqueId = null;
let connectionGeneration = 0;
const usedQuestionIds = new Set();

// ==================== UTILITIES ====================

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
  if (/forbidden|403/.test(lower)) {
    return 'تم حظر الوصول من TikTok. جرب لاحقاً.';
  }
  if (/authenticate|auth|cookie/.test(lower)) {
    return 'مشكلة في المصادقة مع TikTok. جرب لاحقاً.';
  }
  return raw;
}

function normalizeQuestion(doc) {
  if (!doc) return null;
  const category = doc.category || doc.domain;
  const words = Array.isArray(doc.words) ? doc.words.slice(0, 10) : [];
  if (!category || words.length !== 10) return null;
  return { 
    id: doc.id ?? String(doc._id ?? ''), 
    category, 
    domain: category, 
    words 
  };
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

// ==================== MONGODB ====================

async function connectMongoDB() {
  if (!MONGODB_URI) {
    console.warn('[Mongo] MONGODB_URI غير موجود؛ سيتم استخدام questions.json.');
    return;
  }
  try {
    mongoClient = new MongoClient(MONGODB_URI, { 
      serverSelectionTimeoutMS: 10000,
      maxPoolSize: 10
    });
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
        console.log(`[Mongo] Seeded ${questions.length} questions.`);
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
    try {
      let candidates = await questionsCollection
        .find({})
        .project({ _id: 0, id: 1, category: 1, domain: 1, words: 1 })
        .toArray();
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
    } catch (err) {
      console.error('[Mongo] query failed:', err.message);
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

// ==================== API ROUTES ====================

app.get('/api/question/random', async (_req, res) => {
  try {
    const question = await getRandomQuestion();
    if (!question) {
      return res.status(404).json({ 
        success: false, 
        message: 'لا توجد مجالات صالحة.' 
      });
    }
    res.json(question);
  } catch (err) {
    console.error('[Question] error:', err);
    res.status(500).json({ 
      success: false, 
      message: 'تعذر جلب المجال.' 
    });
  }
});

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    server: 'online',
    connector: 'tiktok-live-connector 2.5.0',
    mongodb: Boolean(questionsCollection),
    tiktok: Boolean(tiktokConnection),
    activeTikTok: activeUniqueId || null
  });
});

// ==================== TIKTOK MANAGEMENT ====================

async function disconnectTikTok() {
  const old = tiktokConnection;
  tiktokConnection = null;
  activeUniqueId = null;
  if (old) {
    try { 
      await old.disconnect(); 
    } catch (_) {}
  }
}

function emitError(socket, message) {
  socket.emit('tiktok-error', { message });
  socket.emit('tiktok-status', { status: 'disconnected', message });
}

// ==================== SOCKET.IO ====================

io.on('connection', socket => {
  console.log('[Socket.IO] client connected:', socket.id);

  socket.on('connect-tiktok', async (data = {}) => {
    const uniqueId = cleanTikTokId(data.uniqueId);
    if (!uniqueId) {
      emitError(socket, 'يرجى إدخال اسم مستخدم TikTok صحيح.');
      return;
    }

    const generation = ++connectionGeneration;
    await disconnectTikTok();

    socket.emit('tiktok-connecting', { 
      uniqueId, 
      message: 'جاري محاولة الاتصال بالبث...' 
    });
    socket.emit('tiktok-status', { 
      status: 'connecting', 
      uniqueId, 
      message: 'جاري محاولة الاتصال بالبث...' 
    });

    let connection;
    let timeoutId = null;

    try {
      // TikTokLiveConnection for tiktok-live-connector v2.5.0
      connection = new TikTokLiveConnection(uniqueId, {
        processInitialData: false,
        enableExtendedGiftInfo: true,
        webClientOptions: {
          timeout: { request: 10000 }
        },
        wsClientOptions: {
          handshakeTimeout: 10000
        }
      });

      tiktokConnection = connection;
      activeUniqueId = uniqueId;
      let connected = false;

      // Event: Connected
      connection.on('connected', state => {
        if (generation !== connectionGeneration || tiktokConnection !== connection) return;
        connected = true;
        console.log(`[TikTok] connected @${uniqueId} room=${state?.roomId || 'unknown'}`);
        
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
      });

      // Event: Chat message (WebcastEvent.CHAT in v2.5.0)
      connection.on(WebcastEvent.CHAT, data => {
        if (generation !== connectionGeneration || tiktokConnection !== connection) return;
        
        io.emit('chat', {
          uniqueId: data?.user?.uniqueId || data?.uniqueId || '',
          nickname: data?.user?.nickname || data?.nickname || data?.user?.uniqueId || 'مستخدم',
          comment: data?.comment || '',
          profilePictureUrl: data?.user?.profilePictureUrl || data?.profilePictureUrl || ''
        });
      });

      // Event: Member join (WebcastEvent.MEMBER in v2.5.0)
      connection.on(WebcastEvent.MEMBER, data => {
        if (generation !== connectionGeneration || tiktokConnection !== connection) return;
        
        io.emit('tiktok-member-event', {
          uniqueId: data?.user?.uniqueId || data?.uniqueId || '',
          nickname: data?.user?.nickname || data?.nickname || data?.uniqueId || 'مستخدم',
          profilePictureUrl: data?.user?.profilePictureUrl || data?.profilePictureUrl || '',
          action: 'join'
        });
      });

      // Event: Stream ended
      connection.on(WebcastEvent.STREAM_END, () => {
        if (generation !== connectionGeneration) return;
        emitError(socket, 'انتهى البث المباشر.');
        if (tiktokConnection === connection) {
          tiktokConnection = null;
          activeUniqueId = null;
        }
      });

      // Event: Disconnected
      connection.on('disconnected', () => {
        if (generation !== connectionGeneration) return;
        if (!connected) {
          emitError(socket, 'تم قطع الاتصال قبل اكتمال الاتصال بالبث.');
        } else {
          socket.emit('tiktok-status', { 
            status: 'disconnected', 
            message: 'تم قطع الاتصال بالبث المباشر.' 
          });
        }
        if (tiktokConnection === connection) {
          tiktokConnection = null;
          activeUniqueId = null;
        }
      });

      // Event: Error
      connection.on('error', err => {
        if (generation !== connectionGeneration) return;
        const msg = errorMessage(err);
        console.error(`[TikTok] error @${uniqueId}:`, err?.message || err);
        emitError(socket, msg);
        if (tiktokConnection === connection) {
          tiktokConnection = null;
          activeUniqueId = null;
        }
      });

      // Timeout handler
      timeoutId = setTimeout(async () => {
        if (generation !== connectionGeneration || connected) return;
        console.error(`[TikTok] timeout @${uniqueId}`);
        emitError(socket, 'انتهت مهلة الاتصال بالبث بعد 20 ثانية. تحقق من أن البث مفتوح واسم المستخدم صحيح.');
        if (tiktokConnection === connection) {
          tiktokConnection = null;
          activeUniqueId = null;
          try { await connection.disconnect(); } catch (_) {}
        }
      }, 20000);

      // Connect
      const state = await connection.connect();
      
      clearTimeout(timeoutId);
      
      if (generation !== connectionGeneration || tiktokConnection !== connection) return;
      
      if (!connected) {
        connected = true;
        console.log(`[TikTok] connected @${uniqueId} room=${state?.roomId || 'unknown'}`);
        
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

    } catch (err) {
      if (timeoutId) clearTimeout(timeoutId);
      if (generation !== connectionGeneration) return;
      
      console.error(`[TikTok] connect failed @${uniqueId}:`, err);
      
      if (tiktokConnection === connection) {
        tiktokConnection = null;
        activeUniqueId = null;
      }
      emitError(socket, errorMessage(err));
    }
  });

  socket.on('disconnect-tiktok', async () => {
    ++connectionGeneration;
    await disconnectTikTok();
    socket.emit('tiktok-status', { 
      status: 'disconnected', 
      message: 'تم فصل الاتصال بالحساب.' 
    });
  });

  socket.on('disconnect', () => {
    console.log('[Socket.IO] disconnected:', socket.id);
  });
});

// ==================== STARTUP ====================

async function start() {
  // Start server immediately (don't block on MongoDB)
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ TOP10 server listening on port ${PORT}`);
  });

  // Connect to MongoDB in background
  try {
    await connectMongoDB();
  } catch (err) {
    console.error('[Startup] MongoDB connection failed:', err.message);
    console.log('[Startup] Continuing with local questions.json fallback...');
  }
}

start().catch(err => {
  console.error('❌ Fatal startup error:', err);
  process.exit(1);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('[Shutdown] SIGTERM received, cleaning up...');
  try { await disconnectTikTok(); } catch (_) {}
  try { 
    if (mongoClient) await mongoClient.close(); 
  } catch (_) {}
  server.close(() => {
    console.log('[Shutdown] Server closed.');
    process.exit(0);
  });
});

process.on('SIGINT', async () => {
  console.log('[Shutdown] SIGINT received, cleaning up...');
  process.emit('SIGTERM');
});
