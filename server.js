const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");
const { MongoClient } = require("mongodb");
const { TikTokLiveConnection, WebcastEvent, ControlEvent } = require("tiktok-live-connector");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;
const HOST = "0.0.0.0";
const MONGODB_URI = process.env.MONGODB_URI;
const MONGODB_DB = process.env.MONGODB_DB || "Top10";
const MONGODB_COLLECTION = process.env.MONGODB_COLLECTION || "questions";
const QUESTIONS_FILE = path.join(__dirname, "questions.json");

let mongoClient = null;
let questionsCollection = null;
let localQuestions = [];
const usedQuestionIds = new Set();
const activeTikTokConnections = new Map();

app.use(express.json());
app.use(express.static(__dirname));

function loadQuestionsFile() {
  try {
    const data = require(QUESTIONS_FILE);
    localQuestions = Array.isArray(data) ? data : [];
    console.log(`Loaded ${localQuestions.length} questions from questions.json`);
  } catch (error) {
    console.error("Failed to load questions.json:", error.message);
  }
}

function normalizeQuestion(q, index = 0) {
  const category = q?.category || q?.domain || "مجال";
  return {
    id: String(q?.id ?? index + 1),
    category,
    domain: category,
    words: Array.isArray(q?.words) ? q.words.slice(0, 10) : []
  };
}

async function connectMongoDB() {
  loadQuestionsFile();
  if (!MONGODB_URI) {
    console.warn("MONGODB_URI is not configured. Using questions.json only.");
    return;
  }

  try {
    mongoClient = new MongoClient(MONGODB_URI, {
      serverSelectionTimeoutMS: 10000,
      connectTimeoutMS: 10000,
      maxPoolSize: 10
    });

    await mongoClient.connect();
    const db = mongoClient.db(MONGODB_DB);
    questionsCollection = db.collection(MONGODB_COLLECTION);

    const count = await questionsCollection.countDocuments();
    if (count === 0 && localQuestions.length) {
      const docs = localQuestions
        .map((q, i) => normalizeQuestion(q, i))
        .filter(q => q.words.length >= 10);

      if (docs.length) {
        await questionsCollection.insertMany(
          docs.map(q => ({ ...q, seededAt: new Date() })),
          { ordered: false }
        );
        console.log(`Seeded ${docs.length} questions into MongoDB.`);
      }
    }

    console.log(`MongoDB connected: ${MONGODB_DB}.${MONGODB_COLLECTION}`);
  } catch (error) {
    console.error("MongoDB connection failed:", error.message);
    console.warn("Continuing with questions.json fallback.");
    questionsCollection = null;
  }
}

async function getRandomQuestion() {
  if (questionsCollection) {
    const total = await questionsCollection.countDocuments();
    if (total > 0) {
      if (usedQuestionIds.size >= total) usedQuestionIds.clear();

      for (let i = 0; i < 20; i++) {
        const rows = await questionsCollection.aggregate([{ $sample: { size: 1 } }]).toArray();
        if (!rows.length) break;
        const q = normalizeQuestion(rows[0]);
        if (!usedQuestionIds.has(q.id) && q.words.length >= 10) {
          usedQuestionIds.add(q.id);
          return q;
        }
      }
    }
  }

  const questions = localQuestions
    .map((q, i) => normalizeQuestion(q, i))
    .filter(q => q.words.length >= 10);

  if (!questions.length) throw new Error("لا توجد مجالات صالحة في questions.json");
  if (usedQuestionIds.size >= questions.length) usedQuestionIds.clear();

  const available = questions.filter(q => !usedQuestionIds.has(q.id));
  const q = available[Math.floor(Math.random() * available.length)] || questions[0];
  usedQuestionIds.add(q.id);
  return q;
}

app.get("/api/health", (req, res) => {
  res.json({ ok: true, mongodb: !!questionsCollection, questionsFile: localQuestions.length });
});

app.get("/api/question/random", async (req, res) => {
  try {
    res.json(await getRandomQuestion());
  } catch (error) {
    console.error("Question API error:", error.message);
    res.status(500).json({ error: "تعذر تحميل المجال" });
  }
});

function cleanupTikTokConnection(socketId) {
  const connection = activeTikTokConnections.get(socketId);
  if (connection) {
    try { connection.disconnect(); } catch (_) {}
    activeTikTokConnections.delete(socketId);
  }
}

io.on("connection", (socket) => {
  socket.on("connect-tiktok", async (payload) => {
    const raw = typeof payload === "string"
      ? payload
      : payload?.uniqueId || payload?.username || "";

    const username = String(raw).trim().replace(/^@/, "");
    if (!username) {
      socket.emit("tiktok-error", { message: "أدخل اسم مستخدم TikTok." });
      return;
    }

    cleanupTikTokConnection(socket.id);
    const connection = new TikTokLiveConnection(username);
    activeTikTokConnections.set(socket.id, connection);

    try {
      const state = await connection.connect();

      socket.emit("tiktok-connected", {
        uniqueId: username,
        roomId: state?.roomId || null
      });

      connection.on(WebcastEvent.CHAT, (data) => {
        socket.emit("chat", {
          uniqueId: data?.user?.uniqueId || data?.user?.unique_id || "",
          nickname: data?.user?.nickname || data?.user?.nickName || "",
          profilePictureUrl:
            data?.user?.profilePictureUrl ||
            data?.user?.profilePicture?.urls?.[0] || "",
          comment: data?.comment || ""
        });
      });

      connection.on(WebcastEvent.MEMBER, (data) => {
        socket.emit("tiktok-member-event", {
          uniqueId: data?.user?.uniqueId || "",
          nickname: data?.user?.nickname || "",
          profilePictureUrl:
            data?.user?.profilePictureUrl ||
            data?.user?.profilePicture?.urls?.[0] || ""
        });
      });

      connection.on(ControlEvent.DISCONNECTED, () => {
        socket.emit("tiktok-error", { message: "تم قطع اتصال TikTok LIVE." });
        activeTikTokConnections.delete(socket.id);
      });

      connection.on(ControlEvent.STREAM_END, () => {
        socket.emit("tiktok-error", { message: "انتهى البث المباشر." });
        activeTikTokConnections.delete(socket.id);
      });
    } catch (error) {
      console.error(`TikTok connection failed for @${username}:`, error);
      activeTikTokConnections.delete(socket.id);
      socket.emit("tiktok-error", {
        message: error?.message || "فشل الاتصال ببث TikTok."
      });
    }
  });

  socket.on("disconnect", () => cleanupTikTokConnection(socket.id));
});

async function shutdown(signal) {
  console.log(`${signal}: shutting down...`);
  for (const socketId of activeTikTokConnections.keys()) cleanupTikTokConnection(socketId);
  if (mongoClient) {
    try { await mongoClient.close(); } catch (_) {}
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

(async () => {
  await connectMongoDB();
  server.listen(PORT, HOST, () => console.log(`TOP10 server listening on ${HOST}:${PORT}`));
})();
