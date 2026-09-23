import express from "express";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Server } from "socket.io";
import { MongoClient } from "mongodb";
import {
  TikTokLiveConnection,
  WebcastEvent,
  ControlEvent
} from "tiktok-live-connector";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const httpServer = http.createServer(app);
const io = new Server(httpServer);

const PORT = process.env.PORT || 3000;
const HOST = "0.0.0.0";

const MONGODB_URI = process.env.MONGODB_URI;
const MONGODB_DB = process.env.MONGODB_DB || "Top10";
const MONGODB_COLLECTION = process.env.MONGODB_COLLECTION || "questions";

let mongoClient = null;
let questionsCollection = null;
let localQuestions = [];
const usedQuestionIds = new Set();
const activeConnections = new Map();

app.use(express.json());
app.use(express.static(__dirname));

async function loadQuestionsFile() {
  try {
    const response = await import("./questions.json", { with: { type: "json" } });
    localQuestions = Array.isArray(response.default) ? response.default : [];
    console.log(`Loaded ${localQuestions.length} questions from questions.json`);
  } catch (error) {
    console.error("questions.json load error:", error.message);
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
  await loadQuestionsFile();

  if (!MONGODB_URI) {
    console.warn("MONGODB_URI is not configured; using questions.json.");
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

    if (count === 0) {
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
    questionsCollection = null;
  }
}

async function getRandomQuestion() {
  if (questionsCollection) {
    const total = await questionsCollection.countDocuments();

    if (total > 0) {
      if (usedQuestionIds.size >= total) usedQuestionIds.clear();

      for (let i = 0; i < 30; i++) {
        const rows = await questionsCollection
          .aggregate([{ $sample: { size: 1 } }])
          .toArray();

        if (!rows.length) break;

        const q = normalizeQuestion(rows[0]);

        if (!usedQuestionIds.has(q.id) && q.words.length >= 10) {
          usedQuestionIds.add(q.id);
          return q;
        }
      }
    }
  }

  const fallback = localQuestions
    .map((q, i) => normalizeQuestion(q, i))
    .filter(q => q.words.length >= 10);

  if (!fallback.length) throw new Error("لا توجد مجالات صالحة.");

  if (usedQuestionIds.size >= fallback.length) usedQuestionIds.clear();

  const available = fallback.filter(q => !usedQuestionIds.has(q.id));
  const q = available[Math.floor(Math.random() * available.length)] || fallback[0];

  usedQuestionIds.add(q.id);
  return q;
}

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    mongodb: Boolean(questionsCollection),
    questionsFile: localQuestions.length
  });
});

app.get("/api/question/random", async (req, res) => {
  try {
    res.json(await getRandomQuestion());
  } catch (error) {
    console.error("Question API error:", error);
    res.status(500).json({ error: "تعذر تحميل المجال" });
  }
});

function cleanupConnection(socketId) {
  const connection = activeConnections.get(socketId);
  if (!connection) return;

  try {
    connection.disconnect();
  } catch (_) {}

  activeConnections.delete(socketId);
}

io.on("connection", socket => {
  console.log(`Browser connected: ${socket.id}`);

  socket.on("connect-tiktok", async payload => {
    const raw =
      typeof payload === "string"
        ? payload
        : payload?.uniqueId || payload?.username || "";

    const username = String(raw).trim().replace(/^@/, "");

    if (!username) {
      socket.emit("tiktok-error", { message: "أدخل اسم مستخدم TikTok." });
      return;
    }

    cleanupConnection(socket.id);

    console.log(`TikTok connection requested: @${username}`);

    const connection = new TikTokLiveConnection(username);
    activeConnections.set(socket.id, connection);

    // IMPORTANT: register control/error handlers BEFORE connect()
    // so connection failures cannot leave the UI stuck on "connecting".
    connection.on(ControlEvent.ERROR, event => {
      console.error(`TikTok error @${username}:`, event);
      socket.emit("tiktok-error", {
        message:
          event?.exception?.message ||
          event?.info ||
          "حدث خطأ أثناء الاتصال ببث TikTok."
      });
    });

    connection.on(ControlEvent.CONNECTED, state => {
      console.log(`TikTok CONNECTED @${username}`, state?.roomId);

      socket.emit("tiktok-connected", {
        uniqueId: username,
        roomId: state?.roomId || null
      });
    });

    connection.on(ControlEvent.DISCONNECTED, () => {
      console.log(`TikTok DISCONNECTED @${username}`);
      socket.emit("tiktok-error", {
        message: "تم قطع اتصال TikTok LIVE."
      });
      activeConnections.delete(socket.id);
    });

    connection.on(ControlEvent.STREAM_END, () => {
      console.log(`TikTok STREAM END @${username}`);
      socket.emit("tiktok-error", {
        message: "انتهى البث المباشر."
      });
      activeConnections.delete(socket.id);
    });

    connection.on(WebcastEvent.CHAT, data => {
      socket.emit("chat", {
        uniqueId: data?.user?.uniqueId || "",
        nickname: data?.user?.nickname || "",
        profilePictureUrl:
          data?.user?.profilePictureUrl ||
          data?.user?.profilePicture?.urls?.[0] ||
          "",
        comment: data?.comment || ""
      });
    });

    connection.on(WebcastEvent.MEMBER, data => {
      socket.emit("tiktok-member-event", {
        uniqueId: data?.user?.uniqueId || "",
        nickname: data?.user?.nickname || "",
        profilePictureUrl:
          data?.user?.profilePictureUrl ||
          data?.user?.profilePicture?.urls?.[0] ||
          ""
      });
    });

    try {
      // 2.5.0 resolves only when the WebSocket is actually connected.
      // The control event above provides the browser confirmation too.
      const state = await connection.connect();

      if (!state) {
        throw new Error("TikTok لم يرجع حالة اتصال صحيحة.");
      }

      console.log(`TikTok connect() resolved @${username}: ${state.roomId}`);
    } catch (error) {
      console.error(`TikTok connect() failed @${username}:`, error);

      activeConnections.delete(socket.id);

      socket.emit("tiktok-error", {
        message:
          error?.message ||
          error?.cause?.message ||
          "فشل الاتصال ببث TikTok. تحقق من اسم المستخدم وأن البث مباشر."
      });
    }
  });

  socket.on("disconnect", () => {
    cleanupConnection(socket.id);
  });
});

async function shutdown(signal) {
  console.log(`${signal}: shutting down`);

  for (const socketId of activeConnections.keys()) {
    cleanupConnection(socketId);
  }

  if (mongoClient) {
    try {
      await mongoClient.close();
    } catch (_) {}
  }

  httpServer.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

await connectMongoDB();

httpServer.listen(PORT, HOST, () => {
  console.log(`TOP10 server listening on ${HOST}:${PORT}`);
});
