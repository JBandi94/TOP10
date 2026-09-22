const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { Server } = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');
const { MongoClient } = require('mongodb');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.json());
app.use(express.static(__dirname));

const PORT = process.env.PORT || 3000;
const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = process.env.MONGODB_DB || 'Top10';
const QUESTIONS_COLLECTION = process.env.MONGODB_COLLECTION || 'questions';

let mongoClient = null;
let questionsCollection = null;
let tiktokConnection = null;
let activeUniqueId = null;
const usedQuestionIds = new Set();

function cleanTikTokId(value) {
    return String(value || '').trim().replace(/^@+/, '');
}

function normalizeQuestion(doc) {
    if (!doc) return null;
    const category = doc.category || doc.domain;
    const words = Array.isArray(doc.words) ? doc.words.slice(0, 10) : [];
    if (!category || words.length !== 10) return null;
    return {
        id: doc.id ?? String(doc._id ?? ''),
        category,
        words
    };
}

function loadQuestionsFile() {
    const file = path.join(__dirname, 'questions.json');
    if (!fs.existsSync(file)) return [];

    try {
        const data = JSON.parse(fs.readFileSync(file, 'utf8'));
        return (Array.isArray(data) ? data : [])
            .map(normalizeQuestion)
            .filter(Boolean);
    } catch (err) {
        console.error('خطأ في قراءة questions.json:', err.message);
        return [];
    }
}

async function connectMongoDB() {
    if (!MONGODB_URI) {
        console.warn('MONGODB_URI غير موجود. سيتم استخدام questions.json محلياً.');
        return;
    }

    try {
        mongoClient = new MongoClient(MONGODB_URI, {
            serverSelectionTimeoutMS: 10000
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
                console.log(`تمت إضافة ${questions.length} مجالاً من questions.json إلى MongoDB.`);
            }
        }

        console.log(`MongoDB متصل بنجاح. عدد المجالات: ${await questionsCollection.countDocuments()}`);
    } catch (err) {
        console.error('فشل الاتصال بـ MongoDB:', err.message);
        mongoClient = null;
        questionsCollection = null;
    }
}

async function getRandomQuestion() {
    if (questionsCollection) {
        let candidates = await questionsCollection
            .find({})
            .project({ _id: 0, id: 1, category: 1, words: 1 })
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
    }

    const localQuestions = loadQuestionsFile();
    let available = localQuestions.filter(
        q => !usedQuestionIds.has(String(q.id))
    );

    if (!available.length) {
        usedQuestionIds.clear();
        available = localQuestions;
    }

    if (!available.length) return null;

    const selected = available[Math.floor(Math.random() * available.length)];
    usedQuestionIds.add(String(selected.id));
    return selected;
}

app.get('/api/question/random', async (req, res) => {
    try {
        const question = await getRandomQuestion();

        if (!question) {
            return res.status(404).json({
                success: false,
                message: 'لا توجد مجالات صالحة.'
            });
        }

        res.json({ success: true, question });
    } catch (err) {
        console.error('خطأ في اختيار المجال:', err);
        res.status(500).json({
            success: false,
            message: 'تعذر جلب المجال.'
        });
    }
});

app.get('/api/health', (req, res) => {
    res.json({
        success: true,
        server: 'online',
        mongodb: Boolean(questionsCollection),
        tiktok: Boolean(tiktokConnection),
        activeTikTok: activeUniqueId || null
    });
});

io.on('connection', (socket) => {
    console.log('عميل جديد متصل:', socket.id);

    socket.on('connect-tiktok', async (data = {}) => {
        const uniqueId = cleanTikTokId(data.uniqueId);

        if (!uniqueId) {
            return socket.emit('tiktok-error', {
                message: 'يرجى إدخال اسم حساب التيكتوك!'
            });
        }

        if (tiktokConnection) {
            try { tiktokConnection.disconnect(); } catch (_) {}
            tiktokConnection = null;
        }

        try {
            tiktokConnection = new WebcastPushConnection(uniqueId, {
                processInitialData: false,
                enableExtendedOption: true,
                requestOptions: { timeout: 10000 }
            });

            const state = await tiktokConnection.connect();
            activeUniqueId = uniqueId;

            // These event names match the supplied index.html.
            socket.emit('tiktok-connected', {
                success: true,
                message: `تم الاتصال بنجاح ببث: ${uniqueId}`,
                roomId: state.roomId
            });

            tiktokConnection.on('chat', (data) => {
                io.emit('chat', {
                    uniqueId: data.uniqueId,
                    nickname: data.nickname,
                    comment: data.comment,
                    profilePictureUrl: data.profilePictureUrl || ''
                });
            });

            tiktokConnection.on('member', (data) => {
                io.emit('tiktok-member-event', {
                    uniqueId: data.uniqueId,
                    nickname: data.nickname,
                    action: 'join',
                    profilePictureUrl: data.profilePictureUrl || ''
                });
            });

            tiktokConnection.on('streamEnd', () => {
                io.emit('tiktok-error', { message: 'انتهى البث المباشر!' });
                tiktokConnection = null;
                activeUniqueId = null;
            });

            tiktokConnection.on('disconnected', () => {
                io.emit('tiktok-error', {
                    message: 'تم قطع الاتصال بالبث المباشر.'
                });
            });

        } catch (err) {
            console.error('خطأ الاتصال بـ TikTok:', err);
            tiktokConnection = null;
            activeUniqueId = null;

            socket.emit('tiktok-error', {
                message: 'تعذر الاتصال بالبث! تأكد أن الحساب في بث مباشر الآن وأن اسم المستخدم صحيح.'
            });
        }
    });

    socket.on('disconnect-tiktok', () => {
        if (tiktokConnection) {
            try { tiktokConnection.disconnect(); } catch (_) {}
        }

        tiktokConnection = null;
        activeUniqueId = null;

        socket.emit('tiktok-error', {
            message: 'تم فصل الاتصال بالحساب.'
        });
    });

    socket.on('disconnect', () => {
        console.log('انقطع اتصال العميل:', socket.id);
    });
});

async function startServer() {
    await connectMongoDB();

    server.listen(PORT, '0.0.0.0', () => {
        console.log(`الخادم يعمل على المنفذ ${PORT}`);
    });
}

startServer().catch((err) => {
    console.error('خطأ قاتل أثناء تشغيل الخادم:', err);
    process.exit(1);
});

process.on('SIGTERM', async () => {
    try {
        if (tiktokConnection) tiktokConnection.disconnect();
    } catch (_) {}

    try {
        if (mongoClient) await mongoClient.close();
    } catch (_) {}

    process.exit(0);
});
