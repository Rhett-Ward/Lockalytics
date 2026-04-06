/**
 * server.js
 * Express web server for Lockalytics.
 *
 * Routes:
 *   GET  /                      -> serves heroes.html
 *   GET  /api/health            -> uptime check
 *   GET  /api/heroes            -> hero stats (kept for backwards compat)
 *   GET  /api/collections       -> list all collections with row counts
 *   GET  /api/data/:collection  -> paginated query on any allowed collection
 *
 * Query params for /api/data/:collection:
 *   page      - page number (default 1)
 *   limit     - rows per page (default 50, max 200)
 *   sort      - field to sort by
 *   order     - "asc" or "desc" (default desc)
 *   hero_id   - filter by hero_id (for per-hero collections)
 *
 * Start: pm2 start server.js --name lockalytics-web
 */

require("dotenv").config();

const express         = require("express");
const path            = require("path");
const { MongoClient } = require("mongodb");

// ── Config ────────────────────────────────────────────────────────────────────
const PORT       = process.env.PORT            || 3000;
const MONGO_URI  = process.env.MONGO_URI;
const DB_NAME    = process.env.MONGO_DB_NAME   || "lockalytics";
const HERO_COLL  = process.env.MONGO_COLLECTION || "hero_stats";

if (!MONGO_URI) {
  console.error("ERROR: MONGO_URI is not set in .env");
  process.exit(1);
}

// Whitelist of collections the frontend is allowed to query.
// requiresHero: true means the collection has so many rows that loading it
// without a hero_id filter would be painfully slow. Anything under ~100k rows
// is fine to browse paginated, so requiresHero is false there.
const COLLECTIONS = [
  { name: "hero_stats",             category: "Heroes",       requiresHero: false },
  { name: "item_stats",             category: "Items",        requiresHero: false },
  { name: "hero_counter_stats",     category: "Matchups",     requiresHero: false },
  { name: "hero_synergy_stats",     category: "Matchups",     requiresHero: false },
  { name: "ability_order_stats",    category: "Builds",       requiresHero: true  },
  { name: "build_item_stats",       category: "Builds",       requiresHero: true  },
  { name: "search_builds",          category: "Builds",       requiresHero: false },
  { name: "bulk_metadata",          category: "Matches",      requiresHero: false },
  { name: "match_metadata",         category: "Matches",      requiresHero: false },
  { name: "match_history",          category: "Matches",      requiresHero: false },
  { name: "hero_scoreboard",        category: "Leaderboards", requiresHero: false },
  { name: "player_scoreboard",      category: "Leaderboards", requiresHero: false },
  { name: "badge_distribution",     category: "Analytics",    requiresHero: false },
  { name: "player_performance_curve",category:"Analytics",    requiresHero: false },
  { name: "player_stat_metrics",    category: "Analytics",    requiresHero: false },
];

const ALLOWED = new Set(COLLECTIONS.map(c => c.name));

// ── MongoDB ───────────────────────────────────────────────────────────────────
const mongoClient = new MongoClient(MONGO_URI);
let db;

async function connectMongo() {
  await mongoClient.connect();
  db = mongoClient.db(DB_NAME);
  console.log(`Connected to MongoDB: ${DB_NAME}`);
}

// ── Express ───────────────────────────────────────────────────────────────────
const app = express();
app.use(express.static(path.join(__dirname)));

// ── Routes ────────────────────────────────────────────────────────────────────

app.get("/api/health", (_req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

// Original hero stats route — kept so anything that depended on it still works
app.get("/api/heroes", async (req, res) => {
  try {
    const sortField = req.query.sort  || "win_rate";
    const sortOrder = req.query.order === "asc" ? 1 : -1;
    const limit     = Math.min(parseInt(req.query.limit) || 200, 500);

    const heroes = await db.collection(HERO_COLL)
      .find({})
      .sort({ [sortField]: sortOrder })
      .limit(limit)
      .toArray();

    res.json({
      ok:           true,
      count:        heroes.length,
      last_updated: heroes[0]?.last_updated ?? null,
      heroes,
    });
  } catch (err) {
    console.error("MongoDB query error:", err.message);
    res.status(500).json({ ok: false, error: "Database error" });
  }
});

// Returns the collection list with live row counts.
// The frontend uses this to populate the nav and show collection sizes.
app.get("/api/collections", async (_req, res) => {
  try {
    const results = await Promise.all(
      COLLECTIONS.map(async (meta) => {
        try {
          const count = await db.collection(meta.name).estimatedDocumentCount();
          return { ...meta, count };
        } catch {
          return { ...meta, count: 0 };
        }
      })
    );
    res.json({ ok: true, collections: results });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Generic paginated query on any allowed collection.
app.get("/api/data/:collection", async (req, res) => {
  const { collection } = req.params;

  if (!ALLOWED.has(collection)) {
    return res.status(400).json({ ok: false, error: "Unknown collection" });
  }

  const page      = Math.max(1, parseInt(req.query.page)  || 1);
  const limit     = Math.min(Math.max(1, parseInt(req.query.limit) || 50), 200);
  const skip      = (page - 1) * limit;
  const sortField = req.query.sort  || null;
  const sortOrder = req.query.order === "asc" ? 1 : -1;
  const heroId    = req.query.hero_id !== undefined ? parseInt(req.query.hero_id) : null;

  const query = {};
  if (heroId !== null && !isNaN(heroId)) query.hero_id = heroId;

  try {
    const coll  = db.collection(collection);
    const total = await coll.countDocuments(query);

    let cursor = coll.find(query, { projection: { _id: 0 } });
    if (sortField) cursor = cursor.sort({ [sortField]: sortOrder });

    const data = await cursor.skip(skip).limit(limit).toArray();

    res.json({
      ok: true,
      collection,
      total,
      page,
      pages:  Math.ceil(total / limit),
      limit,
      count:  data.length,
      data,
    });
  } catch (err) {
    console.error(`Query error on ${collection}:`, err.message);
    res.status(500).json({ ok: false, error: "Database error" });
  }
});

// Catch-all — SPA fallback
app.get("*", (_req, res) => {
  res.sendFile(path.join(__dirname, "heroes.html"));
});

// ── Boot ──────────────────────────────────────────────────────────────────────
connectMongo()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Lockalytics server running on http://localhost:${PORT}`);
    });
  })
  .catch((err) => {
    console.error("Failed to connect to MongoDB:", err.message);
    process.exit(1);
  });
