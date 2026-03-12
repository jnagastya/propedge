// =================================================================
// PROPEDGE v3 — BACKEND SERVER
// =================================================================
// Express server that:
//   1. Proxies The Odds API (player props, scores)
//   2. Proxies BallDontLie / NBA stats
//   3. Caches all responses (node-cache) to conserve rate limits
//   4. Computes analytics: edge, confidence, splits, DvP, with/without
//   5. Serves the static frontend
// =================================================================

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');
const NodeCache = require('node-cache');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ---- CACHE SETUP ----
const cache = new NodeCache({
  stdTTL: parseInt(process.env.CACHE_TTL_ODDS) || 120,
  checkperiod: 30,
});

let ODDS_KEY = process.env.ODDS_API_KEY || '';
let BDL_KEY = process.env.BDL_API_KEY || '';
const ODDS_BASE = 'https://api.the-odds-api.com/v4';
const BDL_BASE = 'https://api.balldontlie.io/nba/v1';
const NBA_BASE = 'https://stats.nba.com/stats';
const STATS_TTL = 3 * 60 * 60; // 3-hour cache for stats (games happen daily)

// ---- SUPABASE SETUP ----
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const supabase = SUPABASE_URL && SUPABASE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_KEY)
  : null;

// Read game log from Supabase cache (returns null if not found or stale)
async function sbGetGameLog(playerName, maxAgeHours = 24) {
  if (!supabase) return null;
  try {
    const { data, error } = await supabase
      .from('player_stats')
      .select('game_log, last_fetched, season')
      .eq('player_name', playerName)
      .single();
    if (error || !data) return null;
    // Reject if from a different season (e.g. last year's data still in Supabase)
    if (data.season !== NBA_SEASON) return null;
    // Treat as stale if older than maxAgeHours
    const age = Date.now() - new Date(data.last_fetched).getTime();
    if (age > maxAgeHours * 60 * 60 * 1000) return null;
    return data.game_log;
  } catch { return null; }
}

// Write game log to Supabase cache (upsert by player_name)
async function sbSetGameLog(playerName, bdlId, gameLog, position) {
  if (!supabase || !gameLog?.length) return;
  try {
    await supabase.from('player_stats').upsert({
      player_name: playerName,
      bdl_id: bdlId,
      game_log: gameLog,
      position: position || null,  // BDL-sourced position: 'G', 'F', or 'C'
      season: NBA_SEASON,
      last_fetched: new Date().toISOString(),
    }, { onConflict: 'player_name' });
  } catch (e) { console.warn('Supabase write failed:', e.message); }
}

// Read odds from Supabase cache (returns null if not found)
async function sbGetOdds(book) {
  if (!supabase) return null;
  try {
    const { data, error } = await supabase
      .from('odds_cache')
      .select('players, last_fetched')
      .eq('book', book)
      .single();
    if (error || !data) return null;
    return data.players;
  } catch { return null; }
}

// Write odds to Supabase cache (upsert by book)
async function sbSetOdds(book, players) {
  if (!supabase || !players?.length) return;
  try {
    await supabase.from('odds_cache').upsert({
      book,
      players,
      last_fetched: new Date().toISOString(),
    }, { onConflict: 'book' });
  } catch (e) { console.warn('Supabase odds write failed:', e.message); }
}

// ---- BDL TEAM ID → ABBREVIATION MAP ----
// BDL uses fixed numeric IDs for all 30 teams

// ---- NBA SEASON HELPER ----
function currentNBASeason() {
  const now = new Date();
  const month = now.getMonth() + 1;
  const year = now.getFullYear();
  // NBA season starts in October; season=year means Oct year – Jun year+1
  return month >= 10 ? year : year - 1;
}
const NBA_SEASON = currentNBASeason(); // e.g. 2025 for 2025-26 season

// ---- HELPERS ----
function cacheGet(key) { return cache.get(key); }
function cacheSet(key, data, ttl) { cache.set(key, data, ttl); }

function teamAbbr(fullName) {
  if (!fullName) return '???';
  const map = {
    'hawks':'ATL','celtics':'BOS','nets':'BKN','hornets':'CHA','bulls':'CHI',
    'cavaliers':'CLE','mavericks':'DAL','nuggets':'DEN','pistons':'DET','warriors':'GSW',
    'rockets':'HOU','pacers':'IND','clippers':'LAC','lakers':'LAL','grizzlies':'MEM',
    'heat':'MIA','bucks':'MIL','timberwolves':'MIN','pelicans':'NOP','knicks':'NYK',
    'thunder':'OKC','magic':'ORL','76ers':'PHI','suns':'PHX','trail blazers':'POR',
    'blazers':'POR','kings':'SAC','spurs':'SAS','raptors':'TOR','jazz':'UTA','wizards':'WAS',
  };
  const parts = fullName.toLowerCase().split(' ');
  for (let i = parts.length - 1; i >= 0; i--) {
    if (map[parts[i]]) return map[parts[i]];
    // Handle "Trail Blazers"
    if (i > 0 && map[parts[i-1] + ' ' + parts[i]]) return map[parts[i-1] + ' ' + parts[i]];
  }
  return fullName.substring(0, 3).toUpperCase();
}

// ---- BDL HELPERS ----
function bdlHeaders() {
  return { 'Authorization': BDL_KEY };
}

// Fetch one season's game log rows from BDL (cursor-paginated)
// Fetch full season game log from BDL (cursor-paginated, retries on 429)
async function fetchBDLGameLog(playerId) {
  let allRows = [];
  let cursor = null;
  do {
    let url = `${BDL_BASE}/stats?player_ids[]=${playerId}&seasons[]=${NBA_SEASON}&per_page=100`;
    if (cursor) url += `&cursor=${cursor}`;
    let resp;
    // Retry up to 3 times on rate limit with exponential backoff
    for (let attempt = 0; attempt < 3; attempt++) {
      resp = await fetch(url, { headers: bdlHeaders() });
      if (resp.status !== 429) break;
      const wait = (attempt + 1) * 2000; // 2s, 4s, 6s
      console.warn(`BDL 429 for player ${playerId}, retrying in ${wait}ms (attempt ${attempt + 1})`);
      await new Promise(r => setTimeout(r, wait));
    }
    if (!resp.ok) throw new Error(`BDL ${resp.status}`);
    const data = await resp.json();
    allRows = allRows.concat(data.data || []);
    cursor = data.meta?.next_cursor || null;
  } while (cursor);

  return allRows.map(g => {
    if (!g.game) return null;
    // MATCHUP: determine home/away from team abbreviation in game context
    // BDL game object: { date, home_team_id, visitor_team_id }
    const home = g.game.home_team_id === g.team?.id;
    const opp = home ? g.game.visitor_team_id : g.game.home_team_id;
    return {
      date: (g.game.date || '').split('T')[0],
      pts: +g.pts || 0, reb: +g.reb || 0, ast: +g.ast || 0,
      fg3m: +g.fg3m || 0, stl: +g.stl || 0, blk: +g.blk || 0,
      turnover: +g.turnover || 0, min: String(g.min || '0'),
      home, wl: '', opp_team_id: opp || null,
    };
  }).filter(g => g && g.date && parseInt(g.min || '0') > 0); // exclude DNP/inactive games
}

// Strip common name suffixes so "Jaren Jackson Jr." matches "Jaren Jackson"
function normalizeName(name) {
  return name
    .replace(/\s+(jr\.?|sr\.?|ii|iii|iv|v)$/i, '')
    .replace(/['']/g, "'")  // normalize apostrophes (De'Aaron vs De'Aaron)
    .trim()
    .toLowerCase();
}

// Score how well a BDL player object matches a target name (higher = better)
function nameMatchScore(bdlPlayer, target) {
  const full = `${bdlPlayer.first_name} ${bdlPlayer.last_name}`;
  const normFull = normalizeName(full);
  const normTarget = normalizeName(target);
  if (normFull === normTarget) return 100;                    // exact normalized match
  if (normFull.includes(normTarget) || normTarget.includes(normFull)) return 80; // substring
  // Check if last name matches and first initial matches
  const tParts = normTarget.split(' ');
  const bLast = bdlPlayer.last_name.toLowerCase();
  const bFirst = bdlPlayer.first_name.toLowerCase();
  if (bLast === tParts[tParts.length - 1] && bFirst[0] === tParts[0][0]) return 60;
  if (bLast === tParts[tParts.length - 1]) return 40;        // last name only
  return 0;
}

// Normalize BDL position string to broad category used for DvP
function normalizeBDLPosition(pos) {
  if (!pos) return 'F';
  const p = pos.toUpperCase();
  if (p.startsWith('G')) return 'G';   // G, G-F, PG, SG
  if (p === 'C' || p === 'F-C' || p === 'C-F') return 'C';
  return 'F';                           // F, SF, PF, F-G, F-C (non-center)
}

// Search BDL for player ID + position by name, cached 24h
// Returns { id, position } or { id: null, position: null }
async function getBDLPlayerId(name) {
  const ck = `bdl_pid_${name.toLowerCase().replace(/\s+/g, '_')}`;
  const cached = cacheGet(ck);
  if (cached !== undefined) return cached;

  const trySearch = async (query) => {
    try {
      let resp;
      for (let attempt = 0; attempt < 3; attempt++) {
        resp = await fetch(`${BDL_BASE}/players?search=${encodeURIComponent(query)}&per_page=10`, { headers: bdlHeaders() });
        if (resp.status !== 429) break;
        const wait = (attempt + 1) * 2000;
        console.warn(`BDL 429 on player search "${query}", retrying in ${wait}ms`);
        await new Promise(r => setTimeout(r, wait));
      }
      if (!resp.ok) return [];
      const data = await resp.json();
      return data.data || [];
    } catch { return []; }
  };

  // Search 1: full name
  let results = await trySearch(name);

  // Helper: score a result set and update running best/score
  const updateBest = (candidates, curBest, curScore) => {
    let best = curBest, bestScore = curScore;
    for (const p of candidates) {
      const score = nameMatchScore(p, name);
      if (score > bestScore) {
        bestScore = score; best = p;
      } else if (score === bestScore && best) {
        const pActive = !!p.team, bestActive = !!best.team;
        if (pActive && !bestActive) { best = p; }
        else if (pActive === bestActive && p.id > best.id) { best = p; }
      }
    }
    return { best, bestScore };
  };

  let best = null, bestScore = 0;
  ({ best, bestScore } = updateBest(results, best, bestScore));

  // Search 2: normalized name (strip suffix Jr/III/etc) — run if no good match yet
  const normed = normalizeName(name);
  const normedTitle = normed.replace(/\b\w/g, c => c.toUpperCase());
  if (bestScore < 40 && normedTitle !== name) {
    ({ best, bestScore } = updateBest(await trySearch(normedTitle), best, bestScore));
  }

  // Search 3: expand common abbreviations (CJ → C.J., PJ → P.J., AJ → A.J.)
  if (bestScore < 40) {
    const expanded = name.replace(/\b([A-Z]{2,3})\b/g, m => m.split('').join('.') + '.');
    if (expanded !== name) ({ best, bestScore } = updateBest(await trySearch(expanded), best, bestScore));
  }

  // Search 4: first name only (helps with apostrophe variants like Ja'Kobe)
  if (bestScore < 40) {
    const firstName = name.trim().split(' ')[0].replace(/['\u2018\u2019]/g, '');
    ({ best, bestScore } = updateBest(await trySearch(firstName), best, bestScore));
  }

  // Search 5: last name fallback
  if (bestScore < 40) {
    const lastName = name.trim().split(' ').pop();
    ({ best, bestScore } = updateBest(await trySearch(lastName), best, bestScore));
  }

  const resolved = (best && bestScore >= 40) ? best : null;
  const id = resolved?.id ?? null;
  const position = resolved ? normalizeBDLPosition(resolved.position) : null;

  if (resolved) console.log(`BDL name match: "${name}" → "${resolved.first_name} ${resolved.last_name}" pos=${resolved.position} (score=${bestScore})`);
  else console.warn(`BDL name lookup failed: "${name}" (best score=${bestScore})`);

  const result = { id, position };
  // Cache successful lookups for 24h; failures only 30min so they auto-retry
  cacheSet(ck, result, id ? 24 * 3600 : 1800);
  return result;
}


// ============================================================
// ROUTE: GET /api/status — health check + API key validation
// ============================================================
app.get('/api/status', (req, res) => {
  res.json({
    server: 'ok',
    envVars: {
      ODDS_API_KEY: !!process.env.ODDS_API_KEY,
      BDL_API_KEY: !!process.env.BDL_API_KEY,
      SUPABASE_URL: !!process.env.SUPABASE_URL,
      SUPABASE_SERVICE_KEY: !!process.env.SUPABASE_SERVICE_KEY,
      CRON_SECRET: !!process.env.CRON_SECRET,
    },
    supabase: supabase ? 'configured' : 'missing',
    cacheKeys: cache.keys().length,
    uptime: process.uptime(),
  });
});

// ============================================================
// ROUTE: GET /api/health — alias for /api/status (frontend compat)
// ============================================================
app.get('/api/health', (req, res) => {
  res.json({
    server: 'ok',
    apis: {
      odds: ODDS_KEY ? 'configured' : 'missing',
      stats: BDL_KEY ? 'BDL configured' : 'BDL key missing',
    },
    season: NBA_SEASON,
    uptime: process.uptime(),
  });
});

// ============================================================
// ROUTE: POST /api/config — accept API keys from frontend
// ============================================================
app.post('/api/config', (req, res) => {
  const { oddsKey } = req.body || {};
  if (oddsKey) ODDS_KEY = oddsKey.trim();
  cache.flushAll(); // clear cache so fresh data loads with new keys
  res.json({
    odds: ODDS_KEY ? 'configured' : 'missing',
    stats: 'stats.nba.com (keyless)',
  });
});

// ============================================================
// HELPER: Parse raw props array into flat player list
// Supports single-book selection OR 'combined' (all books avg)
// ============================================================
function aggregatePlayers(rawProps, book) {
  const players = [];
  rawProps.forEach(evt => {
    if (!evt.bookmakers || !evt.bookmakers.length) return;
    const meta = {
      matchup: `${teamAbbr(evt.away_team)} @ ${teamAbbr(evt.home_team)}`,
      homeTeam: teamAbbr(evt.home_team),
      awayTeam: teamAbbr(evt.away_team),
      gameTime: evt.commence_time,
    };

    if (book === 'combined') {
      // Aggregate regulated US sportsbooks only — exclude offshore/unlicensed books
      const EXCLUDED_BOOKS = new Set(['bovada', 'betonlineag', 'mybookieag', 'lowvig']);
      const playerMap = {}; // key = playerName|market
      evt.bookmakers.filter(bk => !EXCLUDED_BOOKS.has(bk.key)).forEach(bk => {
        bk.markets?.forEach(mkt => {
          const byPlayer = {};
          mkt.outcomes?.forEach(o => {
            const pName = o.description || o.name;
            if (!byPlayer[pName]) byPlayer[pName] = {};
            byPlayer[pName][o.name] = { price: o.price, point: o.point };
          });
          Object.entries(byPlayer).forEach(([playerName, sides]) => {
            const over = sides['Over'];
            if (!over) return;
            const mapKey = `${playerName}|${mkt.key}`;
            if (!playerMap[mapKey]) {
              playerMap[mapKey] = { name: playerName, market: mkt.key, ...meta, bookmaker: 'Combined', bookLines: {} };
            }
            playerMap[mapKey].bookLines[bk.key] = {
              bookName: bk.title,
              line: over.point,
              overOdds: over.price,
              underOdds: sides['Under']?.price ?? null,
            };
          });
        });
      });
      Object.values(playerMap).forEach(p => {
        const lines = Object.values(p.bookLines);
        if (!lines.length) return;
        const avgLine = +(lines.reduce((s, b) => s + b.line, 0) / lines.length).toFixed(1);
        const bestOver = Math.max(...lines.map(b => b.overOdds));
        const underLines = lines.filter(b => b.underOdds != null);
        const bestUnder = underLines.length ? Math.max(...underLines.map(b => b.underOdds)) : null;
        players.push({ ...p, line: avgLine, overOdds: bestOver, underOdds: bestUnder });
      });
    } else {
      // Single-book mode
      const bk = evt.bookmakers.find(b => b.key === book) || evt.bookmakers[0];
      if (!bk) return;
      bk.markets?.forEach(mkt => {
        const byPlayer = {};
        mkt.outcomes?.forEach(o => {
          const pName = o.description || o.name;
          if (!byPlayer[pName]) byPlayer[pName] = {};
          byPlayer[pName][o.name] = { price: o.price, point: o.point };
        });
        Object.entries(byPlayer).forEach(([playerName, sides]) => {
          const over = sides['Over'];
          if (!over) return;
          players.push({ name: playerName, line: over.point, overOdds: over.price, underOdds: sides['Under']?.price ?? null, market: mkt.key, ...meta, bookmaker: bk.title });
        });
      });
    }
  });
  return players;
}

// ============================================================
// ROUTE: GET /api/odds/events — today's NBA games
// ============================================================
app.get('/api/odds/events', async (req, res) => {
  try {
    if (!ODDS_KEY) return res.status(400).json({ error: 'ODDS_API_KEY not configured' });

    const ck = 'odds_events';
    const cached = cacheGet(ck);
    if (cached) return res.json({ data: cached, cached: true });

    const url = `${ODDS_BASE}/sports/basketball_nba/events?apiKey=${ODDS_KEY}&regions=us&oddsFormat=american`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`Odds API returned ${resp.status}: ${await resp.text()}`);

    const data = await resp.json();
    const remaining = resp.headers.get('x-requests-remaining');

    cacheSet(ck, data, parseInt(process.env.CACHE_TTL_ODDS) || 120);
    res.json({ data, cached: false, requestsRemaining: remaining });
  } catch (err) {
    console.error('Events error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/odds/props/:eventId — player props for a game
// ============================================================
app.get('/api/odds/props/:eventId', async (req, res) => {
  try {
    if (!ODDS_KEY) return res.status(400).json({ error: 'ODDS_API_KEY not configured' });

    const { eventId } = req.params;
    const market = req.query.market || 'player_points';
    const ck = `props_${eventId}_${market}`;
    const cached = cacheGet(ck);
    if (cached) return res.json({ data: cached, cached: true });

    const url = `${ODDS_BASE}/sports/basketball_nba/events/${eventId}/odds?apiKey=${ODDS_KEY}&regions=us&markets=${market}&oddsFormat=american`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`Props API returned ${resp.status}`);

    const data = await resp.json();
    const remaining = resp.headers.get('x-requests-remaining');

    cacheSet(ck, data, parseInt(process.env.CACHE_TTL_ODDS) || 120);
    res.json({ data, cached: false, requestsRemaining: remaining });
  } catch (err) {
    console.error('Props error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/odds/props-all — all props across all games
// ============================================================
app.get('/api/odds/props-all', async (req, res) => {
  try {
    if (!ODDS_KEY) return res.status(400).json({ error: 'ODDS_API_KEY not configured' });

    const market = req.query.market || 'player_points';
    const book = req.query.book || 'draftkings';
    const ck = `propsall_${market}_${book}`;
    const cached = cacheGet(ck);
    if (cached) return res.json({ data: cached, cached: true });

    // 1. Get events
    const evtUrl = `${ODDS_BASE}/sports/basketball_nba/events?apiKey=${ODDS_KEY}&regions=us&oddsFormat=american`;
    const evtResp = await fetch(evtUrl);
    if (!evtResp.ok) throw new Error(`Events: ${evtResp.status}`);
    const events = await evtResp.json();

    // 2. Fetch props for each event (limit to 8 to conserve calls)
    const propPromises = events.slice(0, 8).map(async (evt) => {
      try {
        const pUrl = `${ODDS_BASE}/sports/basketball_nba/events/${evt.id}/odds?apiKey=${ODDS_KEY}&regions=us&markets=${market}&oddsFormat=american`;
        const pResp = await fetch(pUrl);
        if (!pResp.ok) return null;
        return await pResp.json();
      } catch { return null; }
    });

    const rawProps = (await Promise.all(propPromises)).filter(Boolean);

    // 3. Parse into flat player array
    const players = aggregatePlayers(rawProps, book);

    cacheSet(ck, players, parseInt(process.env.CACHE_TTL_ODDS) || 120);
    res.json({ data: players, cached: false, gamesScanned: events.length });
  } catch (err) {
    console.error('Props-all error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/odds/scores — live + recent scores
// ============================================================
app.get('/api/odds/scores', async (req, res) => {
  try {
    if (!ODDS_KEY) return res.status(400).json({ error: 'ODDS_API_KEY not configured' });

    const ck = 'scores';
    const cached = cacheGet(ck);
    if (cached) return res.json({ data: cached, cached: true });

    const url = `${ODDS_BASE}/sports/basketball_nba/scores?apiKey=${ODDS_KEY}&daysFrom=1`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`Scores: ${resp.status}`);
    const raw = await resp.json();

    const scores = raw.map(g => ({
      away: teamAbbr(g.away_team),
      home: teamAbbr(g.home_team),
      awayScore: g.scores?.find(s => s.name === g.away_team)?.score || null,
      homeScore: g.scores?.find(s => s.name === g.home_team)?.score || null,
      completed: g.completed || false,
      live: !g.completed && !!g.scores,
      commence: g.commence_time,
    }));

    cacheSet(ck, scores, parseInt(process.env.CACHE_TTL_SCORES) || 30);
    res.json({ data: scores, cached: false });
  } catch (err) {
    console.error('Scores error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/stats/player/search — search players via NBA.com
// NOTE: Specific string routes MUST come before param routes (:id)
// ============================================================
app.get('/api/stats/player/search', async (req, res) => {
  const { name } = req.query;
  if (!name) return res.status(400).json({ error: 'name required' });
  try {
    const { id } = await getBDLPlayerId(name);
    res.json({ data: id ? [{ id, name }] : [], cached: false });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/stats/player/name/:name/games — game log by player name
// IMPORTANT: Must be before /api/stats/player/:id/games
// ============================================================
app.get('/api/stats/player/name/:name/games', async (req, res) => {
  const name = decodeURIComponent(req.params.name);
  try {
    // Layer 1: in-memory cache (fastest)
    const ckMem = `gl_name_${NBA_SEASON}_${name.toLowerCase().replace(/\s+/g,'_')}`;
    const memHit = cacheGet(ckMem);
    if (memHit) return res.json({ data: memHit, cached: true, source: 'memory' });

    // Layer 2: Supabase persistent cache
    const sbHit = await sbGetGameLog(name);
    if (sbHit) {
      cacheSet(ckMem, sbHit, STATS_TTL);
      return res.json({ data: sbHit, cached: true, source: 'supabase' });
    }

    // Layer 3: Live BDL fetch
    const { id: nbaId, position: bdlPos } = await getBDLPlayerId(name);
    if (!nbaId) return res.json({ data: [], cached: false, error: `Player not found: ${name}` });
    const games = await fetchBDLGameLog(nbaId);
    // Only cache non-empty results; empty arrays should be retried on next request
    if (games.length) cacheSet(ckMem, games, STATS_TTL);
    await sbSetGameLog(name, nbaId, games, bdlPos); // persist for future requests
    res.json({ data: games, cached: false, source: 'bdl' });
  } catch (err) {
    console.error('Game log by name error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/stats/player/name/:name/splits — splits by player name
// IMPORTANT: Must be before /api/stats/player/:id/splits
// ============================================================
app.get('/api/stats/player/name/:name/splits', async (req, res) => {
  const name = decodeURIComponent(req.params.name);
  const line = parseFloat(req.query.line) || 0;
  const stat = req.query.stat || 'pts';
  try {
    const ckMem = `gl_name_${NBA_SEASON}_${name.toLowerCase().replace(/\s+/g,'_')}`;
    let games = cacheGet(ckMem);
    if (!games) {
      games = await sbGetGameLog(name);
      if (!games) {
        const { id: nbaId, position: bdlPos } = await getBDLPlayerId(name);
        if (!nbaId) return res.json({ data: null, cached: false, error: `Player not found: ${name}` });
        games = await fetchBDLGameLog(nbaId);
        await sbSetGameLog(name, nbaId, games, bdlPos);
      }
      if (games.length) cacheSet(ckMem, games, STATS_TTL);
    }
    res.json({ data: computeSplits(games, line, stat), cached: false, gamesAnalyzed: games.length });
  } catch (err) {
    console.error('Splits by name error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/stats/player/:id/games — game log by NBA player ID
// ============================================================
app.get('/api/stats/player/:id/games', async (req, res) => {
  const id = req.params.id;
  const ck = `bdl_gl_${id}_${NBA_SEASON}`;
  const cached = cacheGet(ck);
  if (cached) return res.json({ data: cached, cached: true });
  try {
    const games = await fetchBDLGameLog(id);
    cacheSet(ck, games, STATS_TTL);
    res.json({ data: games, cached: false });
  } catch (err) {
    console.error('Game log error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/stats/player/:id/splits — computed splits by NBA player ID
// ============================================================
app.get('/api/stats/player/:id/splits', async (req, res) => {
  const id = req.params.id;
  const line = parseFloat(req.query.line) || 0;
  const stat = req.query.stat || 'pts';
  const ck = `bdl_gl_${id}_${NBA_SEASON}`;
  let games = cacheGet(ck);
  if (!games) {
    try {
      games = await fetchNBAGameLog(id);
      cacheSet(ck, games, STATS_TTL);
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }
  res.json({ data: computeSplits(games, line, stat), cached: false, gamesAnalyzed: games.length });
});

function computeSplits(games, line, statKey) {
  if (!games || !games.length) return null;

  const getVal = (g) => {
    const map = { pts: g.pts, reb: g.reb, ast: g.ast, stl: g.stl, blk: g.blk,
      fg3m: g.fg3m, pra: (g.pts||0)+(g.reb||0)+(g.ast||0), turnover: g.turnover };
    return map[statKey] ?? g.pts ?? 0;
  };

  const calc = (subset) => {
    if (!subset.length) return { avg: 0, gp: 0, hitRate: 0, values: [] };
    const vals = subset.map(getVal);
    const avg = +(vals.reduce((a, b) => a + b, 0) / vals.length).toFixed(1);
    const hr = line ? Math.round(vals.filter(v => v >= line).length / vals.length * 100) : 0;
    return { avg, gp: vals.length, hitRate: hr, values: vals };
  };

  const sorted = [...games].filter(g => parseInt(g.min || '0') > 0).sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
  const home = sorted.filter(g => g.home === true);
  const away = sorted.filter(g => g.home === false);

  return {
    all: calc(sorted), home: calc(home), away: calc(away),
    last5: calc(sorted.slice(0, 5)), last10: calc(sorted.slice(0, 10)), last20: calc(sorted.slice(0, 20)),
    b2b: { avg: 0, gp: 0, hitRate: 0, note: 'Requires schedule data' },
    rest3plus: { avg: 0, gp: 0, hitRate: 0, note: 'Requires schedule data' },
  };
}

// ============================================================
// ============================================================
// ROUTE: GET /api/analytics/merged — THE BIG ONE
// Full merged dataset: odds + stats + analytics
// All markets fetched in a single per-game request to minimise Odds API quota usage
const ALL_PROP_MARKETS = 'player_points,player_rebounds,player_assists,player_threes,player_points_rebounds_assists';

// ============================================================
app.get('/api/analytics/merged', async (req, res) => {
  try {
    const market = req.query.market || 'player_points';
    const book = req.query.book || 'combined';

    // Fast path: per-market enriched cache
    const ck = `merged_${market}_${book}`;
    const cached = cacheGet(ck);
    if (cached) return res.json({ data: cached, cached: true });

    // Step 1: Get raw props for all markets at once (shared across market switches)
    const allCk = `allMarkets_${book}`;
    let allPlayers = cacheGet(allCk);

    if (!allPlayers) {
      // Layer 1: Supabase odds cache (populated by cron — zero Odds API calls for users)
      allPlayers = await sbGetOdds(book);
      if (allPlayers) {
        cacheSet(allCk, allPlayers, parseInt(process.env.CACHE_TTL_ODDS) || 1800);
      }
    }

    if (!allPlayers && ODDS_KEY) {
      // Layer 2: Live Odds API fallback (only if Supabase has no data yet)
      const now = new Date();
      allPlayers = [];
      const evtResp = await fetch(`${ODDS_BASE}/sports/basketball_nba/events?apiKey=${ODDS_KEY}&regions=us&oddsFormat=american`);
      if (evtResp.ok) {
        const events = await evtResp.json();
        const upcomingEvents = events.filter(evt => new Date(evt.commence_time) > now);

        const propPromises = upcomingEvents.map(async (evt) => {
          try {
            const pUrl = `${ODDS_BASE}/sports/basketball_nba/events/${evt.id}/odds?apiKey=${ODDS_KEY}&regions=us&markets=${ALL_PROP_MARKETS}&oddsFormat=american`;
            const pResp = await fetch(pUrl);
            return pResp.ok ? await pResp.json() : null;
          } catch { return null; }
        });

        const rawProps = (await Promise.all(propPromises)).filter(Boolean);
        allPlayers = aggregatePlayers(rawProps, book);
        cacheSet(allCk, allPlayers, parseInt(process.env.CACHE_TTL_ODDS) || 1800);
      }
    }

    // Filter to requested market
    const players = (allPlayers || []).filter(p => p.market === market);

    // Step 2: Enrich each player with estimated stats + EV from odds
    const enriched = players.map((p) => {
      const l10 = generateFakeL10(p.line);
      const avg = +(l10.reduce((a, b) => a + b, 0) / l10.length).toFixed(1);
      const hitRate = p.line ? Math.round(l10.filter(v => v >= p.line).length / l10.length * 100) : 50;
      const edge = p.line ? +((avg - p.line) / p.line * 100).toFixed(1) : 0;
      const pos = guessPosition(p.name);
      const team = guessTeam(p.name);
      const modelProb = hitRate / 100;
      const impliedProbOver = impliedProb(p.overOdds);
      const impliedProbUnder = impliedProb(p.underOdds);
      const stdDev = +(Math.sqrt(l10.reduce((s,v) => s + Math.pow(v - avg, 2), 0) / l10.length)).toFixed(1);
      const confidence = computeConfidence({ hitRate, modelProb, impliedProbOver, avg, stdDev });
      const evOver = calcEV(modelProb, p.overOdds);
      const evUnder = calcEV(1 - modelProb, p.underOdds);
      return {
        ...p, team, position: pos, avg, l10, hitRate, edge, confidence,
        modelProb, impliedProbOver, impliedProbUnder, evOver, evUnder, isLive: false,
        hasRealStats: false, gamesPlayed: 0,
      };
    });

    cacheSet(ck, enriched, 90);
    res.json({ data: enriched, cached: false, total: enriched.length });
  } catch (err) {
    console.error('Merged error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ---- ANALYTICS HELPERS ----
function generateFakeL10(line) {
  return Array.from({ length: 10 }, () => Math.max(0, Math.round(line + (Math.random() - 0.45) * line * 0.4)));
}

function computeConfidence({ hitRate, modelProb, impliedProbOver, avg, stdDev }) {
  // 1. EV Edge (0–44 pts)
  const evEdge = (modelProb || 0.5) - (impliedProbOver || 0.5);
  const evPts = Math.min(Math.max(evEdge * 220 + 22, 0), 44);
  // 2. Hit Rate Signal (−17 to +33 pts)
  const hrPts = Math.min(Math.max((hitRate - 50) * 0.67, -17), 33);
  // 3. Consistency (0–23 pts): inverse CV
  const cv = avg > 0 && stdDev != null ? stdDev / avg : 0.4;
  const cvPts = Math.max(0, Math.round((0.5 - Math.min(cv, 0.5)) / 0.5 * 23));
  // Scale to 0–100 (max raw = 44+33+23 = 100)
  return Math.max(0, Math.min(100, Math.round(evPts + hrPts + cvPts)));
}

function guessPosition(name) {
  const l = name.toLowerCase();
  const guards = ['curry','young','brunson','fox','haliburton','ball','doncic','murray','morant','cunningham','maxey','mitchell','edwards','booker','reaves'];
  const centers = ['jokic','embiid','sabonis','adebayo','gobert','towns','porzingis','holmgren'];
  if (guards.some(g => l.includes(g))) return 'PG';
  if (centers.some(c => l.includes(c))) return 'C';
  return 'SF';
}

// Implied probability from American odds (removes vig)
function impliedProb(odds) {
  if (!odds) return 0.5;
  const o = +odds;
  return o > 0 ? 100 / (o + 100) : Math.abs(o) / (Math.abs(o) + 100);
}

// Expected value per $1 bet (positive = +EV)
function calcEV(modelProb, odds) {
  if (!odds || !modelProb) return 0;
  const o = +odds;
  const payout = o > 0 ? o / 100 : 100 / Math.abs(o);
  return +((modelProb * payout - (1 - modelProb)) * 100).toFixed(1);
}

function guessTeam(name) {
  const map = {
    // ATL
    'trae young':'ATL','bogdan bogdanovic':'ATL','dejounte murray':'ATL',"de'andre hunter":'ATL','onyeka okongwu':'ATL','clint capela':'ATL','dyson daniels':'ATL','larry nance jr.':'ATL',
    // BOS
    'jayson tatum':'BOS','jaylen brown':'BOS','jrue holiday':'BOS','al horford':'BOS','kristaps porzingis':'BOS','payton pritchard':'BOS','sam hauser':'BOS','derrick white':'BOS',
    // BKN
    'cam thomas':'BKN','nic claxton':'BKN',"day'ron sharpe":'BKN','ben simmons':'BKN','ziaire williams':'BKN',
    // CHA
    'lamelo ball':'CHA','brandon miller':'CHA','miles bridges':'CHA','mark williams':'CHA','grant williams':'CHA','josh green':'CHA',
    // CHI
    'zach lavine':'CHI','nikola vucevic':'CHI','coby white':'CHI','josh giddey':'CHI','patrick williams':'CHI','ayo dosunmu':'CHI',
    // CLE
    'donovan mitchell':'CLE','darius garland':'CLE','evan mobley':'CLE','jarrett allen':'CLE','max strus':'CLE','isaac okoro':'CLE',
    // DAL
    'luka doncic':'DAL','kyrie irving':'DAL','klay thompson':'DAL','p.j. washington':'DAL','dereck lively ii':'DAL','naji marshall':'DAL','dante exum':'DAL',
    // DEN
    'nikola jokic':'DEN','jamal murray':'DEN','michael porter jr.':'DEN','aaron gordon':'DEN','kentavious caldwell-pope':'DEN','reggie jackson':'DEN',
    // DET
    'cade cunningham':'DET','jalen duren':'DET','ausar thompson':'DET','bojan bogdanovic':'DET','monté morris':'DET','malik beasley':'DET',
    // GSW
    'stephen curry':'GSW','draymond green':'GSW','andrew wiggins':'GSW','jonathan kuminga':'GSW','buddy hield':'GSW','gary payton ii':'GSW',
    // HOU
    'alperen sengun':'HOU','jalen green':'HOU','fred vanvleet':'HOU','jabari smith jr.':'HOU','amen thompson':'HOU','dillon brooks':'HOU','tari eason':'HOU',
    // IND
    'tyrese haliburton':'IND','pascal siakam':'IND','myles turner':'IND','benedict mathurin':'IND','andrew nembhard':'IND','t.j. mcconnell':'IND',
    // LAC
    'kawhi leonard':'LAC','james harden':'LAC','ivica zubac':'LAC','norman powell':'LAC','terance mann':'LAC','bones hyland':'LAC',
    // LAL
    'lebron james':'LAL','anthony davis':'LAL','austin reaves':'LAL',"d'angelo russell":'LAL','rui hachimura':'LAL','max christie':'LAL','gabe vincent':'LAL',
    // MEM
    'ja morant':'MEM','desmond bane':'MEM','jaren jackson jr.':'MEM','marcus smart':'MEM','ziaire williams':'MEM','luke kennard':'MEM',
    // MIA
    'bam adebayo':'MIA','tyler herro':'MIA','jimmy butler':'MIA','terry rozier':'MIA','haywood highsmith':'MIA','caleb martin':'MIA',
    // MIL
    'giannis antetokounmpo':'MIL','damian lillard':'MIL','khris middleton':'MIL','brook lopez':'MIL','bobby portis':'MIL','malik beasley':'MIL',
    // MIN
    'anthony edwards':'MIN','rudy gobert':'MIN','jaden mcdaniels':'MIN','naz reid':'MIN','mike conley':'MIN','nickeil alexander-walker':'MIN',
    // NOP
    'zion williamson':'NOP','cj mccollum':'NOP','brandon ingram':'NOP','trey murphy iii':'NOP','herb jones':'NOP','jonas valanciunas':'NOP',
    // NYK
    'jalen brunson':'NYK','karl-anthony towns':'NYK','mikal bridges':'NYK','og anunoby':'NYK','josh hart':'NYK','donte divincenzo':'NYK',
    // OKC
    'shai gilgeous-alexander':'OKC','jalen williams':'OKC','chet holmgren':'OKC','lu dort':'OKC','isaiah joe':'OKC','alex caruso':'OKC',
    // ORL
    'paolo banchero':'ORL','franz wagner':'ORL','wendell carter jr.':'ORL','cole anthony':'ORL','jalen suggs':'ORL','markelle fultz':'ORL',
    // PHI
    'joel embiid':'PHI','tyrese maxey':'PHI','paul george':'PHI','kelly oubre jr.':'PHI','tobias harris':'PHI',
    // PHX
    'devin booker':'PHX','kevin durant':'PHX','bradley beal':'PHX','grayson allen':'PHX','jusuf nurkic':'PHX','eric gordon':'PHX',
    // POR
    'anfernee simons':'POR','scoot henderson':'POR','jerami grant':'POR','deandre ayton':'POR','shaedon sharpe':'POR',
    // SAC
    "de'aaron fox":'SAC','domantas sabonis':'SAC','keegan murray':'SAC','malik monk':'SAC','harrison barnes':'SAC',
    // SAS
    'victor wembanyama':'SAS','keldon johnson':'SAS','devin vassell':'SAS','jeremy sochan':'SAS','stephon castle':'SAS','tre jones':'SAS',
    // TOR
    'scottie barnes':'TOR','rj barrett':'TOR','immanuel quickley':'TOR','gradey dick':'TOR','jakob poeltl':'TOR',
    // UTA
    'lauri markkanen':'UTA','jordan clarkson':'UTA','collin sexton':'UTA','john collins':'UTA','walker kessler':'UTA','keyonte george':'UTA',
    // WAS
    'kyle kuzma':'WAS','bilal coulibaly':'WAS','tyus jones':'WAS','deni avdija':'WAS','marvin bagley iii':'WAS',
  };
  return map[name.toLowerCase()] || '???';
}


// ============================================================
// ROUTE: GET /api/cache/clear — manual cache flush
// ============================================================
app.get('/api/cache/clear', (req, res) => {
  cache.flushAll();
  res.json({ message: 'Cache cleared', keys: 0 });
});

// ============================================================
// ROUTE: GET /api/cron/refresh-stats — nightly BDL → Supabase pre-fetch
// Called by Vercel Cron. Fetches game logs for all players currently in
// the props feed and stores them in Supabase so page loads are instant.
// ============================================================
app.get('/api/cron/refresh-stats', async (req, res) => {
  // Accept secret via header (Vercel Cron) or query param (manual browser trigger)
  const secret = req.headers['x-cron-secret'] || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!supabase) return res.status(503).json({ error: 'Supabase not configured' });
  if (!BDL_KEY) return res.status(503).json({ error: 'BDL key not configured' });

  const results = { ok: [], failed: [], skipped: [] };
  try {
    // Get player names from Supabase odds_cache (populated by refresh-odds cron)
    // This avoids extra Odds API calls and ensures we use the same player list users see
    const oddsPlayers = await sbGetOdds('combined');
    if (!oddsPlayers || !oddsPlayers.length) {
      return res.status(503).json({ error: 'No odds data in Supabase — run refresh-odds first' });
    }
    const playerNames = new Set(oddsPlayers.map(p => p.name).filter(Boolean));

    // Fetch BDL stats for each player with throttling
    const names = [...playerNames];
    for (let i = 0; i < names.length; i++) {
      const name = names[i];
      try {
        // Skip if refreshed within last 20h — daily cron always re-fetches (runs every 24h)
        const existing = await sbGetGameLog(name, 20);
        if (existing) { results.skipped.push(name); continue; }

        const { id: bdlId, position: bdlPos } = await getBDLPlayerId(name);
        if (!bdlId) { results.failed.push(`${name} (not found in BDL)`); continue; }
        const games = await fetchBDLGameLog(bdlId);
        await sbSetGameLog(name, bdlId, games, bdlPos);
        results.ok.push(name);
      } catch (e) {
        results.failed.push(`${name} (${e.message})`);
      }
      // Throttle to avoid BDL rate limits
      if (i < names.length - 1) await new Promise(r => setTimeout(r, 400));
    }

    res.json({ success: true, processed: names.length, ...results });
  } catch (err) {
    res.status(500).json({ error: err.message, ...results });
  }
});

// ============================================================
// ROUTE: GET /api/cron/refresh-odds — store Odds API data in Supabase
// Runs on schedule so users never hit the Odds API directly
// ============================================================
app.get('/api/cron/refresh-odds', async (req, res) => {
  const secret = req.headers['x-cron-secret'] || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!supabase) return res.status(503).json({ error: 'Supabase not configured' });
  if (!ODDS_KEY) return res.status(503).json({ error: 'Odds API key not configured' });

  try {
    // Fetch upcoming events
    const evtResp = await fetch(`${ODDS_BASE}/sports/basketball_nba/events?apiKey=${ODDS_KEY}&regions=us&oddsFormat=american`);
    if (!evtResp.ok) return res.status(502).json({ error: `Events fetch failed: ${evtResp.status}` });
    const events = await evtResp.json();
    const now = new Date();
    const upcoming = events.filter(e => new Date(e.commence_time) > now);

    if (!upcoming.length) return res.json({ success: true, message: 'No upcoming games', stored: [] });

    // Fetch all markets for all upcoming games sequentially to avoid hammering the API
    const rawProps = [];
    for (const evt of upcoming) {
      try {
        const pUrl = `${ODDS_BASE}/sports/basketball_nba/events/${evt.id}/odds?apiKey=${ODDS_KEY}&regions=us&markets=${ALL_PROP_MARKETS}&oddsFormat=american`;
        const pResp = await fetch(pUrl);
        if (pResp.ok) rawProps.push(await pResp.json());
      } catch { /* skip */ }
    }

    // Aggregate and store for each book mode we support
    const books = ['combined', 'draftkings', 'fanduel', 'betmgm', 'caesars', 'pointsbet'];
    const stored = [];
    for (const book of books) {
      const players = aggregatePlayers(rawProps, book);
      if (players.length) {
        await sbSetOdds(book, players);
        stored.push({ book, players: players.length });
      }
    }

    // Bust in-memory cache so next request pulls fresh Supabase data
    books.forEach(b => cache.del(`allMarkets_${b}`));

    res.json({ success: true, games: upcoming.length, stored });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/debug/player/:name — diagnose a specific player lookup
// ============================================================
app.get('/api/debug/player/:name', async (req, res) => {
  const name = decodeURIComponent(req.params.name);
  try {
    // Bust cached player ID so re-lookup uses latest matching logic
    const ck = `bdl_pid_${name.toLowerCase().replace(/\s+/g, '_')}`;
    cache.delete(ck);
    const { id, position } = await getBDLPlayerId(name);
    if (!id) return res.json({ name, found: false, error: 'Player not found in BDL' });
    const games = await fetchBDLGameLog(id);
    res.json({
      name, found: true, bdlId: id, position,
      season: NBA_SEASON, gamesFound: games.length,
    });
  } catch (err) {
    res.json({ name, error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/debug/clear-player/:name — wipe cached data so next fetch re-resolves
// ============================================================
app.get('/api/debug/clear-player/:name', async (req, res) => {
  const name = decodeURIComponent(req.params.name);
  const pidKey = `bdl_pid_${name.toLowerCase().replace(/\s+/g, '_')}`;
  const glKey  = `gl_name_${NBA_SEASON}_${name.toLowerCase().replace(/\s+/g, '_')}`;
  cache.delete(pidKey);
  cache.delete(glKey);
  let sbDeleted = false;
  if (supabase) {
    const { error } = await supabase.from('player_stats').delete().eq('player_name', name);
    sbDeleted = !error;
  }
  res.json({ name, memoryCleared: true, supabaseDeleted: sbDeleted });
});

// ============================================================
// ROUTE: GET /api/debug/bdl — test BDL connectivity
// ============================================================
app.get('/api/debug/bdl', async (req, res) => {
  const result = { keyConfigured: !!BDL_KEY, season: NBA_SEASON, bdlBase: BDL_BASE };
  if (!BDL_KEY) return res.json({ ...result, error: 'BDL_API_KEY not set in environment' });
  try {
    const searchUrl = `${BDL_BASE}/players?search=LeBron%20James&per_page=1`;
    result.searchUrl = searchUrl;
    const searchResp = await fetch(searchUrl, { headers: bdlHeaders() });
    result.httpStatus = searchResp.status;
    if (!searchResp.ok) return res.json({ ...result, error: `BDL returned ${searchResp.status}` });
    const searchData = await searchResp.json();
    const player = searchData.data?.[0];
    if (!player) return res.json({ ...result, error: 'No player found in search results' });
    result.playerFound = `${player.first_name} ${player.last_name} (id=${player.id})`;
    const statsUrl = `${BDL_BASE}/stats?player_ids[]=${player.id}&seasons[]=${NBA_SEASON}&per_page=3`;
    const statsResp = await fetch(statsUrl, { headers: bdlHeaders() });
    result.statsStatus = statsResp.status;
    if (statsResp.ok) {
      const sd = await statsResp.json();
      result.gamesFound = sd.data?.length || 0;
      result.sampleGame = sd.data?.[0] ? { date: sd.data[0].game?.date, pts: sd.data[0].pts } : null;
    }
    result.ok = true;
    res.json(result);
  } catch (err) {
    res.json({ ...result, error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/headshot/:id — proxy NBA CDN to avoid CORS
// ============================================================
app.get('/api/headshot/:id', async (req, res) => {
  try {
    const url = `https://cdn.nba.com/headshots/nba/latest/1040x760/${req.params.id}.png`;
    const resp = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': 'https://www.nba.com/',
        'Accept': 'image/png,image/*,*/*',
      },
    });
    if (!resp.ok) return res.status(404).end();
    const buf = await resp.buffer();
    res.set('Content-Type', 'image/png');
    res.set('Cache-Control', 'public, max-age=86400');
    res.send(buf);
  } catch {
    res.status(404).end();
  }
});

// ============================================================
// AUTH ROUTES
// ============================================================

// Helper: extract & validate Bearer token → returns user or null
async function authUser(req) {
  if (!supabase) return null;
  const token = (req.headers.authorization || '').replace('Bearer ', '').trim();
  if (!token) return null;
  try {
    const { data: { user }, error } = await supabase.auth.getUser(token);
    return error ? null : user;
  } catch { return null; }
}

app.post('/api/auth/login', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Auth not configured' });
  const { email, password } = req.body || {};
  if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
  try {
    const { data, error } = await supabase.auth.signInWithPassword({ email, password });
    if (error) return res.status(401).json({ error: error.message });
    const { session, user } = data;
    res.json({ token: session.access_token, userId: user.id, email: user.email });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/auth/logout', async (req, res) => {
  res.json({ ok: true });
});

app.get('/api/user/profile', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const { data: profile } = await supabase
      .from('user_profiles')
      .select('*')
      .eq('id', user.id)
      .single();
    if (!profile) {
      // First login — create default profile
      const def = { id: user.id, balance: 1000, bets: [], preferences: {} };
      await supabase.from('user_profiles').insert(def);
      return res.json({ balance: 1000, bets: [], preferences: {}, displayName: user.email.split('@')[0] });
    }
    res.json({
      balance: profile.balance ?? 1000,
      bets: profile.bets || [],
      preferences: profile.preferences || {},
      displayName: profile.display_name || user.email.split('@')[0],
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/user/profile', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const { balance, bets, preferences, displayName } = req.body || {};
    const updates = { id: user.id, updated_at: new Date().toISOString() };
    if (balance !== undefined) updates.balance = balance;
    if (bets !== undefined) updates.bets = bets;
    if (preferences !== undefined) updates.preferences = preferences;
    if (displayName !== undefined) updates.display_name = displayName;
    await supabase.from('user_profiles').upsert(updates, { onConflict: 'id' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---- CATCH-ALL: Serve frontend ----
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ---- START (local dev only) ----
if (require.main === module) {
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`\n  ╔═══════════════════════════════════════╗`);
    console.log(`  ║  PropEdge v3 Server                   ║`);
    console.log(`  ║  Running on http://localhost:${PORT}      ║`);
    console.log(`  ║  Odds API:    ${ODDS_KEY ? '✓ Configured' : '✗ Missing'}           ║`);
    console.log(`  ║  BDL API:     ${BDL_KEY ? '✓ Configured' : '✗ Missing'}              ║`);
    console.log(`  ╚═══════════════════════════════════════╝\n`);
  });
}

// Load DvP cache from Supabase on cold start (non-blocking)

// Required for Vercel serverless
module.exports = app;
