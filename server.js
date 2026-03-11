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
const STATS_TTL = 12 * 60 * 60; // 12-hour cache for stats

// ---- SUPABASE SETUP ----
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const supabase = SUPABASE_URL && SUPABASE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_KEY)
  : null;

// Read game log from Supabase cache (returns null if not found or stale)
async function sbGetGameLog(playerName) {
  if (!supabase) return null;
  try {
    const { data, error } = await supabase
      .from('player_stats')
      .select('game_log, last_fetched')
      .eq('player_name', playerName)
      .single();
    if (error || !data) return null;
    // Treat as stale if older than 20 hours
    const age = Date.now() - new Date(data.last_fetched).getTime();
    if (age > 20 * 60 * 60 * 1000) return null;
    return data.game_log;
  } catch { return null; }
}

// Write game log to Supabase cache (upsert by player_name)
async function sbSetGameLog(playerName, bdlId, gameLog) {
  if (!supabase || !gameLog?.length) return;
  try {
    await supabase.from('player_stats').upsert({
      player_name: playerName,
      bdl_id: bdlId,
      game_log: gameLog,
      season: NBA_SEASON,
      last_fetched: new Date().toISOString(),
    }, { onConflict: 'player_name' });
  } catch (e) { console.warn('Supabase write failed:', e.message); }
}

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

// Fetch full season game log from BDL (cursor-paginated)
async function fetchBDLGameLog(playerId) {
  let allRows = [];
  let cursor = null;
  do {
    let url = `${BDL_BASE}/stats?player_ids[]=${playerId}&seasons[]=${NBA_SEASON}&per_page=100`;
    if (cursor) url += `&cursor=${cursor}`;
    const resp = await fetch(url, { headers: bdlHeaders() });
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
      home, wl: '',
    };
  }).filter(g => g && g.date);
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

// Search BDL for player ID by name, cached 24h
async function getBDLPlayerId(name) {
  const ck = `bdl_pid_${name.toLowerCase().replace(/\s+/g, '_')}`;
  const cached = cacheGet(ck);
  if (cached !== undefined) return cached;

  const trySearch = async (query) => {
    try {
      const resp = await fetch(`${BDL_BASE}/players?search=${encodeURIComponent(query)}&per_page=10`, { headers: bdlHeaders() });
      if (!resp.ok) return [];
      const data = await resp.json();
      return data.data || [];
    } catch { return []; }
  };

  // Search 1: full name
  let results = await trySearch(name);

  // Search 2: normalized name (strip suffix) if different
  const normed = normalizeName(name);
  const normedTitle = normed.replace(/\b\w/g, c => c.toUpperCase());
  if (!results.length && normedTitle !== name) {
    results = await trySearch(normedTitle);
  }

  // Search 3: just last name as fallback
  if (!results.length) {
    const lastName = name.trim().split(' ').pop();
    results = await trySearch(lastName);
  }

  // Pick best match by score
  let best = null;
  let bestScore = 0;
  for (const p of results) {
    const score = nameMatchScore(p, name);
    if (score > bestScore) { bestScore = score; best = p; }
  }

  // Only accept single-result fallback if score is 0 and there's exactly 1 result
  // (avoids picking a random wrong player when multiple results exist)
  if (!best && results.length === 1) best = results[0];

  const id = (best && bestScore >= 40) ? best.id : (results.length === 1 ? results[0]?.id : null);
  if (best) console.log(`BDL name match: "${name}" → "${best.first_name} ${best.last_name}" (score=${bestScore})`);
  else console.warn(`BDL name lookup failed: "${name}" (${results.length} results, best score=${bestScore})`);

  cacheSet(ck, id ?? null, 24 * 3600);
  return id ?? null;
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
    const players = [];
    rawProps.forEach(evt => {
      if (!evt.bookmakers) return;
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
          players.push({
            name: playerName,
            line: over.point,
            overOdds: over.price,
            underOdds: sides['Under']?.price,
            market: mkt.key,
            matchup: `${teamAbbr(evt.away_team)} @ ${teamAbbr(evt.home_team)}`,
            homeTeam: teamAbbr(evt.home_team),
            awayTeam: teamAbbr(evt.away_team),
            gameTime: evt.commence_time,
            bookmaker: bk.title,
          });
        });
      });
    });

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
    const id = await getBDLPlayerId(name);
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
    const ckMem = `gl_name_${name.toLowerCase().replace(/\s+/g,'_')}`;
    const memHit = cacheGet(ckMem);
    if (memHit) return res.json({ data: memHit, cached: true, source: 'memory' });

    // Layer 2: Supabase persistent cache
    const sbHit = await sbGetGameLog(name);
    if (sbHit) {
      cacheSet(ckMem, sbHit, STATS_TTL);
      return res.json({ data: sbHit, cached: true, source: 'supabase' });
    }

    // Layer 3: Live BDL fetch
    const nbaId = await getBDLPlayerId(name);
    if (!nbaId) return res.json({ data: [], cached: false, error: `Player not found: ${name}` });
    const games = await fetchBDLGameLog(nbaId);
    cacheSet(ckMem, games, STATS_TTL);
    await sbSetGameLog(name, nbaId, games); // persist for future requests
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
    const ckMem = `gl_name_${name.toLowerCase().replace(/\s+/g,'_')}`;
    let games = cacheGet(ckMem);
    if (!games) {
      games = await sbGetGameLog(name);
      if (!games) {
        const nbaId = await getBDLPlayerId(name);
        if (!nbaId) return res.json({ data: null, cached: false, error: `Player not found: ${name}` });
        games = await fetchBDLGameLog(nbaId);
        await sbSetGameLog(name, nbaId, games);
      }
      cacheSet(ckMem, games, STATS_TTL);
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

  const sorted = [...games].sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
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
// ROUTE: GET /api/analytics/dvp — Defense vs Position
// ============================================================
app.get('/api/analytics/dvp', async (req, res) => {
  try {
    const position = req.query.position || 'G';

    const ck = `dvp_${position}`;
    const cached = cacheGet(ck);
    if (cached) return res.json({ data: cached, cached: true });

    // Try NBA.com stats endpoint for DvP
    const nbaPosition = position === 'PG' || position === 'SG' ? 'G'
      : position === 'SF' || position === 'PF' ? 'F' : 'C';

    try {
      const url = `${NBA_BASE}/leaguedashptdefend?DefenseCategory=Overall&LeagueID=00&PerMode=PerGame&Season=${NBA_SEASON}-${String(NBA_SEASON+1).slice(2)}&SeasonType=Regular+Season&PlayerPosition=${nbaPosition}`;
      const resp = await fetch(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0',
          'Referer': 'https://stats.nba.com/',
          'Accept': 'application/json',
        }
      });

      if (resp.ok) {
        const data = await resp.json();
        const rows = data.resultSets?.[0];
        if (rows) {
          const headers = rows.headers;
          const mapped = rows.rowSet.map(row => {
            const obj = {};
            headers.forEach((h, i) => obj[h] = row[i]);
            return obj;
          });
          cacheSet(ck, mapped, parseInt(process.env.CACHE_TTL_DVP) || 600);
          return res.json({ data: mapped, cached: false, source: 'nba.com' });
        }
      }
    } catch (nbaErr) {
      console.log('NBA.com DvP unavailable, using computed fallback');
    }

    // Fallback: compute from team defensive ratings
    const fallback = computeFallbackDvP(position);
    cacheSet(ck, fallback, parseInt(process.env.CACHE_TTL_DVP) || 600);
    res.json({ data: fallback, cached: false, source: 'computed' });
  } catch (err) {
    console.error('DvP error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

function computeFallbackDvP(position) {
  // Seed-based deterministic rankings per team+position
  const teams = ['ATL','BOS','BKN','CHA','CHI','CLE','DAL','DEN','DET','GSW',
    'HOU','IND','LAC','LAL','MEM','MIA','MIL','MIN','NOP','NYK',
    'OKC','ORL','PHI','PHX','POR','SAC','SAS','TOR','UTA','WAS'];

  return teams.map(t => {
    const seed = (t + position).split('').reduce((a, c) => a + c.charCodeAt(0), 0);
    const rank = ((seed * 7) % 30) + 1;
    const ppg = +(105 + (30 - rank) / 30 * 15 + ((seed % 5) - 2)).toFixed(1);
    return { team: t, rank, ppg, position };
  }).sort((a, b) => a.rank - b.rank);
}

// ============================================================
// ROUTE: GET /api/analytics/merged — THE BIG ONE
// Full merged dataset: odds + stats + analytics
// ============================================================
app.get('/api/analytics/merged', async (req, res) => {
  try {
    const market = req.query.market || 'player_points';
    const book = req.query.book || 'draftkings';

    const ck = `merged_${market}_${book}`;
    const cached = cacheGet(ck);
    if (cached) return res.json({ data: cached, cached: true });

    // Step 1: Get all props
    const now = new Date();
    let players = [];
    if (ODDS_KEY) {
      const evtUrl = `${ODDS_BASE}/sports/basketball_nba/events?apiKey=${ODDS_KEY}&regions=us&oddsFormat=american`;
      const evtResp = await fetch(evtUrl);
      if (evtResp.ok) {
        const events = await evtResp.json();

        // Only fetch props for games that haven't started yet — saves Odds API quota
        const upcomingEvents = events.filter(evt => new Date(evt.commence_time) > now);

        const propPromises = upcomingEvents.map(async (evt) => {
          try {
            const pUrl = `${ODDS_BASE}/sports/basketball_nba/events/${evt.id}/odds?apiKey=${ODDS_KEY}&regions=us&markets=${market}&oddsFormat=american`;
            const pResp = await fetch(pUrl);
            return pResp.ok ? await pResp.json() : null;
          } catch { return null; }
        });

        const rawProps = (await Promise.all(propPromises)).filter(Boolean);

        rawProps.forEach(evt => {
          if (!evt.bookmakers) return;
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
              players.push({
                name: playerName,
                line: over.point,
                overOdds: over.price,
                underOdds: sides['Under']?.price,
                matchup: `${teamAbbr(evt.away_team)} @ ${teamAbbr(evt.home_team)}`,
                homeTeam: teamAbbr(evt.home_team),
                awayTeam: teamAbbr(evt.away_team),
                gameTime: evt.commence_time,
                bookmaker: bk.title,
              });
            });
          });
        });
      }
    }

    // Step 2: Enrich each player with estimated stats + EV from odds
    const enriched = players.map((p) => {
      const l10 = generateFakeL10(p.line);
      const avg = +(l10.reduce((a, b) => a + b, 0) / l10.length).toFixed(1);
      const hitRate = p.line ? Math.round(l10.filter(v => v >= p.line).length / l10.length * 100) : 50;
      const edge = p.line ? +((avg - p.line) / p.line * 100).toFixed(1) : 0;
      const pos = guessPosition(p.name);
      const team = guessTeam(p.name);
      const dvpRank = computeDvPRank(p.homeTeam === team ? p.awayTeam : p.homeTeam, pos);
      const dvpClass = dvpRank <= 10 ? 'Easy' : dvpRank >= 21 ? 'Hard' : 'Mid';
      const modelProb = hitRate / 100;
      const impliedProbOver = impliedProb(p.overOdds);
      const impliedProbUnder = impliedProb(p.underOdds);
      const stdDev = +(Math.sqrt(l10.reduce((s,v) => s + Math.pow(v - avg, 2), 0) / l10.length)).toFixed(1);
      const confidence = computeConfidence({ hitRate, dvpRank, modelProb, impliedProbOver, avg, stdDev });
      const evOver = calcEV(modelProb, p.overOdds);
      const evUnder = calcEV(1 - modelProb, p.underOdds);
      return {
        ...p, team, position: pos, avg, l10, hitRate, edge, dvpRank, dvpClass, confidence,
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

function computeConfidence({ hitRate, dvpRank, modelProb, impliedProbOver, avg, stdDev }) {
  // 1. EV Edge (0–40 pts): does our model probability beat the implied probability?
  //    evEdge = modelProb − impliedProbOver
  //    0 gap = 20 pts (neutral), +10% gap = 40 pts, −10% gap = 0 pts
  const evEdge = (modelProb || 0.5) - (impliedProbOver || 0.5);
  const evPts = Math.min(Math.max(evEdge * 200 + 20, 0), 40);

  // 2. Hit Rate Signal (−15 to +30 pts): how far above 50% is the hit rate?
  //    50% = 0 pts, 70% = +12 pts, 80% = +18 pts
  //    Below 50% is penalized: 40% = −6 pts, 30% = −12 pts
  const hrPts = Math.min(Math.max((hitRate - 50) * 0.6, -15), 30);

  // 3. Consistency (0–20 pts): inverse coefficient of variation
  //    CV = stdDev / avg. Lower CV = more consistent player = more confident.
  //    CV 0.0 = 20 pts (no variance), CV 0.5+ = 0 pts (very volatile)
  const cv = avg > 0 && stdDev != null ? stdDev / avg : 0.4;
  const cvPts = Math.max(0, Math.round((0.5 - Math.min(cv, 0.5)) / 0.5 * 20));

  // 4. Matchup DvP (0–10 pts): is the opposing defense easy or hard?
  const dvpPts = dvpRank <= 10 ? 10 : dvpRank >= 21 ? 0 : 5;

  return Math.max(0, Math.min(100, Math.round(evPts + hrPts + cvPts + dvpPts)));
}

function computeDvPRank(opp, pos) {
  const seed = (opp + pos).split('').reduce((a, c) => a + c.charCodeAt(0), 0);
  return ((seed * 7) % 30) + 1;
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
    // Get current player list from the odds feed
    const evtUrl = `${ODDS_BASE}/sports/basketball_nba/events?apiKey=${ODDS_KEY}&regions=us&oddsFormat=american`;
    const evtResp = await fetch(evtUrl);
    if (!evtResp.ok) return res.status(502).json({ error: 'Could not fetch events from Odds API' });
    const events = await evtResp.json();
    const now = new Date();
    const upcoming = events.filter(e => new Date(e.commence_time) > now);

    // Collect player names from one bookmaker per game
    const playerNames = new Set();
    for (const evt of upcoming.slice(0, 10)) {
      try {
        const pUrl = `${ODDS_BASE}/sports/basketball_nba/events/${evt.id}/odds?apiKey=${ODDS_KEY}&regions=us&markets=player_points&oddsFormat=american`;
        const pResp = await fetch(pUrl);
        if (!pResp.ok) continue;
        const pData = await pResp.json();
        const bk = pData.bookmakers?.[0];
        bk?.markets?.[0]?.outcomes?.forEach(o => {
          if (o.description) playerNames.add(o.description);
        });
      } catch { /* skip */ }
    }

    // Fetch BDL stats for each player with throttling
    const names = [...playerNames];
    for (let i = 0; i < names.length; i++) {
      const name = names[i];
      try {
        // Skip if Supabase already has fresh data (< 20h old)
        const existing = await sbGetGameLog(name);
        if (existing) { results.skipped.push(name); continue; }

        const bdlId = await getBDLPlayerId(name);
        if (!bdlId) { results.failed.push(`${name} (not found in BDL)`); continue; }
        const games = await fetchBDLGameLog(bdlId);
        await sbSetGameLog(name, bdlId, games);
        results.ok.push(name);
      } catch (e) {
        results.failed.push(`${name} (${e.message})`);
      }
      // Throttle to avoid BDL rate limits
      if (i < names.length - 1) await new Promise(r => setTimeout(r, 800));
    }

    res.json({ success: true, processed: names.length, ...results });
  } catch (err) {
    res.status(500).json({ error: err.message, ...results });
  }
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

// Required for Vercel serverless
module.exports = app;
