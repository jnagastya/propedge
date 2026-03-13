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
const webpush = require('web-push');
const sharp = require('sharp');
const ESPN_PLAYERS = require('./data/espn-players.json'); // name → ESPN athlete ID
// Odds API name aliases → ESPN file names
const ESPN_ALIASES = {
  'G.G. Jackson': 'GG Jackson',
  'R.J. Barrett': 'RJ Barrett',
  'Herb Jones': 'Herbert Jones',
  'Robert Williams': 'Robert Williams III',
  'Isaiah Stewart II': 'Isaiah Stewart',
  'Derrick Jones': 'Derrick Jones Jr.',
  'Jabari Smith Jr': 'Jabari Smith Jr.',
};
Object.entries(ESPN_ALIASES).forEach(([alias, real]) => { if (ESPN_PLAYERS[real]) ESPN_PLAYERS[alias] = ESPN_PLAYERS[real]; });
const NodeCache = require('node-cache');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ---- PWA ICON ROUTES (SVG → PNG for iOS apple-touch-icon) ----
const ICON_SVG = `<svg width="512" height="512" viewBox="0 0 512 512" fill="none" xmlns="http://www.w3.org/2000/svg"><rect width="512" height="512" rx="114" fill="url(#lg)"/><line x1="212" y1="72" x2="179" y2="33" stroke="rgba(255,255,255,.55)" stroke-width="18" stroke-linecap="round"/><line x1="300" y1="72" x2="333" y2="33" stroke="rgba(255,255,255,.55)" stroke-width="18" stroke-linecap="round"/><rect x="223" y="67" width="66" height="55" rx="33" fill="rgba(255,255,255,.55)"/><path d="M256 122C178 122 112 178 112 268L112 301Q112 346 145 346L367 346Q400 346 400 301L400 268C400 178 334 122 256 122Z" fill="white" opacity=".9"/><circle cx="256" cy="395" r="33" fill="rgba(255,255,255,.7)"/><path d="M83 178Q45 256 83 334" stroke="rgba(255,255,255,.65)" stroke-width="20" stroke-linecap="round" fill="none"/><path d="M429 178Q467 256 429 334" stroke="rgba(255,255,255,.65)" stroke-width="20" stroke-linecap="round" fill="none"/><defs><linearGradient id="lg" x1="0" y1="0" x2="512" y2="512" gradientUnits="userSpaceOnUse"><stop offset="0%" stop-color="#4f46e5"/><stop offset="100%" stop-color="#7c3aed"/></linearGradient></defs></svg>`;
let _iconCache = {};
async function serveIconPng(size, res) {
  if (!_iconCache[size]) {
    _iconCache[size] = await sharp(Buffer.from(ICON_SVG)).resize(size, size).png().toBuffer();
  }
  res.set('Content-Type', 'image/png').set('Cache-Control', 'public, max-age=86400').send(_iconCache[size]);
}
app.get('/icon-192.png', (req, res) => serveIconPng(192, res).catch(() => res.status(500).end()));
app.get('/icon-512.png', (req, res) => serveIconPng(512, res).catch(() => res.status(500).end()));

// ---- CACHE SETUP ----
const cache = new NodeCache({
  stdTTL: parseInt(process.env.CACHE_TTL_ODDS) || 120,
  checkperiod: 30,
});

let ODDS_KEY = process.env.ODDS_API_KEY || '';
let BDL_KEY = process.env.BDL_API_KEY || '';

// Player → team cache (populated by refresh-stats, cold-start load, and stats fetches)
const _playerTeamCache = {
  'Derik Queen':'HOU','Jeremiah Fears':'OKC','Mohamed Diawara':'ATL',
  'Ivica Zubac':'IND','Gui Santos':'GSW','Marcus Sasser':'DET',
  'Cody Williams':'UTA','Mitchell Robinson':'NYK','Brice Sensabaugh':'TOR',
  'Ace Bailey':'BKN','Sandro Mamukelashvili':'SAS','Matas Buzelis':'CHI',
  'Keon Ellis':'SAC','Olivier-Maxence Prosper':'DAL','Isaiah Collier':'UTA',
  'Kyle Filipowski':'UTA','Jaylon Tyson':'CLE','Dennis Schroder':'BKN',
  'Taylor Hendricks':'UTA','G.G. Jackson':'MEM','Royce O\'Neale':'PHX',
  'Ty Jerome':'MEM','Jarace Walker':'IND','Landry Shamet':'NYK',
  'Brandin Podziemski':'GSW','De\'Anthony Melton':'BKN','Cedric Coward':'LAC',
  'Duncan Robinson':'MIA','Kristaps Porzingis':'GSW','Reed Sheppard':'HOU',
  'Julius Randle':'MIN','Derrick Jones':'LAC','Kobe Sanders':'LAC',
  'R.J. Barrett':'TOR','Collin Gillespie':'DEN','Sam Merrill':'CLE',
  'Aaron Nesmith':'IND','Oso Ighodaro':'PHX','Will Richard':'GSW',
  'Toumani Camara':'POR','Bennedict Mathurin':'IND','Jaylen Wells':'MEM',
  'Dean Wade':'CLE','Robert Williams':'POR','Isaiah Stewart II':'DET',
  'Donovan Clingan':'POR','Cooper Flagg':'LAL','Saddiq Bey':'ATL',
  'Kris Dunn':'LAC','Kevin Huerter':'IND','Jabari Smith Jr':'HOU',
};
const ODDS_BASE = 'https://api.the-odds-api.com/v4';
const BDL_BASE = 'https://api.balldontlie.io/nba/v1';
const NBA_BASE = 'https://stats.nba.com/stats';
const STATS_TTL = 3 * 60 * 60; // 3-hour cache for stats (games happen daily)

// ---- WEB PUSH SETUP ----
const VAPID_PUBLIC = process.env.VAPID_PUBLIC_KEY || '';
const VAPID_PRIVATE = process.env.VAPID_PRIVATE_KEY || '';
if (VAPID_PUBLIC && VAPID_PRIVATE) {
  webpush.setVapidDetails('mailto:agent@propedge.app', VAPID_PUBLIC, VAPID_PRIVATE);
}

// ---- SUPABASE SETUP ----
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const supabase = SUPABASE_URL && SUPABASE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_KEY)
  : null;

// Read game log from Supabase cache (returns null if not found or stale)
async function sbGetGameLog(playerName, maxAgeHours = 168) { // 7-day default
  if (!supabase) return null;
  try {
    const { data, error } = await supabase
      .from('player_stats')
      .select('game_log, last_fetched, season')
      .eq('player_name', playerName)
      .single();
    if (error || !data) return null;
    if (data.season !== NBA_SEASON) return null;
    const age = Date.now() - new Date(data.last_fetched).getTime();
    if (age > maxAgeHours * 60 * 60 * 1000) return null;
    return data.game_log;
  } catch { return null; }
}

// Returns full Supabase record for incremental merge in the cron (no TTL check)
async function sbGetGameLogRecord(playerName) {
  if (!supabase) return null;
  try {
    const { data, error } = await supabase
      .from('player_stats')
      .select('game_log, last_fetched, season')
      .eq('player_name', playerName)
      .single();
    if (error || !data) return null;
    if (data.season !== NBA_SEASON) return null;
    return data;
  } catch { return null; }
}

// Write game log to Supabase cache (upsert by player_name)
async function sbSetGameLog(playerName, bdlId, gameLog, position) {
  if (!supabase || !gameLog?.length) return;
  try {
    const { error } = await supabase.from('player_stats').upsert({
      player_name: playerName,
      bdl_id: bdlId,
      game_log: gameLog,
      position: position || null,  // BDL-sourced position: 'G', 'F', or 'C'
      season: NBA_SEASON,
      last_fetched: new Date().toISOString(),
    }, { onConflict: 'player_name' });
    if (error) console.warn(`Supabase write failed for ${playerName}:`, error.message);
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
    const { error } = await supabase.from('odds_cache').upsert({
      book,
      players,
      last_fetched: new Date().toISOString(),
    }, { onConflict: 'book' });
    if (error) console.warn(`Supabase odds write failed for ${book}:`, error.message);
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
async function fetchBDLGameLog(playerId, startDate = null) {
  let allRows = [];
  let cursor = null;
  do {
    let url = `${BDL_BASE}/stats?player_ids[]=${playerId}&seasons[]=${NBA_SEASON}&per_page=100`;
    if (startDate) url += `&start_date=${startDate}`;
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
      team: g.team?.abbreviation || null,
      home, wl: '', opp_team_id: opp || null,
    };
  }).filter(g => g && g.date); // keep DNP rows (min=0) so client can show missed games + injury flags
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
  // Check if last name matches and first name matches to varying degrees
  const tParts = normTarget.split(' ');
  const bLast = bdlPlayer.last_name.toLowerCase();
  const bFirst = bdlPlayer.first_name.toLowerCase();
  const tFirst = tParts[0];
  const tLast = tParts[tParts.length - 1];
  if (bLast === tLast && bFirst === tFirst) return 75;        // exact first+last (post-normalize)
  if (bLast === tLast && (bFirst.startsWith(tFirst) || tFirst.startsWith(bFirst))) return 65; // first name prefix
  if (bLast === tLast && bFirst[0] === tFirst[0]) return 55; // last name + first initial
  if (bLast === tLast) return 40;                             // last name only
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

// Manual overrides for players BDL search can't find by name.
// Format: 'Odds API Name' -> { id, position, team }
// Find a player's BDL ID via: /api/debug/bdl-search/<name>
const BDL_PLAYER_OVERRIDES = {
  'Tre Jones':        { id: 3547274,    position: 'G', team: 'CHI' },
  'Tre Johnson':      { id: 1057262985, position: 'G', team: 'WAS' },
  'Isaiah Stewart':    { id: 3547267,    position: 'C', team: 'DET' },
  'Isaiah Stewart II': { id: 3547267,    position: 'C', team: 'DET' }, // same player, Odds API uses II suffix
  'Isaiah Joe':       { id: 3547272,    position: 'G', team: 'OKC' },
  'Ron Holland':      { id: 1028026508, position: 'F', team: 'DET' }, // listed as "Ronald Holland II"
  'Kevin Porter Jr.': { id: 666849,     position: 'G', team: 'MIL' },
  'Jalen Johnson':    { id: 17896040,   position: 'F', team: 'ATL' },
  'Cam Thomas':       { id: 17896048,   position: 'G', team: 'MIL' },
  'G.G. Jackson':     { id: 56677830,   position: 'F', team: 'MEM' }, // listed as "GG Jackson"
  'Brandon Williams': { id: 24489167,   position: 'G', team: 'DAL' },
  'Moe Wagner':             { id: 462,          position: 'C', team: 'ORL' }, // listed as "Moritz Wagner"
  'Kasparas Jakucionis':    { id: 1057272939,   position: 'G', team: 'MIA' }, // BDL has special char: Jakučionis
  'Will Richard':           { id: 1057395872,   position: 'G', team: 'GSW' },
  'Kobe Sanders':           { id: 1057396055,   position: 'G', team: 'LAC' },
  'Brook Lopez':            { id: 283,           position: 'C', team: 'LAC' },
  'Derrick Jones':          { id: 247,           position: 'F', team: 'LAC' }, // BDL: "Derrick Jones Jr."
  'Trey Murphy III':        { id: 18677986,      position: 'F', team: 'NOP' },
  'Anthony Edwards':        { id: 3547238,       position: 'G', team: 'MIN' },
  'Ty Jerome':              { id: 666676,        position: 'G', team: 'MEM' },
  'Scoot Henderson':        { id: 56677747,      position: 'G', team: 'POR' },
};

// Search BDL for player ID + position by name, cached 24h
// Returns { id, position } or { id: null, position: null }
async function getBDLPlayerId(name) {
  const ck = `bdl_pid_${name.toLowerCase().replace(/\s+/g, '_')}`;
  const cached = cacheGet(ck);
  if (cached !== undefined) return cached;

  // Check manual overrides first
  const override = BDL_PLAYER_OVERRIDES[name];
  if (override) {
    console.log(`BDL override used: "${name}" → id=${override.id} team=${override.team}`);
    const result = { id: override.id, position: override.position ?? null, team: override.team ?? null, matchedName: name };
    cacheSet(ck, result, 24 * 3600);
    return result;
  }

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

  // Search 2: normalized name (strip suffix Jr/III/etc) — run if no confident match yet
  const normed = normalizeName(name);
  const normedTitle = normed.replace(/\b\w/g, c => c.toUpperCase());
  if (bestScore < 65 && normedTitle !== name) {
    ({ best, bestScore } = updateBest(await trySearch(normedTitle), best, bestScore));
  }

  // Search 3: expand common abbreviations (CJ → C.J., PJ → P.J., AJ → A.J.)
  if (bestScore < 65) {
    const expanded = name.replace(/\b([A-Z]{2,3})\b/g, m => m.split('').join('.') + '.');
    if (expanded !== name) ({ best, bestScore } = updateBest(await trySearch(expanded), best, bestScore));
  }

  // Search 4: first name only (helps with apostrophe variants like Ja'Kobe, and sibling conflicts like Tre/Tyus)
  if (bestScore < 65) {
    const firstName = name.trim().split(' ')[0].replace(/['\u2018\u2019]/g, '');
    ({ best, bestScore } = updateBest(await trySearch(firstName), best, bestScore));
  }

  // Search 5: last name fallback
  if (bestScore < 65) {
    const lastName = name.trim().split(' ').pop();
    ({ best, bestScore } = updateBest(await trySearch(lastName), best, bestScore));
  }

  const resolved = (best && bestScore >= 40) ? best : null;
  const id = resolved?.id ?? null;
  const position = resolved ? normalizeBDLPosition(resolved.position) : null;
  const team = resolved?.team?.abbreviation?.toUpperCase() ?? null;
  const nbaPlayerId = resolved?.nba_player_id ?? null;

  const matchedName = resolved ? `${resolved.first_name} ${resolved.last_name}` : null;
  if (resolved) console.log(`BDL name match: "${name}" → "${matchedName}" team=${team} pos=${resolved.position} nba_id=${nbaPlayerId} (score=${bestScore})`);
  else console.warn(`BDL name lookup failed: "${name}" (best score=${bestScore})`);

  const result = { id, position, team, matchedName, nbaPlayerId };
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
// ROUTE: GET /api/stats/bulk — bulk fetch cached stats for multiple players
// Returns all players found in memory/Supabase cache in one shot.
// Client falls back to individual requests only for missing players.
// ============================================================
app.get('/api/stats/bulk', async (req, res) => {
  const names = (req.query.names || '').split(',').map(n => n.trim()).filter(Boolean);
  if (!names.length) return res.json({ data: {} });

  const result = {};

  // Layer 1: in-memory cache (instant)
  names.forEach(name => {
    const ck = `gl_name_${NBA_SEASON}_${name.toLowerCase().replace(/\s+/g, '_')}`;
    const hit = cacheGet(ck);
    if (hit) result[name] = { data: hit, source: 'memory' };
  });

  // Layer 2: bulk Supabase query for all remaining players in one DB call
  const missing = names.filter(n => !result[n]);
  if (supabase && missing.length) {
    try {
      const { data } = await supabase
        .from('player_stats')
        .select('player_name, game_log, season, last_fetched')
        .in('player_name', missing)
        .eq('season', NBA_SEASON);

      if (data) {
        data.forEach(row => {
          const age = Date.now() - new Date(row.last_fetched).getTime();
          if (age <= 7 * 24 * 60 * 60 * 1000 && row.game_log?.length) {
            const ck = `gl_name_${NBA_SEASON}_${row.player_name.toLowerCase().replace(/\s+/g, '_')}`;
            cacheSet(ck, row.game_log, STATS_TTL);
            result[row.player_name] = { data: row.game_log, source: 'supabase' };
          }
        });
      }
    } catch (e) { console.warn('Bulk Supabase fetch error:', e.message); }
  }

  res.json({ data: result });
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
    // Always look up player ID (cached 24h) so we can return team/position even on cache hit
    const { id: nbaId, position: bdlPos, team: bdlTeam, nbaPlayerId } = await getBDLPlayerId(name);
    if (memHit) return res.json({ data: memHit, cached: true, source: 'memory', team: bdlTeam, position: bdlPos, nbaPlayerId });

    // Layer 2: Supabase persistent cache
    const sbHit = await sbGetGameLog(name);
    if (sbHit) {
      cacheSet(ckMem, sbHit, STATS_TTL);
      return res.json({ data: sbHit, cached: true, source: 'supabase', team: bdlTeam, position: bdlPos, nbaPlayerId });
    }

    // Layer 3: Live BDL fetch
    if (!nbaId) return res.json({ data: [], cached: false, error: `Player not found: ${name}` });
    const games = await fetchBDLGameLog(nbaId);
    // Only cache non-empty results; empty arrays should be retried on next request
    if (games.length) cacheSet(ckMem, games, STATS_TTL);
    await sbSetGameLog(name, nbaId, games, bdlPos); // persist for future requests
    res.json({ data: games, cached: false, source: 'bdl', team: bdlTeam, position: bdlPos, nbaPlayerId });
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
      const team = _playerTeamCache[p.name] || guessTeam(p.name);
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
  // 1. Market Edge (0–40 pts): model prob vs market implied prob
  //    neutral (0% edge) = 20pts, +15% edge = 40pts, -15% edge = 0pts
  const evEdge = (modelProb || 0.5) - (impliedProbOver || 0.5);
  const edgePts = Math.min(40, Math.max(0, evEdge / 0.15 * 20 + 20));
  // 2. Hit Rate (0–40 pts): 25% HR = 0pts, 50% = 20pts, 75% = 40pts
  const hrPts = Math.min(40, Math.max(0, (hitRate - 25) * 0.8));
  // 3. Consistency (0–20 pts): inverse CV
  //    CV ≤ 0.20 = 20pts, CV ≥ 0.60 = 0pts
  const cv = (avg > 0 && stdDev != null) ? stdDev / avg : 0.5;
  const cvPts = Math.min(20, Math.max(0, (0.6 - Math.min(cv, 0.6)) / 0.4 * 20));
  return Math.max(0, Math.min(100, Math.round(edgePts + hrPts + cvPts)));
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
    'jayson tatum':'BOS','jaylen brown':'BOS','jrue holiday':'BOS','al horford':'BOS','kristaps porzingis':'GSW','payton pritchard':'BOS','sam hauser':'BOS','derrick white':'BOS',
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
    'kawhi leonard':'LAC','james harden':'LAC','ivica zubac':'IND','norman powell':'LAC','terance mann':'LAC','bones hyland':'LAC',
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
  const secret = (req.headers.authorization || '').replace('Bearer ', '') || req.headers['x-cron-secret'] || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!supabase) return res.status(503).json({ error: 'Supabase not configured' });
  if (!BDL_KEY) return res.status(503).json({ error: 'BDL key not configured' });

  const results = { ok: [], failed: [], skipped: [], newPlayers: [] };
  try {
    // Get player names from Supabase odds_cache (populated by refresh-odds cron)
    const oddsPlayers = await sbGetOdds('combined');
    if (!oddsPlayers || !oddsPlayers.length) {
      return res.status(503).json({ error: 'No odds data in Supabase — run refresh-odds first' });
    }
    const playerNames = [...new Set(oddsPlayers.map(p => p.name).filter(Boolean))];

    // Batch-read existing records (metadata only — no game_log to keep query fast)
    const { data: existingRecords } = await supabase
      .from('player_stats')
      .select('player_name, bdl_id, position, last_fetched, season')
      .eq('season', NBA_SEASON)
      .in('player_name', playerNames);
    const recordMap = new Map((existingRecords || []).map(r => [r.player_name, r]));

    // Split players: incremental (have data) vs full fetch (new)
    const now = new Date();
    const incremental = [];
    const fullFetch = [];

    for (const name of playerNames) {
      const rec = recordMap.get(name);
      if (rec?.bdl_id) {
        // Skip if already updated within last 12 hours
        if (rec.last_fetched && (now - new Date(rec.last_fetched)) < 12 * 60 * 60 * 1000) {
          results.skipped.push(name);
          continue;
        }
        incremental.push({ name, bdlId: rec.bdl_id, position: rec.position });
      } else {
        fullFetch.push(name);
      }
    }

    // Helper: process a batch of promises with concurrency limit
    async function processBatch(items, fn, concurrency) {
      for (let i = 0; i < items.length; i += concurrency) {
        await Promise.all(items.slice(i, i + concurrency).map(fn));
      }
    }

    // Process incremental updates — 5 concurrent
    // Each fetches only last 3 days of games, then merges with stored data
    await processBatch(incremental, async ({ name, bdlId, position }) => {
      try {
        // Fetch existing game_log for this player
        const record = await sbGetGameLogRecord(name);
        const existingGames = record?.game_log || [];

        const lastGameDate = existingGames.map(g => g.date).filter(Boolean).sort().pop();
        const fetchFrom = lastGameDate ? new Date(lastGameDate) : new Date(NBA_SEASON + '-10-01');
        fetchFrom.setDate(fetchFrom.getDate() - 3);
        const startDate = fetchFrom.toISOString().split('T')[0];

        const newGames = await fetchBDLGameLog(bdlId, startDate);
        const byDate = new Map(existingGames.map(g => [g.date, g]));
        newGames.forEach(g => byDate.set(g.date, g));
        const merged = [...byDate.values()].sort((a, b) => new Date(b.date) - new Date(a.date));
        await sbSetGameLog(name, bdlId, merged, position);
        // Cache team from most recent game
        const latestTeam = merged[0]?.team;
        if (latestTeam) _playerTeamCache[name] = latestTeam;
        results.ok.push(`${name} (+${newGames.length})`);
      } catch (e) {
        results.failed.push(`${name} (${e.message})`);
      }
    }, 5);

    // Process new players — 3 concurrent, cap at 30 per run
    const newBatch = fullFetch.slice(0, 30);
    await processBatch(newBatch, async (name) => {
      try {
        const { id: bdlId, position: bdlPos } = await getBDLPlayerId(name);
        if (!bdlId) { results.failed.push(`${name} (not found in BDL)`); return; }
        const games = await fetchBDLGameLog(bdlId);
        await sbSetGameLog(name, bdlId, games, bdlPos);
        const latestTeam = games.sort((a, b) => new Date(b.date) - new Date(a.date))[0]?.team;
        if (latestTeam) _playerTeamCache[name] = latestTeam;
        results.newPlayers.push(`${name} (${games.length} games)`);
      } catch (e) {
        results.failed.push(`${name} (${e.message})`);
      }
    }, 3);
    if (fullFetch.length > 30) {
      results.deferred = fullFetch.length - 30;
    }

    res.json({ success: true, total: playerNames.length, incremental: incremental.length, newFetched: newBatch.length, skipped: results.skipped.length, ...results });
  } catch (err) {
    res.status(500).json({ error: err.message, ...results });
  }
});

// ============================================================
// ROUTE: GET /api/cron/grade-bets — auto-grade open bets using stored game logs
// Runs nightly after games finish. For each user with open bets whose
// gameTime is >4h in the past, looks up the actual stat in player_stats
// and marks the bet won/lost/void, updating balance accordingly.
// ============================================================
function getEtDate(isoString) {
  // Returns YYYY-MM-DD in Eastern Time (matches BDL game_log date format)
  return new Date(isoString).toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
}

function getStatValue(game, market) {
  switch (market) {
    case 'player_points':                    return game.pts  ?? null;
    case 'player_rebounds':                  return game.reb  ?? null;
    case 'player_assists':                   return game.ast  ?? null;
    case 'player_threes':                    return game.fg3m ?? null;
    case 'player_steals':                    return game.stl  ?? null;
    case 'player_blocks':                    return game.blk  ?? null;
    case 'player_turnovers':                 return game.turnover ?? null;
    case 'player_points_rebounds_assists':   return (game.pts||0)+(game.reb||0)+(game.ast||0);
    default: return null;
  }
}

function gradeSingleOutcome(actual, line, direction) {
  if (direction === 'over')  return actual >= line ? 'won' : 'lost';
  if (direction === 'under') return actual <  line ? 'won' : 'lost';
  return 'void';
}

// Looks up a player's actual stat for a given ET game date from Supabase
async function getActualStat(playerName, gameDate, market) {
  const record = await sbGetGameLogRecord(playerName);
  if (!record?.game_log?.length) return { status: 'missing' };
  const game = record.game_log.find(g => g.date === gameDate);
  if (!game) return { status: 'missing' }; // stats not in yet
  const minPlayed = parseInt(game.min || '0', 10);
  if (minPlayed === 0) return { status: 'dnp' };
  const val = getStatValue(game, market);
  if (val === null) return { status: 'missing' };
  return { status: 'ok', value: val };
}

app.get('/api/cron/grade-bets', async (req, res) => {
  const secret = (req.headers.authorization || '').replace('Bearer ', '') || req.headers['x-cron-secret'] || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!supabase) return res.status(503).json({ error: 'Supabase not configured' });

  const now = Date.now();
  const GRADE_AFTER_MS = 10 * 60 * 60 * 1000; // grade bets whose gameTime is >10h ago
  const summary = { usersProcessed: 0, betsGraded: 0, betsSkipped: 0, errors: [] };

  try {
    // Fetch all user profiles that have at least one bet
    const { data: profiles, error: fetchErr } = await supabase
      .from('user_profiles')
      .select('id, balance, bets');
    if (fetchErr) return res.status(500).json({ error: fetchErr.message });

    for (const profile of profiles || []) {
      const bets = profile.bets || [];
      const openBets = bets.filter(b => b.status === 'open' && b.gameTime &&
        (now - new Date(b.gameTime).getTime()) > GRADE_AFTER_MS);
      if (!openBets.length) continue;

      let balance = profile.balance ?? 1000;
      let changed = false;

      for (const bet of openBets) {
        const gameDate = getEtDate(bet.gameTime);
        try {
          if (bet.type === 'parlay') {
            // Grade parlay: all legs must win; any DNP voids the parlay
            let parlayResult = 'won';
            let anyMissing = false;
            for (const leg of (bet.legs || [])) {
              const { status, value } = await getActualStat(leg.name, gameDate, leg.market);
              if (status === 'missing') { anyMissing = true; break; }
              if (status === 'dnp')    { parlayResult = 'void'; break; }
              const legResult = gradeSingleOutcome(value, leg.line, leg.direction);
              if (legResult === 'lost') { parlayResult = 'lost'; break; }
            }
            if (anyMissing) { summary.betsSkipped++; continue; }
            bet.status    = parlayResult;
            bet.settledAt = new Date().toISOString();
            if (parlayResult === 'won')  { bet.pnl = +bet.toWin;   balance += bet.stake + bet.toWin; }
            else if (parlayResult === 'lost') { bet.pnl = -bet.stake; }
            else                         { bet.pnl = 0; balance += bet.stake; } // void refund
          } else {
            // Grade single bet
            const { status, value } = await getActualStat(bet.player, gameDate, bet.market);
            if (status === 'missing') { summary.betsSkipped++; continue; }
            const outcome = status === 'dnp' ? 'void' : gradeSingleOutcome(value, bet.line, bet.direction);
            bet.status       = outcome;
            bet.settledAt    = new Date().toISOString();
            bet.actualResult = status === 'dnp' ? 'DNP' : value;
            if (outcome === 'won')  { bet.pnl = +bet.toWin;   balance += bet.stake + bet.toWin; }
            else if (outcome === 'lost') { bet.pnl = -bet.stake; }
            else                    { bet.pnl = 0; balance += bet.stake; } // void refund
          }
          changed = true;
          summary.betsGraded++;
        } catch (e) {
          summary.errors.push(`${bet.player || 'parlay'}: ${e.message}`);
          summary.betsSkipped++;
        }
      }

      if (changed) {
        summary.usersProcessed++;
        const { error: writeErr } = await supabase
          .from('user_profiles')
          .update({ bets, balance, updated_at: new Date().toISOString() })
          .eq('id', profile.id);
        if (writeErr) summary.errors.push(`profile ${profile.id}: ${writeErr.message}`);
      }
    }

    res.json({ success: true, ...summary });
  } catch (err) {
    res.status(500).json({ error: err.message, ...summary });
  }
});

// ============================================================
// ROUTE: GET /api/cron/refresh-odds — store Odds API data in Supabase
// Runs on schedule so users never hit the Odds API directly
// ============================================================
app.get('/api/cron/refresh-odds', async (req, res) => {
  const secret = (req.headers.authorization || '').replace('Bearer ', '') || req.headers['x-cron-secret'] || req.query.secret;
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
        // Preserve previous lines for movement tracking
        const prevPlayers = await sbGetOdds(book);
        if (prevPlayers && prevPlayers.length) {
          const prevMap = {};
          for (const pp of prevPlayers) {
            prevMap[`${pp.name}|${pp.market}`] = pp;
          }
          for (const p of players) {
            const prev = prevMap[`${p.name}|${p.market}`];
            if (prev) {
              // Use the previous snapshot's own prev data if line hasn't changed since then
              // Otherwise record the previous line as the movement reference
              if (prev.line !== p.line || prev.overOdds !== p.overOdds || prev.underOdds !== p.underOdds) {
                p.prev_line = prev.line;
                p.prev_overOdds = prev.overOdds;
                p.prev_underOdds = prev.underOdds;
              } else if (prev.prev_line != null) {
                // Line didn't change this refresh — carry forward the previous movement data
                p.prev_line = prev.prev_line;
                p.prev_overOdds = prev.prev_overOdds;
                p.prev_underOdds = prev.prev_underOdds;
              }
            }
          }
        }
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
    cache.del(ck);
    const { id, position, team, matchedName } = await getBDLPlayerId(name);
    if (!id) return res.json({ name, found: false, error: 'Player not found in BDL' });
    const games = await fetchBDLGameLog(id);
    res.json({
      name, found: true, bdlId: id, matchedName, team, position,
      season: NBA_SEASON, gamesFound: games.length,
    });
  } catch (err) {
    res.json({ name, error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/debug/bdl-search/:query — show raw BDL search results + scores
// ============================================================
app.get('/api/debug/bdl-search/:query', async (req, res) => {
  const query = decodeURIComponent(req.params.query);
  try {
    const perPage = parseInt(req.query.per_page) || 25;
    const resp = await fetch(`${BDL_BASE}/players?search=${encodeURIComponent(query)}&per_page=${perPage}`, { headers: bdlHeaders() });
    if (!resp.ok) return res.json({ query, error: `BDL returned ${resp.status}` });
    const data = await resp.json();
    const players = (data.data || []).map(p => ({
      id: p.id,
      name: `${p.first_name} ${p.last_name}`,
      team: p.team?.abbreviation ?? null,
      position: p.position,
      score: nameMatchScore(p, query),
    }));
    res.json({ query, results: players });
  } catch (err) {
    res.json({ query, error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/debug/clear-player/:name — wipe cached data so next fetch re-resolves
// ============================================================
app.get('/api/debug/clear-player/:name', async (req, res) => {
  const name = decodeURIComponent(req.params.name);
  const pidKey = `bdl_pid_${name.toLowerCase().replace(/\s+/g, '_')}`;
  const glKey  = `gl_name_${NBA_SEASON}_${name.toLowerCase().replace(/\s+/g, '_')}`;
  cache.del(pidKey);
  cache.del(glKey);
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
// ROUTE: GET /api/player/nba-id/:name — return ESPN athlete ID from static map
// Used by frontend throttled prefetch queue to resolve headshots
// ============================================================
app.get('/api/player/nba-id/:name', (req, res) => {
  const name = decodeURIComponent(req.params.name);
  const espnId = ESPN_PLAYERS[name] || null;
  res.json({ nbaPlayerId: espnId });
});

// ============================================================
// ROUTE: GET /api/headshot/name/:name — resolve player name → ESPN headshot
// Must be registered BEFORE /api/headshot/:id so "name" isn't treated as an ID
// ============================================================
app.get('/api/headshot/name/:name', async (req, res) => {
  const name = decodeURIComponent(req.params.name);
  try {
    const espnId = ESPN_PLAYERS[name];
    if (!espnId) return res.status(404).end();
    const resp = await fetch(`https://a.espncdn.com/i/headshots/nba/players/full/${espnId}.png`);
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
// ROUTE: GET /api/headshot/:id — proxy ESPN CDN to avoid CORS
// ============================================================
app.get('/api/headshot/:id', async (req, res) => {
  try {
    const resp = await fetch(`https://a.espncdn.com/i/headshots/nba/players/full/${req.params.id}.png`);
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
    const { data: profile, error: profileErr } = await supabase
      .from('user_profiles')
      .select('*')
      .eq('id', user.id)
      .single();
    // PGRST116 = no rows found (genuine first login). Any other error = DB/schema issue.
    // Return 500 on real errors so the client never overwrites good data with defaults.
    if (profileErr && profileErr.code !== 'PGRST116') {
      console.error('Profile fetch error:', profileErr);
      return res.status(500).json({ error: 'Profile temporarily unavailable, please retry' });
    }
    if (!profile) {
      // Genuine first login — create default profile
      const def = { id: user.id, balance: 1000, bets: [], preferences: {} };
      await supabase.from('user_profiles').insert(def);
      return res.json({ balance: 1000, bets: [], preferences: {}, displayName: user.email.split('@')[0], isNew: true });
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

// ---- SOCIAL ROUTES ----
// Run this SQL in your Supabase SQL Editor before using social features:
//   ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS username TEXT UNIQUE;
//   CREATE TABLE IF NOT EXISTS friendships (
//     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//     requester_id UUID NOT NULL REFERENCES user_profiles(id) ON DELETE CASCADE,
//     addressee_id UUID NOT NULL REFERENCES user_profiles(id) ON DELETE CASCADE,
//     status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','accepted','declined')),
//     created_at TIMESTAMPTZ DEFAULT now(),
//     UNIQUE(requester_id, addressee_id)
//   );
//   CREATE TABLE IF NOT EXISTS conversations (
//     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//     name TEXT,
//     type TEXT NOT NULL DEFAULT 'dm' CHECK (type IN ('dm','group')),
//     created_by UUID REFERENCES user_profiles(id) ON DELETE SET NULL,
//     created_at TIMESTAMPTZ DEFAULT now()
//   );
//   CREATE TABLE IF NOT EXISTS conversation_members (
//     conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
//     user_id UUID NOT NULL REFERENCES user_profiles(id) ON DELETE CASCADE,
//     last_read_at TIMESTAMPTZ DEFAULT now(),
//     joined_at TIMESTAMPTZ DEFAULT now(),
//     PRIMARY KEY (conversation_id, user_id)
//   );
//   CREATE TABLE IF NOT EXISTS messages (
//     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//     conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
//     sender_id UUID NOT NULL REFERENCES user_profiles(id) ON DELETE CASCADE,
//     content TEXT,
//     type TEXT NOT NULL DEFAULT 'text' CHECK (type IN ('text','line','bet')),
//     line_data JSONB,
//     created_at TIMESTAMPTZ DEFAULT now()
//   );
//   CREATE INDEX IF NOT EXISTS messages_conv_idx ON messages(conversation_id, created_at DESC);
//   CREATE INDEX IF NOT EXISTS conv_members_user_idx ON conversation_members(user_id);
//   CREATE INDEX IF NOT EXISTS friendships_addressee_idx ON friendships(addressee_id);

// Fetch user profile rows by IDs (helper)
async function socProfiles(ids) {
  if (!ids?.length) return {};
  const { data } = await supabase.from('user_profiles').select('id, username, display_name').in('id', [...new Set(ids)]);
  const map = {};
  (data || []).forEach(p => { map[p.id] = { username: p.username, displayName: p.display_name || p.username || 'Unknown' }; });
  return map;
}

app.get('/api/social/me', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const { data } = await supabase.from('user_profiles').select('username, display_name').eq('id', user.id).single();
    res.json({ userId: user.id, username: data?.username || null, displayName: data?.display_name || user.email?.split('@')[0] });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/social/username', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const { username } = req.body || {};
    if (!username || !/^[a-zA-Z0-9_]{3,20}$/.test(username))
      return res.status(400).json({ error: 'Username must be 3–20 characters (letters, numbers, underscores only)' });
    const { data: existing } = await supabase.from('user_profiles').select('id').ilike('username', username).neq('id', user.id).limit(1);
    if (existing?.length) return res.status(409).json({ error: 'Username already taken' });
    await supabase.from('user_profiles').upsert({ id: user.id, username, updated_at: new Date().toISOString() }, { onConflict: 'id' });
    res.json({ ok: true, username });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/social/search', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const q = (req.query.q || '').trim();
    if (q.length < 2) return res.json([]);
    const { data } = await supabase.from('user_profiles').select('id, username, display_name').ilike('username', `%${q}%`).neq('id', user.id).limit(10);
    res.json((data || []).map(u => ({ id: u.id, username: u.username, displayName: u.display_name || u.username })));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/social/friends', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const [{ data: sent }, { data: received }] = await Promise.all([
      supabase.from('friendships').select('id, addressee_id, status').eq('requester_id', user.id),
      supabase.from('friendships').select('id, requester_id, status').eq('addressee_id', user.id),
    ]);
    const ids = [...(sent || []).map(f => f.addressee_id), ...(received || []).map(f => f.requester_id)];
    const pm = await socProfiles(ids);
    res.json({
      sent: (sent || []).map(f => ({ id: f.id, status: f.status, userId: f.addressee_id, ...pm[f.addressee_id] })),
      received: (received || []).map(f => ({ id: f.id, status: f.status, userId: f.requester_id, ...pm[f.requester_id] })),
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/social/friends/request', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const { toId } = req.body || {};
    if (!toId || toId === user.id) return res.status(400).json({ error: 'Invalid user' });
    const { data: existing } = await supabase.from('friendships').select('id, status')
      .or(`and(requester_id.eq.${user.id},addressee_id.eq.${toId}),and(requester_id.eq.${toId},addressee_id.eq.${user.id})`).limit(1);
    if (existing?.length) return res.status(409).json({ error: existing[0].status === 'accepted' ? 'Already friends' : 'Request already exists' });
    const { data } = await supabase.from('friendships').insert({ requester_id: user.id, addressee_id: toId, status: 'pending' }).select().single();
    res.json({ ok: true, friendship: data });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/social/friends/:id', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const { action } = req.body || {};
    if (!['accept', 'decline'].includes(action)) return res.status(400).json({ error: 'Invalid action' });
    const { data } = await supabase.from('friendships')
      .update({ status: action === 'accept' ? 'accepted' : 'declined' })
      .eq('id', req.params.id).eq('addressee_id', user.id).select().single();
    if (!data) return res.status(404).json({ error: 'Not found' });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/social/friends/:id', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    await supabase.from('friendships').delete().eq('id', req.params.id)
      .or(`requester_id.eq.${user.id},addressee_id.eq.${user.id}`);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/social/conversations', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const { data: memberships } = await supabase.from('conversation_members').select('conversation_id, last_read_at').eq('user_id', user.id);
    if (!memberships?.length) return res.json([]);
    const convIds = memberships.map(m => m.conversation_id);
    const [{ data: convs }, { data: allMembers }, { data: lastMsgs }] = await Promise.all([
      supabase.from('conversations').select('id, name, type, created_at').in('id', convIds),
      supabase.from('conversation_members').select('conversation_id, user_id').in('conversation_id', convIds),
      supabase.from('messages').select('id, conversation_id, sender_id, content, type, created_at').in('conversation_id', convIds).order('created_at', { ascending: false }).limit(convIds.length * 3),
    ]);
    const pm = await socProfiles((allMembers || []).map(m => m.user_id));
    const lastMsgMap = {};
    (lastMsgs || []).forEach(m => { if (!lastMsgMap[m.conversation_id]) lastMsgMap[m.conversation_id] = m; });
    const membersMap = {};
    (allMembers || []).forEach(m => { if (!membersMap[m.conversation_id]) membersMap[m.conversation_id] = []; membersMap[m.conversation_id].push({ userId: m.user_id, ...pm[m.user_id] }); });
    const readMap = {};
    memberships.forEach(m => { readMap[m.conversation_id] = m.last_read_at; });
    const result = (convs || []).map(conv => {
      const lastMsg = lastMsgMap[conv.id];
      const hasUnread = lastMsg && lastMsg.sender_id !== user.id && readMap[conv.id]
        ? new Date(lastMsg.created_at) > new Date(readMap[conv.id]) : false;
      return { id: conv.id, name: conv.name, type: conv.type, createdAt: conv.created_at, members: membersMap[conv.id] || [], lastMessage: lastMsg || null, hasUnread };
    });
    result.sort((a, b) => new Date(b.lastMessage?.created_at || b.createdAt) - new Date(a.lastMessage?.created_at || a.createdAt));
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/social/conversations', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const { type = 'dm', name, memberIds = [] } = req.body || {};
    const allIds = [...new Set([user.id, ...memberIds])];
    if (type === 'dm' && allIds.length === 2) {
      const otherId = allIds.find(id => id !== user.id);
      const { data: myMems } = await supabase.from('conversation_members').select('conversation_id').eq('user_id', user.id);
      if (myMems?.length) {
        const { data: otherMems } = await supabase.from('conversation_members').select('conversation_id').eq('user_id', otherId).in('conversation_id', myMems.map(m => m.conversation_id));
        if (otherMems?.length) {
          const { data: dmConvs } = await supabase.from('conversations').select('id').eq('type', 'dm').in('id', otherMems.map(m => m.conversation_id)).limit(1);
          if (dmConvs?.length) return res.json({ id: dmConvs[0].id, existing: true });
        }
      }
    }
    const { data: conv } = await supabase.from('conversations').insert({ type, name: name || null, created_by: user.id }).select().single();
    await supabase.from('conversation_members').insert(allIds.map(uid => ({ conversation_id: conv.id, user_id: uid })));
    res.json({ id: conv.id, existing: false });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/social/conversations/:id/messages', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const { data: mem } = await supabase.from('conversation_members').select('user_id').eq('conversation_id', req.params.id).eq('user_id', user.id).single();
    if (!mem) return res.status(403).json({ error: 'Not a member' });
    let query = supabase.from('messages').select('id, conversation_id, sender_id, content, type, line_data, created_at').eq('conversation_id', req.params.id);
    let ascending = false;
    if (req.query.after) {
      // Poll for new messages only — ascending, no limit
      query = query.gt('created_at', req.query.after).order('created_at', { ascending: true });
      ascending = true;
    } else {
      query = query.order('created_at', { ascending: false }).limit(50);
      if (req.query.before) query = query.lt('created_at', req.query.before);
    }
    const { data: msgs } = await query;
    const pm = await socProfiles((msgs || []).map(m => m.sender_id));
    if (!req.query.after) await supabase.from('conversation_members').update({ last_read_at: new Date().toISOString() }).eq('conversation_id', req.params.id).eq('user_id', user.id);
    const sorted = ascending ? (msgs || []) : (msgs || []).reverse();
    res.json(sorted.map(m => ({ ...m, sender: pm[m.sender_id] || {} })));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/social/conversations/:id/messages', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const { data: mem } = await supabase.from('conversation_members').select('user_id').eq('conversation_id', req.params.id).eq('user_id', user.id).single();
    if (!mem) return res.status(403).json({ error: 'Not a member' });
    const { content, type = 'text', lineData, betData } = req.body || {};
    if (!content?.trim() && !lineData && !betData) return res.status(400).json({ error: 'Empty message' });
    const msg = { conversation_id: req.params.id, sender_id: user.id, type };
    if (content?.trim()) msg.content = content.trim();
    if (lineData || betData) msg.line_data = lineData || betData;
    const { data } = await supabase.from('messages').insert(msg).select('id, conversation_id, sender_id, content, type, line_data, created_at').single();
    const pm = await socProfiles([user.id]);
    res.json({ ...data, sender: pm[user.id] || {} });

    // Send push notifications to other conversation members (non-blocking)
    (async () => {
      try {
        const { data: members } = await supabase.from('conversation_members').select('user_id').eq('conversation_id', req.params.id).neq('user_id', user.id);
        if (!members?.length) return;
        const senderName = pm[user.id]?.displayName || pm[user.id]?.username || 'Someone';
        const MKT_SHORT = { player_points:'Pts', player_rebounds:'Reb', player_assists:'Ast', player_threes:'3PM', player_steals:'Stl', player_blocks:'Blk', player_turnovers:'TO', player_points_rebounds_assists:'PRA' };
        const fmtOdds = o => o != null ? (o > 0 ? '+' : '') + o : null;
        const fmtEV = v => v != null ? (v >= 0 ? '+' : '') + Number(v).toFixed(1) + '%' : null;
        const fmtProb = v => v != null ? Math.round(v * 100) + '%' : null;
        let pushTitle = '';
        let pushBody = '';
        if (type === 'line' && lineData) {
          const ld = lineData;
          const isOver = (ld.edge || 0) >= 0;
          const dir = isOver ? '▲ OVER' : '▼ UNDER';
          const mkt = MKT_SHORT[ld.market] || ld.market || '';
          const odds = fmtOdds(isOver ? ld.overOdds : ld.underOdds);
          const ev = fmtEV(ld.dirEV);
          const prob = fmtProb(ld.modelProb != null ? (isOver ? ld.modelProb : 1 - ld.modelProb) : null);
          const gameTimeFmt = ld.gameTime ? (() => { try { return new Date(ld.gameTime).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/Los_Angeles' }) + ' PT'; } catch { return null; } })() : null;
          pushTitle = `${senderName} shared a line`;
          pushBody = [
            `${ld.name || 'Player'} ${dir} ${ld.line ?? ''} ${mkt}`,
            [odds, ev ? `EV ${ev}` : null, prob ? `Model ${prob}` : null].filter(Boolean).join(' · '),
            gameTimeFmt ? `🕐 ${gameTimeFmt}` : null,
          ].filter(Boolean).join('\n');
        } else if (type === 'bet' && betData) {
          const bd = betData;
          if (bd.type === 'parlay') {
            const legs = (bd.legs || []).slice(0, 3).map(l => `${l.name} ${l.direction === 'over' ? '▲' : '▼'} ${l.line}`).join(', ');
            pushTitle = `${senderName} shared a parlay`;
            pushBody = `${bd.legs?.length || 0}-Leg · ${bd.combinedOdds || ''}\n${legs}`;
          } else {
            if (bd.status && bd.status !== 'open') return;
            const dir = bd.direction === 'over' ? '▲ OVER' : '▼ UNDER';
            const mktB = MKT_SHORT[bd.market] || bd.marketLabel || '';
            const odds = fmtOdds(bd.odds);
            const ev = fmtEV(bd.evParlay ?? (bd.edge != null ? bd.edge : null));
            const prob = fmtProb(bd.modelProb);
            pushTitle = `${senderName} shared a bet`;
            pushBody = [
              `${bd.player || 'Player'} ${dir} ${bd.line ?? ''} ${mktB}`.trim(),
              [odds, ev ? `EV ${ev}` : null, prob ? `Model ${prob}` : null].filter(Boolean).join(' · ')
            ].filter(Boolean).join('\n');
          }
        } else {
          pushTitle = `${senderName} sent a message`;
          pushBody = content?.trim()?.substring(0, 120) || 'New message';
        }
        const payload = {
          title: pushTitle,
          body: pushBody,
          tag: `conv-${req.params.id}`,
          url: `/?tab=social&conv=${req.params.id}`,
          actions: [
            { action: 'react_yes', title: '✅ Like' },
            { action: 'react_maybe', title: '❓ Maybe' },
            { action: 'react_no', title: '❌ Pass' },
          ],
          messageId: data.id,
          conversationId: req.params.id,
        };
        await Promise.allSettled(members.map(m => sendPushToUser(m.user_id, payload)));
      } catch {}
    })();
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/social/conversations/:id', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    await supabase.from('conversation_members').delete().eq('conversation_id', req.params.id).eq('user_id', user.id);
    const { count } = await supabase.from('conversation_members').select('user_id', { count: 'exact', head: true }).eq('conversation_id', req.params.id);
    if (!count) await supabase.from('conversations').delete().eq('id', req.params.id);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/social/unread', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const { data: memberships } = await supabase.from('conversation_members').select('conversation_id, last_read_at').eq('user_id', user.id);
    let msgUnread = 0;
    if (memberships?.length) {
      const convIds = memberships.map(m => m.conversation_id);
      const { data: recentMsgs } = await supabase.from('messages').select('conversation_id, created_at, sender_id').in('conversation_id', convIds).neq('sender_id', user.id).order('created_at', { ascending: false });
      const readMap = {};
      memberships.forEach(m => { readMap[m.conversation_id] = m.last_read_at; });
      const counted = new Set();
      (recentMsgs || []).forEach(msg => {
        if (counted.has(msg.conversation_id)) return;
        if (!readMap[msg.conversation_id] || new Date(msg.created_at) > new Date(readMap[msg.conversation_id])) { msgUnread++; counted.add(msg.conversation_id); }
      });
    }
    const { count: pendingRequests } = await supabase.from('friendships').select('id', { count: 'exact', head: true }).eq('addressee_id', user.id).eq('status', 'pending');
    res.json({ count: msgUnread, pendingRequests: pendingRequests || 0 });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ============================================================
// MESSAGE REACTIONS
// ============================================================
// Schema (run in Supabase SQL editor):
//   CREATE TABLE IF NOT EXISTS message_reactions (
//     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//     message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
//     user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
//     emoji TEXT NOT NULL,
//     created_at TIMESTAMPTZ DEFAULT now(),
//     UNIQUE(message_id, user_id, emoji)
//   );
//   CREATE INDEX idx_reactions_message ON message_reactions(message_id);

// Get reactions for messages in a conversation (batch)
app.get('/api/social/reactions/:conversationId', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const { data: mem } = await supabase.from('conversation_members').select('user_id').eq('conversation_id', req.params.conversationId).eq('user_id', user.id).single();
    if (!mem) return res.status(403).json({ error: 'Not a member' });
    const { data } = await supabase.from('message_reactions').select('id, message_id, user_id, emoji, created_at')
      .in('message_id', (await supabase.from('messages').select('id').eq('conversation_id', req.params.conversationId).limit(200)).data?.map(m => m.id) || []);
    res.json(data || []);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Add reaction to a message
app.post('/api/social/reactions/:messageId', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const { emoji } = req.body || {};
  if (!emoji) return res.status(400).json({ error: 'Missing emoji' });
  try {
    // Verify user is member of the conversation this message belongs to
    const { data: msg } = await supabase.from('messages').select('conversation_id').eq('id', req.params.messageId).single();
    if (!msg) return res.status(404).json({ error: 'Message not found' });
    const { data: mem } = await supabase.from('conversation_members').select('user_id').eq('conversation_id', msg.conversation_id).eq('user_id', user.id).single();
    if (!mem) return res.status(403).json({ error: 'Not a member' });
    const { data } = await supabase.from('message_reactions').upsert({
      message_id: req.params.messageId, user_id: user.id, emoji,
    }, { onConflict: 'message_id,user_id,emoji' }).select('id, message_id, user_id, emoji, created_at').single();
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Remove reaction from a message
app.delete('/api/social/reactions/:messageId/:emoji', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    await supabase.from('message_reactions').delete()
      .eq('message_id', req.params.messageId).eq('user_id', user.id).eq('emoji', decodeURIComponent(req.params.emoji));
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ============================================================
// WEB PUSH NOTIFICATIONS
// ============================================================
// Schema (run in Supabase SQL editor):
//   CREATE TABLE IF NOT EXISTS push_subscriptions (
//     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//     user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
//     subscription JSONB NOT NULL,
//     created_at TIMESTAMPTZ DEFAULT now(),
//     UNIQUE(user_id, subscription)
//   );

// Subscribe to push notifications
app.post('/api/push/subscribe', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const { subscription } = req.body;
  if (!subscription?.endpoint) return res.status(400).json({ error: 'Invalid subscription' });
  try {
    // Upsert — same endpoint = update, new endpoint = insert
    await supabase.from('push_subscriptions').upsert(
      { user_id: user.id, subscription },
      { onConflict: 'user_id,subscription' }
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Unsubscribe
app.post('/api/push/unsubscribe', async (req, res) => {
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const user = await authUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const { endpoint } = req.body;
  try {
    if (endpoint) {
      await supabase.from('push_subscriptions').delete().eq('user_id', user.id).filter('subscription->>endpoint', 'eq', endpoint);
    } else {
      await supabase.from('push_subscriptions').delete().eq('user_id', user.id);
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Get VAPID public key (client needs this to subscribe)
app.get('/api/push/vapid-key', (req, res) => {
  res.json({ key: VAPID_PUBLIC });
});

// Helper: send push notification to a user
async function sendPushToUser(userId, payload) {
  if (!VAPID_PUBLIC || !VAPID_PRIVATE || !supabase) return;
  try {
    const { data: subs } = await supabase.from('push_subscriptions').select('id, subscription').eq('user_id', userId);
    if (!subs?.length) return;
    const body = JSON.stringify(payload);
    const results = await Promise.allSettled(
      subs.map(s => webpush.sendNotification(s.subscription, body).catch(async err => {
        // Remove expired/invalid subscriptions (410 Gone or 404)
        if (err.statusCode === 410 || err.statusCode === 404) {
          await supabase.from('push_subscriptions').delete().eq('id', s.id);
        }
        throw err;
      }))
    );
    return results.filter(r => r.status === 'fulfilled').length;
  } catch {}
}

// ============================================================
// AGENT BETTING SYSTEM — Automated value alert validation
// ============================================================
// Schema (run in Supabase SQL editor):
//   CREATE TABLE IF NOT EXISTS agent_bets (
//     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//     bet_date DATE NOT NULL,
//     player_name TEXT NOT NULL,
//     team TEXT,
//     matchup TEXT,
//     market TEXT NOT NULL,
//     market_label TEXT,
//     line NUMERIC NOT NULL,
//     direction TEXT NOT NULL CHECK (direction IN ('over','under')),
//     odds INTEGER NOT NULL,
//     model_prob NUMERIC,
//     confidence NUMERIC,
//     edge NUMERIC,
//     ev NUMERIC,
//     value_score INTEGER,
//     hit_rate NUMERIC,
//     is_control BOOLEAN DEFAULT false,
//     stake NUMERIC DEFAULT 10,
//     status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','won','lost','void','dnp')),
//     actual_result NUMERIC,
//     pnl NUMERIC,
//     game_time TIMESTAMPTZ,
//     graded_at TIMESTAMPTZ,
//     created_at TIMESTAMPTZ DEFAULT now()
//   );
//   CREATE INDEX IF NOT EXISTS idx_agent_bets_date ON agent_bets(bet_date);
//   CREATE INDEX IF NOT EXISTS idx_agent_bets_status ON agent_bets(status);

const { Resend } = require('resend');
const resend = process.env.RESEND_API_KEY ? new Resend(process.env.RESEND_API_KEY) : null;
const NEWSLETTER_TO = process.env.NEWSLETTER_EMAIL || '';
const NEWSLETTER_FROM = process.env.NEWSLETTER_FROM || 'Value Alert Agent <agent@propedge.app>';
const DISCORD_PICKS_WEBHOOK = process.env.DISCORD_PICKS_WEBHOOK || '';
const DISCORD_RESULTS_WEBHOOK = process.env.DISCORD_RESULTS_WEBHOOK || '';

// ---- Discord webhook helpers ----
async function postDiscord(webhookUrl, payload) {
  if (!webhookUrl) return;
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    clearTimeout(timeout);
  } catch (e) { console.error('Discord webhook error:', e.message); }
}

async function sendDiscordPicks(betsInserted, date) {
  if (!DISCORD_PICKS_WEBHOOK || !betsInserted.length) return;

  const valueBets = betsInserted.filter(b => !b.is_control).sort((a, b) => b.value_score - a.value_score);
  const controlBets = betsInserted.filter(b => b.is_control);
  const fmtOdds = o => o > 0 ? `+${o}` : `${o}`;
  const dateStr = new Date(date + 'T12:00:00').toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });

  // Main summary embed
  const embeds = [{
    title: `🏀 Value Alert Agent — ${dateStr}`,
    description: `Placed **${valueBets.length} value picks** and **${controlBets.length} control picks** for today's slate.`,
    color: 0x6366f1,
    fields: [
      { name: 'Total Picks', value: `${betsInserted.length}`, inline: true },
      { name: 'Value Alerts', value: `${valueBets.length}`, inline: true },
      { name: 'Control Group', value: `${controlBets.length}`, inline: true },
    ],
    timestamp: new Date().toISOString(),
  }];

  const dirIcon = d => d === 'over' ? '🟢 OVER' : '🔴 UNDER';
  const evFmt = v => v > 0 ? `🟢 +${v}%` : `🔴 ${v}%`;

  // Top picks by value score (favorites)
  if (valueBets.length) {
    const topPicks = valueBets.slice(0, 5);
    const favLines = topPicks.map((b, i) => {
      const medal = i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : '▸';
      return `${medal} **${b.player_name}** — ${dirIcon(b.direction)} ${b.line} ${b.market_label}\n` +
        `　Odds: \`${fmtOdds(b.odds)}\` · Model: \`${(b.model_prob * 100).toFixed(0)}%\` · EV: ${evFmt(b.ev)} · VS: \`${b.value_score}\``;
    }).join('\n\n');

    embeds.push({
      title: '⭐ Favorite Picks (by Value Score)',
      description: favLines,
      color: 0xf59e0b,
    });
  }

  // Best EV picks
  if (valueBets.length) {
    const bestEV = [...valueBets].sort((a, b) => b.ev - a.ev).slice(0, 3);
    const evLines = bestEV.map(b =>
      `💎 **${b.player_name}** — ${dirIcon(b.direction)} ${b.line} ${b.market_label}\n` +
      `　EV: ${evFmt(b.ev)} · Odds: \`${fmtOdds(b.odds)}\` · Edge: ${b.edge > 0 ? '🟢' : '🔴'} \`${b.edge > 0 ? '+' : ''}${b.edge.toFixed(1)}%\``
    ).join('\n\n');

    embeds.push({
      title: '💰 Best Value (by Expected Value)',
      description: evLines,
      color: 0x10b981,
    });
  }

  // Full pick list (compact)
  if (valueBets.length > 5) {
    const remaining = valueBets.slice(5);
    const listLines = remaining.map(b =>
      `${b.direction === 'over' ? '🟢' : '🔴'} ${b.player_name} — ${b.direction.toUpperCase()} ${b.line} ${b.market_label} (${fmtOdds(b.odds)}, EV ${evFmt(b.ev)})`
    ).join('\n');

    embeds.push({
      title: '📋 All Other Value Picks',
      description: listLines,
      color: 0x64748b,
    });
  }

  await postDiscord(DISCORD_PICKS_WEBHOOK, {
    content: '@everyone 🚨 **New picks are in!** The Value Alert agent has locked in today\'s bets.',
    embeds,
  });
}

async function sendDiscordResults(allBets, latestDate) {
  if (!DISCORD_RESULTS_WEBHOOK || !allBets?.length) return;

  const fmtOdds = o => o > 0 ? `+${o}` : `${o}`;
  const pnlFmt = v => `${v >= 0 ? '+' : '-'}$${Math.abs(v).toFixed(2)}`;
  const dateStr = new Date(latestDate + 'T12:00:00').toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });

  const yesterdayBets = allBets.filter(b => b.bet_date === latestDate);
  const yesterdayValue = yesterdayBets.filter(b => !b.is_control);
  const allValue = allBets.filter(b => !b.is_control);
  const allControl = allBets.filter(b => b.is_control);

  const calcStats = (bets) => {
    const settled = bets.filter(b => b.status === 'won' || b.status === 'lost');
    const won = settled.filter(b => b.status === 'won');
    const pnl = settled.reduce((s, b) => s + (b.pnl || 0), 0);
    const staked = settled.reduce((s, b) => s + b.stake, 0);
    return { total: settled.length, won: won.length, lost: settled.length - won.length,
      winRate: settled.length ? (won.length / settled.length * 100).toFixed(1) : '0.0',
      pnl, roi: staked ? (pnl / staked * 100).toFixed(1) : '0.0' };
  };

  const yStats = calcStats(yesterdayValue);
  const aValStats = calcStats(allValue);
  const aCtlStats = calcStats(allControl);

  // Headline embed
  const pnlEmoji = yStats.pnl >= 0 ? '📈' : '📉';
  const embeds = [{
    title: `${pnlEmoji} Daily Results — ${dateStr}`,
    description: `**${yStats.won}-${yStats.lost}** (${yStats.winRate}%) · P&L: **${pnlFmt(yStats.pnl)}**`,
    color: yStats.pnl >= 0 ? 0x10b981 : 0xf43f5e,
    fields: [
      { name: 'All-Time Value', value: `${aValStats.won}-${aValStats.lost} (${aValStats.winRate}%)\n${aValStats.pnl >= 0 ? '🟢' : '🔴'} ${pnlFmt(aValStats.pnl)} · ROI ${aValStats.roi}%`, inline: true },
      { name: 'All-Time Control', value: `${aCtlStats.won}-${aCtlStats.lost} (${aCtlStats.winRate}%)\n${aCtlStats.pnl >= 0 ? '🟢' : '🔴'} ${pnlFmt(aCtlStats.pnl)} · ROI ${aCtlStats.roi}%`, inline: true },
    ],
  }];

  // Pick-by-pick results — split wins and losses
  const settled = yesterdayValue.filter(b => b.status === 'won' || b.status === 'lost');
  const dirIcon = d => d === 'over' ? '🟢 OVER' : '🔴 UNDER';
  const pnlIcon = v => v >= 0 ? `🟢 ${pnlFmt(v)}` : `🔴 ${pnlFmt(v)}`;
  const fmtPick = b => {
    const actual = b.actual_result != null ? b.actual_result : '—';
    const margin = b.actual_result != null && b.line ? ((b.actual_result - b.line) / b.line * 100).toFixed(1) : '—';
    const marginDir = b.direction === 'under' ? (b.actual_result != null ? (-(b.actual_result - b.line) / b.line * 100).toFixed(1) : '—') : margin;
    const marginIcon = parseFloat(marginDir) >= 0 ? '🟢' : '🔴';
    return `**${b.player_name}** ${dirIcon(b.direction)} ${b.line} ${b.market_label}\n` +
      `　Actual: \`${actual}\` · Margin: ${marginIcon} \`${marginDir}%\` · ${fmtOdds(b.odds)} → **${pnlIcon(b.pnl || 0)}**`;
  };
  const wins = settled.filter(b => b.status === 'won').sort((a, b) => (b.pnl || 0) - (a.pnl || 0));
  const losses = settled.filter(b => b.status === 'lost').sort((a, b) => (a.pnl || 0) - (b.pnl || 0));
  if (wins.length) {
    embeds.push({
      title: `✅ Wins (${wins.length})`,
      description: wins.map(b => fmtPick(b)).join('\n\n').slice(0, 4000),
      color: 0x10b981,
    });
  }
  if (losses.length) {
    embeds.push({
      title: `❌ Losses (${losses.length})`,
      description: losses.map(b => fmtPick(b)).join('\n\n').slice(0, 4000),
      color: 0xf43f5e,
    });
  }

  // Model insights — calibration analysis
  const settledValue = allValue.filter(b => b.status === 'won' || b.status === 'lost');
  if (settledValue.length >= 10) {
    const buckets = {};
    for (const b of settledValue) {
      const mp = Math.round((b.model_prob || 0.5) * 100);
      const bk = `${Math.floor(mp / 10) * 10}-${Math.floor(mp / 10) * 10 + 10}%`;
      if (!buckets[bk]) buckets[bk] = { total: 0, won: 0 };
      buckets[bk].total++;
      if (b.status === 'won') buckets[bk].won++;
    }
    const calLines = Object.entries(buckets).sort().map(([range, d]) => {
      const actualRate = (d.won / d.total * 100).toFixed(0);
      const midpoint = parseInt(range);
      const diff = parseInt(actualRate) - midpoint - 5;
      const status = Math.abs(diff) <= 5 ? '✅' : diff > 5 ? '🔥' : '⚠️';
      return `${status} **${range}**: ${actualRate}% actual (${d.won}/${d.total})`;
    }).join('\n');

    embeds.push({
      title: '🎯 Model Calibration',
      description: calLines + '\n\n' +
        (Object.values(buckets).some(d => d.won / d.total > 0.65) ? '🔥 Model is outperforming in some brackets — edge is real!' : '') +
        (Object.values(buckets).some(d => d.total >= 5 && d.won / d.total < 0.4) ? '\n⚠️ Some brackets are underperforming — may need recalibration.' : ''),
      color: 0xa78bfa,
    });
  }

  // Market performance insights
  const mktMap = {};
  for (const b of settledValue) {
    const mk = b.market_label || 'PTS';
    if (!mktMap[mk]) mktMap[mk] = { total: 0, won: 0, pnl: 0 };
    mktMap[mk].total++;
    if (b.status === 'won') mktMap[mk].won++;
    mktMap[mk].pnl += b.pnl || 0;
  }
  if (Object.keys(mktMap).length) {
    const mktLines = Object.entries(mktMap).sort((a, b) => b[1].pnl - a[1].pnl).map(([mk, d]) => {
      const wr = (d.won / d.total * 100).toFixed(0);
      const icon = d.pnl >= 0 ? '🟢' : '🔴';
      return `${icon} **${mk}**: ${d.won}-${d.total - d.won} (${wr}%) · ${pnlFmt(d.pnl)}`;
    }).join('\n');

    embeds.push({
      title: '📈 Market Performance',
      description: mktLines,
      color: 0x3b82f6,
    });
  }

  // Recommendations based on data
  if (settledValue.length >= 20) {
    const recs = [];
    // Check which markets are best/worst
    const mktEntries = Object.entries(mktMap).filter(([, d]) => d.total >= 5);
    const bestMkt = mktEntries.sort((a, b) => (b[1].won / b[1].total) - (a[1].won / a[1].total))[0];
    const worstMkt = mktEntries.sort((a, b) => (a[1].won / a[1].total) - (b[1].won / b[1].total))[0];
    if (bestMkt) recs.push(`📊 **Best market**: ${bestMkt[0]} at ${(bestMkt[1].won / bestMkt[1].total * 100).toFixed(0)}% win rate — consider increasing exposure`);
    if (worstMkt && worstMkt[0] !== bestMkt?.[0] && worstMkt[1].won / worstMkt[1].total < 0.45)
      recs.push(`⚠️ **Weakest market**: ${worstMkt[0]} at ${(worstMkt[1].won / worstMkt[1].total * 100).toFixed(0)}% — consider raising threshold`);

    // Check high-confidence performance
    const highConf = settledValue.filter(b => b.confidence >= 75);
    if (highConf.length >= 5) {
      const hcWr = highConf.filter(b => b.status === 'won').length / highConf.length * 100;
      if (hcWr >= 60) recs.push(`🔥 **Elite confidence (75+)**: ${hcWr.toFixed(0)}% hit rate — model excels at high conviction`);
      else recs.push(`🔍 **Elite confidence (75+)**: only ${hcWr.toFixed(0)}% — confidence scoring may need tuning`);
    }

    // Trend: last 7 days vs prior
    const dates = [...new Set(settledValue.map(b => b.bet_date))].sort().reverse();
    if (dates.length >= 7) {
      const recent = settledValue.filter(b => dates.slice(0, 7).includes(b.bet_date));
      const prior = settledValue.filter(b => !dates.slice(0, 7).includes(b.bet_date));
      if (prior.length >= 10) {
        const recentWr = recent.filter(b => b.status === 'won').length / recent.length * 100;
        const priorWr = prior.filter(b => b.status === 'won').length / prior.length * 100;
        const trend = recentWr - priorWr;
        if (Math.abs(trend) > 5)
          recs.push(`${trend > 0 ? '📈' : '📉'} **7-day trend**: ${recentWr.toFixed(0)}% vs ${priorWr.toFixed(0)}% prior — ${trend > 0 ? 'model improving' : 'possible cold streak, stay the course'}`);
      }
    }

    // Overall ROI assessment
    if (aValStats.pnl > 0 && parseFloat(aValStats.roi) > 5)
      recs.push(`💰 **ROI at ${aValStats.roi}%** — edge is holding up across ${aValStats.total} bets`);
    else if (parseFloat(aValStats.roi) < -5)
      recs.push(`🔬 **ROI at ${aValStats.roi}%** — consider tightening value score threshold from 30 to 35`);

    if (recs.length) {
      embeds.push({
        title: '💡 Recommendations & Insights',
        description: recs.join('\n\n'),
        color: 0xf59e0b,
        footer: { text: `Based on ${settledValue.length} settled value bets` },
      });
    }
  }

  await postDiscord(DISCORD_RESULTS_WEBHOOK, {
    content: `@everyone ${yStats.pnl >= 0 ? '🟢' : '🔴'} **Results are in!** ${yStats.won}-${yStats.lost} today (${pnlFmt(yStats.pnl)})`,
    embeds: embeds.slice(0, 10), // Discord max 10 embeds
  });
}

const MKT_LABELS = { player_points: 'PTS', player_rebounds: 'REB', player_assists: 'AST', player_threes: '3PM', player_points_rebounds_assists: 'PRA' };
const STAT_KEYS  = { player_points: 'pts', player_rebounds: 'reb', player_assists: 'ast', player_threes: 'fg3m', player_points_rebounds_assists: 'pra' };

// Server-side value scoring (mirrors client logic exactly)
function serverValueScore(edge, hitRate, confidence, dirEV) {
  const evScore = Math.min(Math.max(dirEV, -20), 20);
  const isOver = edge >= 0;
  const dirHR = isOver ? hitRate : (100 - hitRate);
  return Math.round(Math.abs(edge) * 0.25 + Math.max(0, dirHR - 50) * 0.25 + confidence * 0.25 + (evScore + 20) * 0.25);
}

function serverComputeStats(gameLog, line, market, overOdds, underOdds) {
  const statKey = STAT_KEYS[market] || 'pts';
  const getVal = g => statKey === 'pra' ? (+g.pts || 0) + (+g.reb || 0) + (+g.ast || 0) : +(g[statKey] || 0);
  const sorted = [...gameLog].sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
  const played = sorted.filter(g => parseInt(g.min || '0') > 0);
  if (!played.length) return null;

  const vals = played.map(g => getVal(g));
  const l10 = vals.slice(0, 10);
  const l5 = vals.slice(0, 5);
  const avg = +(vals.reduce((a, b) => a + b, 0) / vals.length).toFixed(1);
  const hitRate = line ? Math.round(vals.filter(v => v >= line).length / vals.length * 100) : 50;
  const edge = line ? +((avg - line) / line * 100).toFixed(1) : 0;
  const l10avg = l10.reduce((a, b) => a + b, 0) / l10.length;
  const stdDev = +(Math.sqrt(l10.reduce((s, v) => s + Math.pow(v - l10avg, 2), 0) / l10.length)).toFixed(1);
  const cv = avg ? stdDev / avg : 0.4;

  const l10HitRate = line && l10.length ? Math.round(l10.filter(v => v >= line).length / l10.length * 100) : hitRate;
  const l5HitRate = (l5.length >= 3 && line) ? Math.round(l5.filter(v => v >= line).length / l5.length * 100) : l10HitRate;

  const rawProb = (l5HitRate * 0.35 + l10HitRate * 0.40 + hitRate * 0.25) / 100;
  const modelProb = Math.min(0.97, Math.max(0.03, (rawProb * vals.length + 5) / (vals.length + 10)));

  const evOver = calcEV(modelProb, overOdds);
  const evUnder = calcEV(1 - modelProb, underOdds);

  const isOverBet = edge >= 0;
  const dirModelProb = isOverBet ? modelProb : 1 - modelProb;
  const dirImplied = isOverBet ? impliedProb(overOdds) : impliedProb(underOdds);
  const evEdge = dirModelProb - dirImplied;
  const edgePts = Math.min(40, Math.max(0, evEdge / 0.15 * 20 + 20));
  const dirHR = Math.round(dirModelProb * 100);
  const hrPts = Math.min(40, Math.max(0, (dirHR - 25) * 0.8));
  const cvPts = Math.min(20, Math.max(0, (0.6 - Math.min(cv, 0.6)) / 0.4 * 20));
  const confidence = Math.max(0, Math.min(100, Math.round(edgePts + hrPts + cvPts)));

  const direction = isOverBet ? 'over' : 'under';
  const odds = isOverBet ? overOdds : underOdds;
  const dirEV = isOverBet ? evOver : evUnder;
  const vs = serverValueScore(edge, hitRate, confidence, dirEV);

  return { avg, hitRate, edge, modelProb, confidence, evOver, evUnder, direction, odds, dirEV, vs, stdDev, gamesPlayed: vals.length };
}

// ============================================================
// CRON: Place agent bets — runs daily at 3:30 PM PST
// ============================================================
app.get('/api/cron/agent-bet', async (req, res) => {
  const secret = (req.headers.authorization || '').replace('Bearer ', '') || req.headers['x-cron-secret'] || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!supabase) return res.status(503).json({ error: 'Supabase not configured' });

  const STAKE = 10;
  const today = new Date().toISOString().slice(0, 10);

  // Idempotency: skip if we already placed bets today
  const { count: existing } = await supabase.from('agent_bets').select('id', { count: 'exact', head: true }).eq('bet_date', today);
  if (existing > 0) return res.json({ skipped: true, message: `Already placed ${existing} agent bets for ${today}` });

  const summary = { value: 0, control: 0, skipped: 0, errors: [] };

  try {
    // 1. Get today's odds from cache (combined book)
    const allPlayers = await sbGetOdds('combined');
    if (!allPlayers?.length) return res.status(404).json({ error: 'No odds data in cache' });

    // 2. Filter to only games that haven't started
    const now = new Date();
    const upcoming = allPlayers.filter(p => !p.gameTime || new Date(p.gameTime) > now);

    // 3. For each player/market combo, compute stats and value score
    const betsToInsert = [];
    const processedKeys = new Set();

    for (const p of upcoming) {
      const key = `${p.name}|${p.market}`;
      if (processedKeys.has(key)) continue;
      processedKeys.add(key);

      try {
        // Get real game logs from Supabase
        const record = await sbGetGameLogRecord(p.name);
        if (!record?.game_log?.length) { summary.skipped++; continue; }

        const stats = serverComputeStats(record.game_log, p.line, p.market, p.overOdds, p.underOdds);
        if (!stats || stats.gamesPlayed < 5) { summary.skipped++; continue; }

        const isValue = stats.vs >= 30 && Math.abs(stats.edge) > 4 && stats.confidence >= 55 && stats.dirEV > 3;
        // Control group: nearly qualified but didn't make the cut
        const isControl = !isValue && stats.vs >= 20 && Math.abs(stats.edge) > 2 && stats.dirEV > 0;

        if (!isValue && !isControl) { summary.skipped++; continue; }

        const payout = stats.odds > 0 ? +(STAKE * stats.odds / 100).toFixed(2) : +(STAKE * 100 / Math.abs(stats.odds)).toFixed(2);

        betsToInsert.push({
          bet_date: today,
          player_name: p.name,
          team: guessTeam(p.name) || p.homeTeam || null,
          matchup: p.matchup || null,
          market: p.market,
          market_label: MKT_LABELS[p.market] || p.market,
          line: p.line,
          direction: stats.direction,
          odds: stats.odds,
          model_prob: +stats.modelProb.toFixed(4),
          confidence: stats.confidence,
          edge: stats.edge,
          ev: +stats.dirEV.toFixed(1),
          value_score: stats.vs,
          hit_rate: stats.hitRate,
          is_control: isControl,
          stake: STAKE,
          status: 'open',
          game_time: p.gameTime || null,
        });

        if (isValue) summary.value++;
        else summary.control++;
      } catch (e) {
        summary.errors.push(`${p.name}: ${e.message}`);
      }
    }

    // 4. Bulk insert
    if (betsToInsert.length) {
      const { error: insertErr } = await supabase.from('agent_bets').insert(betsToInsert);
      if (insertErr) return res.status(500).json({ error: insertErr.message, summary });
    }

    // 5. Send Discord notification
    await sendDiscordPicks(betsToInsert, today);

    res.json({ success: true, date: today, ...summary, total: betsToInsert.length });
  } catch (err) {
    res.status(500).json({ error: err.message, ...summary });
  }
});

// ============================================================
// CRON: Grade agent bets — runs after games complete
// ============================================================
app.get('/api/cron/grade-agent-bets', async (req, res) => {
  const secret = (req.headers.authorization || '').replace('Bearer ', '') || req.headers['x-cron-secret'] || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!supabase) return res.status(503).json({ error: 'Supabase not configured' });

  const GRADE_AFTER_MS = 10 * 60 * 60 * 1000;
  const now = Date.now();
  const summary = { graded: 0, skipped: 0, errors: [] };

  try {
    const { data: openBets, error: fetchErr } = await supabase
      .from('agent_bets')
      .select('*')
      .eq('status', 'open');
    if (fetchErr) return res.status(500).json({ error: fetchErr.message });

    for (const bet of (openBets || [])) {
      if (!bet.game_time || (now - new Date(bet.game_time).getTime()) < GRADE_AFTER_MS) {
        summary.skipped++;
        continue;
      }

      try {
        const gameDate = getEtDate(bet.game_time);
        const { status, value } = await getActualStat(bet.player_name, gameDate, bet.market);

        if (status === 'missing') { summary.skipped++; continue; }

        const outcome = status === 'dnp' ? 'void' : gradeSingleOutcome(value, +bet.line, bet.direction);
        let pnl = 0;
        if (outcome === 'won') {
          pnl = bet.odds > 0 ? +(bet.stake * bet.odds / 100).toFixed(2) : +(bet.stake * 100 / Math.abs(bet.odds)).toFixed(2);
        } else if (outcome === 'lost') {
          pnl = -bet.stake;
        }

        const { error: updateErr } = await supabase.from('agent_bets').update({
          status: outcome,
          actual_result: status === 'dnp' ? null : value,
          pnl,
          graded_at: new Date().toISOString(),
        }).eq('id', bet.id);

        if (updateErr) summary.errors.push(`${bet.player_name}: ${updateErr.message}`);
        else summary.graded++;
      } catch (e) {
        summary.errors.push(`${bet.player_name}: ${e.message}`);
      }
    }

    res.json({ success: true, ...summary });
  } catch (err) {
    res.status(500).json({ error: err.message, ...summary });
  }
});

// ============================================================
// CRON: Send daily newsletter — runs morning after games
// ============================================================
app.get('/api/cron/newsletter', async (req, res) => {
  const secret = (req.headers.authorization || '').replace('Bearer ', '') || req.headers['x-cron-secret'] || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!supabase) return res.status(503).json({ error: 'Supabase not configured' });
  if (!resend || !NEWSLETTER_TO) return res.status(503).json({ error: 'Email not configured. Set RESEND_API_KEY and NEWSLETTER_EMAIL.' });

  try {
    // Get all graded bets
    const { data: allBets } = await supabase.from('agent_bets').select('*').neq('status', 'open').order('bet_date', { ascending: false });
    if (!allBets?.length) return res.json({ message: 'No graded bets yet' });

    // Yesterday's bets (most recent bet_date with graded results)
    const latestDate = allBets[0].bet_date;
    const yesterdayBets = allBets.filter(b => b.bet_date === latestDate);
    const yesterdayValue = yesterdayBets.filter(b => !b.is_control);
    const yesterdayControl = yesterdayBets.filter(b => b.is_control);

    // All-time stats
    const allValue = allBets.filter(b => !b.is_control);
    const allControl = allBets.filter(b => b.is_control);

    const stats = (bets) => {
      const settled = bets.filter(b => b.status === 'won' || b.status === 'lost');
      const won = settled.filter(b => b.status === 'won');
      const totalPnl = settled.reduce((s, b) => s + (b.pnl || 0), 0);
      const totalStaked = settled.reduce((s, b) => s + b.stake, 0);
      return {
        total: settled.length, won: won.length, lost: settled.length - won.length,
        winRate: settled.length ? (won.length / settled.length * 100).toFixed(1) : '0.0',
        pnl: totalPnl, roi: totalStaked ? (totalPnl / totalStaked * 100).toFixed(1) : '0.0',
        staked: totalStaked,
      };
    };

    const yValStats = stats(yesterdayValue);
    const yCtlStats = stats(yesterdayControl);
    const aValStats = stats(allValue);
    const aCtlStats = stats(allControl);

    // Calibration buckets (all-time, value bets only)
    const buckets = {};
    for (const b of allValue.filter(b => b.status === 'won' || b.status === 'lost')) {
      const mp = Math.round((b.model_prob || 0.5) * 100);
      const bucket = `${Math.floor(mp / 5) * 5}-${Math.floor(mp / 5) * 5 + 5}%`;
      if (!buckets[bucket]) buckets[bucket] = { total: 0, won: 0 };
      buckets[bucket].total++;
      if (b.status === 'won') buckets[bucket].won++;
    }
    const calibrationRows = Object.entries(buckets).sort().map(([range, d]) =>
      `<tr><td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">${range}</td>
       <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${d.total}</td>
       <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${(d.won / d.total * 100).toFixed(1)}%</td></tr>`
    ).join('');

    // By market breakdown
    const mktMap = {};
    for (const b of allValue.filter(b => b.status === 'won' || b.status === 'lost')) {
      const mk = b.market_label || 'PTS';
      if (!mktMap[mk]) mktMap[mk] = { total: 0, won: 0, pnl: 0 };
      mktMap[mk].total++;
      if (b.status === 'won') mktMap[mk].won++;
      mktMap[mk].pnl += b.pnl || 0;
    }
    const marketRows = Object.entries(mktMap).sort((a, b) => b[1].total - a[1].total).map(([mk, d]) =>
      `<tr><td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">${mk}</td>
       <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${d.won}-${d.total - d.won}</td>
       <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${(d.won / d.total * 100).toFixed(0)}%</td>
       <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:right;color:${d.pnl >= 0 ? '#16a34a' : '#dc2626'}">${d.pnl >= 0 ? '+' : ''}$${d.pnl.toFixed(2)}</td></tr>`
    ).join('');

    // By confidence tier
    const confTiers = { 'Elite (75+)': { min: 75 }, 'Strong (65-74)': { min: 65 }, 'Standard (55-64)': { min: 55 } };
    const confRows = Object.entries(confTiers).map(([label, cfg]) => {
      const tier = allValue.filter(b => (b.status === 'won' || b.status === 'lost') && b.confidence >= cfg.min && (cfg.min === 75 || b.confidence < cfg.min + 10));
      if (!tier.length) return '';
      const w = tier.filter(b => b.status === 'won').length;
      const pnl = tier.reduce((s, b) => s + (b.pnl || 0), 0);
      return `<tr><td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">${label}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${w}-${tier.length - w}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${(w / tier.length * 100).toFixed(0)}%</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:right;color:${pnl >= 0 ? '#16a34a' : '#dc2626'}">${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}</td></tr>`;
    }).filter(Boolean).join('');

    // Yesterday's pick-by-pick results
    const fmtOdds = o => o > 0 ? `+${o}` : `${o}`;
    const pickRows = yesterdayValue.filter(b => b.status !== 'open').map(b => {
      const sc = b.status === 'won' ? '#16a34a' : b.status === 'lost' ? '#dc2626' : '#94a3b8';
      const icon = b.status === 'won' ? '✅' : b.status === 'lost' ? '❌' : '—';
      const actual = b.actual_result != null ? b.actual_result : '—';
      return `<tr>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">${icon} ${b.player_name}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">${b.direction === 'over' ? '▲' : '▼'} ${b.line} ${b.market_label}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${fmtOdds(b.odds)}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${(b.model_prob * 100).toFixed(0)}%</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${actual}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;font-weight:700;color:${sc}">${b.status.toUpperCase()}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:right;color:${(b.pnl||0) >= 0 ? '#16a34a' : '#dc2626'}">${(b.pnl||0) >= 0 ? '+' : ''}$${(b.pnl||0).toFixed(2)}</td>
      </tr>`;
    }).join('');

    // Build HTML
    const pnlFmt = v => `${v >= 0 ? '+' : ''}$${Math.abs(v).toFixed(2)}`;
    const pnlClr = v => v >= 0 ? '#16a34a' : '#dc2626';
    const dateStr = new Date(latestDate + 'T12:00:00').toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' });

    const html = `<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<div style="max-width:640px;margin:0 auto;background:#fff;">

  <!-- HEADER -->
  <div style="background:linear-gradient(135deg,#1e293b,#0f172a);padding:32px 24px;text-align:center;">
    <div style="font-size:28px;font-weight:800;color:#fff;letter-spacing:-0.5px;">Value Alert Daily Report</div>
    <div style="color:#94a3b8;font-size:14px;margin-top:6px;">${dateStr}</div>
  </div>

  <!-- HEADLINE -->
  <div style="padding:24px;text-align:center;border-bottom:1px solid #e2e8f0;">
    <div style="font-size:14px;color:#64748b;text-transform:uppercase;letter-spacing:1px;font-weight:600;">Value Alerts Record</div>
    <div style="font-size:36px;font-weight:800;color:${pnlClr(yValStats.pnl)};margin:8px 0;">${pnlFmt(yValStats.pnl)}</div>
    <div style="font-size:18px;font-weight:700;color:#1e293b;">${yValStats.won}-${yValStats.lost} (${yValStats.winRate}%)</div>
    <div style="font-size:12px;color:#94a3b8;margin-top:4px;">$${yValStats.staked.toFixed(0)} staked · ${yValStats.total} picks</div>
  </div>

  ${yesterdayControl.length ? `
  <!-- CONTROL GROUP HEADLINE -->
  <div style="padding:16px 24px;text-align:center;border-bottom:1px solid #e2e8f0;background:#f8fafc;">
    <div style="font-size:12px;color:#94a3b8;text-transform:uppercase;letter-spacing:1px;font-weight:600;">Control Group (Near-Misses)</div>
    <div style="font-size:20px;font-weight:700;color:${pnlClr(yCtlStats.pnl)};margin-top:4px;">${yCtlStats.won}-${yCtlStats.lost} · ${pnlFmt(yCtlStats.pnl)}</div>
  </div>` : ''}

  <!-- PICK BY PICK -->
  <div style="padding:24px;">
    <div style="font-size:16px;font-weight:700;color:#1e293b;margin-bottom:12px;">Yesterday's Picks</div>
    <table style="width:100%;border-collapse:collapse;font-size:12px;">
      <thead><tr style="background:#f8fafc;">
        <th style="padding:8px 12px;text-align:left;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Player</th>
        <th style="padding:8px 12px;text-align:left;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Line</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Odds</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Prob</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Actual</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Result</th>
        <th style="padding:8px 12px;text-align:right;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">P&L</th>
      </tr></thead>
      <tbody>${pickRows || '<tr><td colspan="7" style="padding:16px;text-align:center;color:#94a3b8;">No graded picks yet</td></tr>'}</tbody>
    </table>
  </div>

  <!-- ALL-TIME SUMMARY -->
  <div style="padding:24px;background:#f8fafc;border-top:1px solid #e2e8f0;">
    <div style="font-size:16px;font-weight:700;color:#1e293b;margin-bottom:12px;">Season Totals</div>
    <table style="width:100%;border-collapse:collapse;font-size:13px;">
      <tr>
        <td style="padding:8px 0;color:#64748b;">Value Alerts</td>
        <td style="padding:8px 0;text-align:right;font-weight:700;">${aValStats.won}-${aValStats.lost} (${aValStats.winRate}%)</td>
        <td style="padding:8px 0;text-align:right;font-weight:700;color:${pnlClr(aValStats.pnl)}">${pnlFmt(aValStats.pnl)}</td>
        <td style="padding:8px 0;text-align:right;color:#64748b;">ROI ${aValStats.roi}%</td>
      </tr>
      <tr>
        <td style="padding:8px 0;color:#64748b;">Control Group</td>
        <td style="padding:8px 0;text-align:right;font-weight:700;">${aCtlStats.won}-${aCtlStats.lost} (${aCtlStats.winRate}%)</td>
        <td style="padding:8px 0;text-align:right;font-weight:700;color:${pnlClr(aCtlStats.pnl)}">${pnlFmt(aCtlStats.pnl)}</td>
        <td style="padding:8px 0;text-align:right;color:#64748b;">ROI ${aCtlStats.roi}%</td>
      </tr>
    </table>
  </div>

  <!-- CALIBRATION -->
  <div style="padding:24px;border-top:1px solid #e2e8f0;">
    <div style="font-size:16px;font-weight:700;color:#1e293b;margin-bottom:4px;">Model Calibration</div>
    <div style="font-size:12px;color:#94a3b8;margin-bottom:12px;">When we say X% probability, how often does it actually hit?</div>
    <table style="width:100%;border-collapse:collapse;font-size:12px;">
      <thead><tr style="background:#f8fafc;">
        <th style="padding:8px 12px;text-align:left;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Model Prob</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Bets</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Actual Hit Rate</th>
      </tr></thead>
      <tbody>${calibrationRows || '<tr><td colspan="3" style="padding:16px;text-align:center;color:#94a3b8;">Not enough data yet</td></tr>'}</tbody>
    </table>
  </div>

  <!-- BY MARKET -->
  <div style="padding:24px;background:#f8fafc;border-top:1px solid #e2e8f0;">
    <div style="font-size:16px;font-weight:700;color:#1e293b;margin-bottom:12px;">Performance by Market</div>
    <table style="width:100%;border-collapse:collapse;font-size:12px;">
      <thead><tr>
        <th style="padding:8px 12px;text-align:left;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Market</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Record</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Win %</th>
        <th style="padding:8px 12px;text-align:right;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">P&L</th>
      </tr></thead>
      <tbody>${marketRows}</tbody>
    </table>
  </div>

  <!-- BY CONFIDENCE TIER -->
  <div style="padding:24px;border-top:1px solid #e2e8f0;">
    <div style="font-size:16px;font-weight:700;color:#1e293b;margin-bottom:12px;">Performance by Confidence</div>
    <table style="width:100%;border-collapse:collapse;font-size:12px;">
      <thead><tr>
        <th style="padding:8px 12px;text-align:left;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Tier</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Record</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Win %</th>
        <th style="padding:8px 12px;text-align:right;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">P&L</th>
      </tr></thead>
      <tbody>${confRows}</tbody>
    </table>
  </div>

  <!-- FOOTER -->
  <div style="padding:24px;background:#0f172a;text-align:center;">
    <div style="color:#64748b;font-size:11px;">Value Alert Agent · Automated Tracking</div>
    <div style="color:#475569;font-size:10px;margin-top:4px;">Flat $${STAKE} units · All bets placed at 3:30 PM PST daily</div>
  </div>

</div></body></html>`;

    // Send email
    const { error: emailErr } = await resend.emails.send({
      from: NEWSLETTER_FROM,
      to: NEWSLETTER_TO,
      subject: `Value Alert Daily: ${yValStats.won}-${yValStats.lost} (${pnlFmt(yValStats.pnl)}) — ${dateStr}`,
      html,
    });

    if (emailErr) return res.status(500).json({ error: emailErr.message });

    // Send Discord results notification
    await sendDiscordResults(allBets, latestDate);

    res.json({ success: true, sent_to: NEWSLETTER_TO, date: latestDate });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---- Diagnostic ping ----
app.get('/api/ping', (req, res) => {
  res.json({ ok: true, time: new Date().toISOString(), discord: { picks: !!DISCORD_PICKS_WEBHOOK, results: !!DISCORD_RESULTS_WEBHOOK } });
});

// ---- TEST: Send sample Discord messages ----
app.get('/api/test-discord', async (req, res) => {
  const secret = (req.headers.authorization || '').replace('Bearer ', '') || req.headers['x-cron-secret'] || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const results = { picks: false, results: false };

  // Test #picks channel
  if (DISCORD_PICKS_WEBHOOK) {
    try {
      await postDiscord(DISCORD_PICKS_WEBHOOK, {
        content: '🧪 **Test message from Value Alert Agent**',
        embeds: [{
          title: '🏀 Value Alert Agent — Test',
          description: 'Placed **4 value picks** and **2 control picks** for today\'s slate.',
          color: 0x6366f1,
          fields: [
            { name: 'Total Picks', value: '6', inline: true },
            { name: 'Value Alerts', value: '4', inline: true },
            { name: 'Control Group', value: '2', inline: true },
          ],
          timestamp: new Date().toISOString(),
        }, {
          title: '⭐ Favorite Picks (by Value Score)',
          description:
            '🥇 **LeBron James** — 🟢 OVER 25.5 PTS\n　Odds: `-110` · Model: `68%` · EV: 🟢 +8.2% · VS: `74`\n\n' +
            '🥈 **Nikola Jokic** — 🟢 OVER 9.5 AST\n　Odds: `+105` · Model: `62%` · EV: 🟢 +11.5% · VS: `68`\n\n' +
            '🥉 **Jayson Tatum** — 🔴 UNDER 4.5 3PM\n　Odds: `-125` · Model: `71%` · EV: 🟢 +6.1% · VS: `61`',
          color: 0xf59e0b,
        }, {
          title: '💰 Best Value (by Expected Value)',
          description:
            '💎 **Nikola Jokic** — 🟢 OVER 9.5 AST\n　EV: 🟢 +11.5% · Odds: `+105` · Edge: 🟢 `+7.2%`\n\n' +
            '💎 **LeBron James** — 🟢 OVER 25.5 PTS\n　EV: 🟢 +8.2% · Odds: `-110` · Edge: 🟢 `+5.8%`',
          color: 0x10b981,
        }],
      });
      results.picks = true;
    } catch (e) { results.picks = e.message; }
  }

  // Test #results channel
  if (DISCORD_RESULTS_WEBHOOK) {
    try {
      await postDiscord(DISCORD_RESULTS_WEBHOOK, {
        content: '🧪 🟢 **Test results from Value Alert Agent** — 3-1 today (+$18.50)',
        embeds: [{
          title: '📈 Daily Results — Test',
          description: '**3-1** (75.0%) · P&L: **+$18.50**',
          color: 0x10b981,
          fields: [
            { name: 'All-Time Value', value: '28-17 (62.2%)\n🟢 +$94.30 · ROI 12.1%', inline: true },
            { name: 'All-Time Control', value: '19-22 (46.3%)\n🔴 -$31.20 · ROI -4.8%', inline: true },
          ],
        }, {
          title: '✅ Wins (3)',
          description:
            '**Nikola Jokic** 🟢 OVER 9.5 AST\n　Actual: `12` · Margin: 🟢 `+26.3%` · +105 → **🟢 +$10.50**\n\n' +
            '**LeBron James** 🟢 OVER 25.5 PTS\n　Actual: `31` · Margin: 🟢 `+21.6%` · -110 → **🟢 +$9.09**\n\n' +
            '**Jayson Tatum** 🔴 UNDER 4.5 3PM\n　Actual: `3` · Margin: 🟢 `+33.3%` · -125 → **🟢 +$8.00**',
          color: 0x10b981,
        }, {
          title: '❌ Losses (1)',
          description:
            '**Anthony Davis** 🟢 OVER 11.5 REB\n　Actual: `9` · Margin: 🔴 `-21.7%` · -115 → **🔴 -$10.00**',
          color: 0xf43f5e,
        }, {
          title: '🎯 Model Calibration',
          description:
            '✅ **50-60%**: 57% actual (8/14)\n' +
            '🔥 **60-70%**: 71% actual (12/17)\n' +
            '🔥 **70-80%**: 78% actual (7/9)\n\n' +
            '🔥 Model is outperforming in some brackets — edge is real!',
          color: 0xa78bfa,
        }, {
          title: '📈 Market Performance',
          description:
            '🟢 **PTS**: 12-6 (67%) · +$41.20\n' +
            '🟢 **AST**: 8-5 (62%) · +$28.50\n' +
            '🟢 **3PM**: 5-3 (63%) · +$18.40\n' +
            '🔴 **REB**: 3-5 (38%) · -$12.80\n' +
            '🟢 **PRA**: 4-2 (67%) · +$19.00',
          color: 0x3b82f6,
        }, {
          title: '💡 Recommendations & Insights',
          description:
            '📊 **Best market**: PTS at 67% win rate — consider increasing exposure\n\n' +
            '⚠️ **Weakest market**: REB at 38% — consider raising threshold\n\n' +
            '🔥 **Elite confidence (75+)**: 78% hit rate — model excels at high conviction\n\n' +
            '📈 **7-day trend**: 65% vs 58% prior — model improving\n\n' +
            '💰 **ROI at 12.1%** — edge is holding up across 45 bets',
          color: 0xf59e0b,
          footer: { text: 'Based on 45 settled value bets' },
        }],
      });
      results.results = true;
    } catch (e) { results.results = e.message; }
  }

  res.json({ success: true, ...results, configured: { picks: !!DISCORD_PICKS_WEBHOOK, results: !!DISCORD_RESULTS_WEBHOOK } });
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

// Load player team cache from Supabase on cold start (non-blocking)
if (supabase) {
  (async () => {
    try {
      const { data } = await supabase.from('player_stats').select('player_name, game_log').eq('season', NBA_SEASON);
      if (data) {
        for (const r of data) {
          const latest = r.game_log?.sort((a, b) => new Date(b.date) - new Date(a.date))?.[0];
          if (latest?.team) _playerTeamCache[r.player_name] = latest.team;
        }
        console.log(`Team cache loaded: ${Object.keys(_playerTeamCache).length} players`);
      }
    } catch (e) { console.warn('Team cache load failed:', e.message); }
  })();
}

// Required for Vercel serverless
module.exports = app;
