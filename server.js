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

// ---- PLAYER NAME NORMALIZATION & ALIAS MAP ----
// Normalize: lowercase, strip suffixes/periods/accents, collapse whitespace
function normalizeName(name) {
  return (name || '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // strip accents
    .replace(/\b(jr\.?|sr\.?|ii|iii|iv)\b/gi, '')     // strip suffixes
    .replace(/\./g, '')                                  // strip periods
    .replace(/\s+/g, ' ')                               // collapse spaces
    .trim().toLowerCase();
}

// Alias map: injury report name → canonical DB/odds name
// Add entries when the PDF parser produces a different name than what's in Supabase
const INJURY_NAME_ALIASES = {
  'Moritz Wagner': 'Moe Wagner',
  'Ronald Holland II': 'Ron Holland',
  'Ronald Holland': 'Ron Holland',
  'GG Jackson': 'G.G. Jackson',
  'G.G. Jackson II': 'G.G. Jackson',
  'GG Jackson II': 'G.G. Jackson',
  'Herbert Jones': 'Herb Jones',
  'RJ Barrett': 'R.J. Barrett',
  'Robert Williams III': 'Robert Williams',
  'Derrick Jones Jr.': 'Derrick Jones',
  'Derrick Jones Jr': 'Derrick Jones',
  'Kevin Porter Jr': 'Kevin Porter Jr.',
  'Jabari Smith': 'Jabari Smith Jr.',
  'Jabari Smith Jr': 'Jabari Smith Jr.',
  'Michael Porter Jr': 'Michael Porter Jr.',
  'Marcus Morris Sr': 'Marcus Morris Sr.',
  'Marcus Morris': 'Marcus Morris Sr.',
  'Larry Nance': 'Larry Nance Jr.',
  'Larry Nance Jr': 'Larry Nance Jr.',
  'Gary Trent': 'Gary Trent Jr.',
  'Gary Trent Jr': 'Gary Trent Jr.',
  'Kelly Oubre': 'Kelly Oubre Jr.',
  'Kelly Oubre Jr': 'Kelly Oubre Jr.',
  'Wendell Carter': 'Wendell Carter Jr.',
  'Wendell Carter Jr': 'Wendell Carter Jr.',
  'Tim Hardaway': 'Tim Hardaway Jr.',
  'Tim Hardaway Jr': 'Tim Hardaway Jr.',
  'Kenyon Martin': 'Kenyon Martin Jr.',
  'Kenyon Martin Jr': 'Kenyon Martin Jr.',
  'Troy Brown': 'Troy Brown Jr.',
  'Troy Brown Jr': 'Troy Brown Jr.',
  'Dennis Smith': 'Dennis Smith Jr.',
  'Dennis Smith Jr': 'Dennis Smith Jr.',
  'Jaren Jackson': 'Jaren Jackson Jr.',
  'Jaren Jackson Jr': 'Jaren Jackson Jr.',
  'Dereck Lively': 'Dereck Lively II',
  'Trey Murphy': 'Trey Murphy III',
  'Jaime Jaquez': 'Jaime Jaquez Jr.',
  'Jaime Jaquez Jr': 'Jaime Jaquez Jr.',
  'Bogdan Bogdanovic': 'Bogdan Bogdanovic',
  'Jonas Valanciunas': 'Jonas Valanciunas',
  'Nikola Jokic': 'Nikola Jokic',
  'Luka Doncic': 'Luka Doncic',
  'Kristaps Porzingis': 'Kristaps Porzingis',
  'Alperen Sengun': 'Alperen Sengun',
  'Naz Reid': 'Naz Reid',
};

// Build a normalized lookup index for fuzzy matching
const _normalizedPlayerIndex = new Map(); // normName → canonical name
function _buildNormalizedIndex(names) {
  for (const name of names) {
    const norm = normalizeName(name);
    if (!_normalizedPlayerIndex.has(norm)) _normalizedPlayerIndex.set(norm, name);
  }
}

// Resolve an injury report name to a canonical DB name
// Priority: exact → alias → normalized match → original
let _indexBuilt = false;
function resolvePlayerName(injuryName) {
  // Lazy-build normalized index from known player names
  if (!_indexBuilt) {
    _buildNormalizedIndex(Object.keys(_playerTeamCache));
    _buildNormalizedIndex(Object.keys(BDL_PLAYER_OVERRIDES));
    _buildNormalizedIndex(Object.keys(ESPN_PLAYERS));
    _indexBuilt = true;
  }
  // 1. Exact alias match
  if (INJURY_NAME_ALIASES[injuryName]) return INJURY_NAME_ALIASES[injuryName];
  // 2. Normalized alias match (handles case/suffix variations)
  const normInj = normalizeName(injuryName);
  for (const [alias, canonical] of Object.entries(INJURY_NAME_ALIASES)) {
    if (normalizeName(alias) === normInj) return canonical;
  }
  // 3. Normalized match against known player names in DB
  if (_normalizedPlayerIndex.has(normInj)) return _normalizedPlayerIndex.get(normInj);
  // 4. Return original
  return injuryName;
}

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
  'Ace Bailey':'UTA','Sandro Mamukelashvili':'SAS','Matas Buzelis':'CHI',
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
  'Kris Dunn':'LAC','Kevin Huerter':'IND','Jabari Smith Jr':'HOU','Leonard Miller':'CHI',
  'Marcus Smart':'LAL','Adem Bona':'PHI','Nolan Traore':'BKN','Norman Powell':'MIA','Jonas Valanciunas':'DEN',
  'Trae Young':'WAS','Alex Sarr':'WAS','Quentin Grimes':'PHI','Tim Hardaway Jr':'DEN',
  'Dylan Harper':'SAS','Will Riley':'WAS','Carlton Carrington':'WAS','Jett Howard':'ORL',
  'Danny Wolf':'BKN','Dominick Barlow':'PHI','Jevon Carter':'ORL','VJ Edgecombe':'PHI',
  'Tristan da Silva':'ORL','Cameron Johnson':'DEN','Julian Champagnie':'SAS',
  'Tre Johnson':'WAS','Wendell Carter Jr':'ORL','Desmond Bane':'ORL',
  'Nickeil Alexander-Walker':'ATL','Jaxson Hayes':'LAL','C.J. McCollum':'ATL',
  'CJ McCollum':'ATL','Davion Mitchell':'MIA','Neemias Queta':'BOS',
  'Christian Braun':'DEN','Noah Clowney':'BKN','Jalen Johnson':'ATL','Jalen Wilson':'BKN',
  'Jock Landale':'ATL','Ousmane Dieng':'MIL','Cameron Payne':'PHI','Baylor Scheierman':'BOS',
  'Kevin Porter Jr.':'MIL','Corey Kispert':'ATL','Deandre Ayton':'LAL','Pelle Larson':'MIA',
  'Sion James':'CHA','Bruce Brown':'DEN','Luke Kennard':'LAL','Zaccharie Risacher':'ATL',
  'Moe Wagner':'ORL','Russell Westbrook':'SAC','Precious Achiuwa':'SAC','Ryan Rollins':'MIL',
  'Dylan Cardwell':'SAC','Justin Edwards':'PHI','Nique Clifford':'SAC',"Kel'el Ware":'MIA',
  'DeMar DeRozan':'SAC','Darius Garland':'LAC','Jaime Jaquez Jr':'MIA','Maxime Raynaud':'SAC',
  'Kobe Brown':'IND','Isaiah Hartenstein':'OKC','Brandon Williams':'DAL','Quinten Post':'GSW',
  'Kyle Anderson':'MIN','Ajay Mitchell':'OKC','Jaylin Williams':'OKC',"Ja'Kobe Walter":'TOR',
  'Aaron Wiggins':'OKC','Cason Wallace':'OKC','Caris LeVert':'DET','Luguentz Dort':'OKC',
  'Khris Middleton':'DAL',
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
  'Brandon Miller':          { id: 56677823,      position: 'F', team: 'CHA' },
  'Coby White':              { id: 666956,        position: 'G', team: 'CHI' },
  'Marcus Smart':            { id: 420,           position: 'G', team: 'LAL' },
  'Adem Bona':               { id: 1028034846,    position: 'F', team: 'PHI' },
  'Nolan Traore':            { id: 1057275262,    position: 'G', team: 'BKN' },
  'Norman Powell':           { id: 380,           position: 'F', team: 'MIA' },
  'Jonas Valanciunas':       { id: 455,           position: 'C', team: 'DEN' },
  'Jalen Wilson':            { id: 56677722,      position: 'F', team: 'BKN' },
  'Russell Westbrook':       { id: 472,           position: 'G', team: 'SAC' },
  'Precious Achiuwa':        { id: 3547249,       position: 'F', team: 'SAC' },
  'Dylan Cardwell':          { id: 1057845274,    position: 'C', team: 'SAC' },
  'Justin Edwards':          { id: 1028214238,    position: 'F', team: 'PHI' },
  'Nique Clifford':          { id: 1057276634,    position: 'G', team: 'SAC' },
  'DeMar DeRozan':           { id: 125,           position: 'G', team: 'SAC' },
  'Darius Garland':          { id: 666581,        position: 'G', team: 'LAC' },
  'Maxime Raynaud':          { id: 1057390745,    position: 'C', team: 'SAC' },
  'Jaylin Williams':         { id: 38017706,      position: 'F', team: 'OKC' },
  "Ja'Kobe Walter":          { id: 1028029111,    position: 'G', team: 'TOR' },
  'Aaron Wiggins':           { id: 17896078,      position: 'G', team: 'OKC' },
  'Khris Middleton':         { id: 246,           position: 'F', team: 'DAL' },
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

    cacheSet(ck, scores, parseInt(process.env.CACHE_TTL_SCORES) || 900);
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
const ALL_PROP_MARKETS = 'player_points,player_rebounds,player_assists,player_threes,player_points_rebounds_assists,player_rebounds_assists';

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

// Standard normal CDF approximation (Abramowitz & Stegun)
function normalCDF(z) {
  const s = z < 0 ? -1 : 1; z = Math.abs(z);
  const k = 1 / (1 + 0.2316419 * z);
  const poly = k * (0.319381530 + k * (-0.356563782 + k * (1.781477937 + k * (-1.821255978 + k * 1.330274429))));
  const cdf = 1 - (1 / Math.sqrt(2 * Math.PI)) * Math.exp(-0.5 * z * z) * poly;
  return s === 1 ? cdf : 1 - cdf;
}

function guessTeam(name) {
  const map = {
    // ATL
    'trae young':'WAS','bogdan bogdanovic':'ATL','dejounte murray':'ATL',"de'andre hunter":'ATL','onyeka okongwu':'ATL','clint capela':'ATL','dyson daniels':'ATL','larry nance jr.':'ATL','nickeil alexander-walker':'ATL','c.j. mccollum':'ATL','cj mccollum':'ATL','jalen johnson':'ATL','jock landale':'ATL','corey kispert':'ATL','zaccharie risacher':'ATL',
    // BOS
    'jayson tatum':'BOS','jaylen brown':'BOS','jrue holiday':'BOS','al horford':'BOS','kristaps porzingis':'GSW','payton pritchard':'BOS','sam hauser':'BOS','derrick white':'BOS','baylor scheierman':'BOS',
    // BKN
    'cam thomas':'MIL','nic claxton':'BKN',"day'ron sharpe":'BKN','ben simmons':'BKN','danny wolf':'BKN','noah clowney':'BKN','nolan traore':'BKN','jalen wilson':'BKN',
    // CHA
    'lamelo ball':'CHA','brandon miller':'CHA','miles bridges':'CHA','mark williams':'CHA','grant williams':'CHA','josh green':'CHA','sion james':'CHA',
    // CHI
    'zach lavine':'CHI','nikola vucevic':'CHI','coby white':'CHI','josh giddey':'CHI','patrick williams':'CHI','ayo dosunmu':'CHI','tre jones':'CHI',
    // CLE
    'donovan mitchell':'CLE','darius garland':'LAC','evan mobley':'CLE','jarrett allen':'CLE','max strus':'CLE','isaac okoro':'CLE',
    // DAL
    'luka doncic':'DAL','kyrie irving':'DAL','klay thompson':'DAL','p.j. washington':'DAL','dereck lively ii':'DAL','naji marshall':'DAL','dante exum':'DAL',
    // DEN
    'nikola jokic':'DEN','jamal murray':'DEN','michael porter jr.':'DEN','aaron gordon':'DEN','kentavious caldwell-pope':'DEN','reggie jackson':'DEN','christian braun':'DEN','cameron johnson':'DEN','tim hardaway jr':'DEN','jonas valanciunas':'DEN','bruce brown':'DEN',
    // DET
    'cade cunningham':'DET','jalen duren':'DET','ausar thompson':'DET','bojan bogdanovic':'DET','monté morris':'DET','malik beasley':'DET',
    // GSW
    'stephen curry':'GSW','draymond green':'GSW','andrew wiggins':'GSW','jonathan kuminga':'GSW','buddy hield':'GSW','gary payton ii':'GSW',
    // HOU
    'alperen sengun':'HOU','jalen green':'HOU','fred vanvleet':'HOU','jabari smith jr.':'HOU','amen thompson':'HOU','dillon brooks':'HOU','tari eason':'HOU',
    // IND
    'tyrese haliburton':'IND','pascal siakam':'IND','myles turner':'IND','benedict mathurin':'IND','andrew nembhard':'IND','t.j. mcconnell':'IND',
    // LAC
    'kawhi leonard':'LAC','james harden':'LAC','ivica zubac':'IND','norman powell':'MIA','terance mann':'LAC','bones hyland':'LAC','darius garland':'LAC',
    // LAL
    'lebron james':'LAL','anthony davis':'LAL','austin reaves':'LAL',"d'angelo russell":'LAL','rui hachimura':'LAL','max christie':'LAL','gabe vincent':'LAL','marcus smart':'LAL','jaxson hayes':'LAL','deandre ayton':'LAL','luke kennard':'LAL',
    // MEM
    'ja morant':'MEM','desmond bane':'ORL','jaren jackson jr.':'MEM','marcus smart':'LAL','luke kennard':'LAL',
    // MIA
    'bam adebayo':'MIA','tyler herro':'MIA','jimmy butler':'MIA','terry rozier':'MIA','haywood highsmith':'MIA','caleb martin':'MIA','norman powell':'MIA','davion mitchell':'MIA','pelle larson':'MIA',"kel'el ware":'MIA','jaime jaquez jr':'MIA',
    // MIL
    'giannis antetokounmpo':'MIL','damian lillard':'MIL','khris middleton':'MIL','brook lopez':'MIL','bobby portis':'MIL','malik beasley':'MIL','ousmane dieng':'MIL','kevin porter jr.':'MIL','ryan rollins':'MIL','cam thomas':'MIL',
    // MIN
    'anthony edwards':'MIN','rudy gobert':'MIN','jaden mcdaniels':'MIN','naz reid':'MIN','mike conley':'MIN',
    // NOP
    'zion williamson':'NOP','brandon ingram':'NOP','trey murphy iii':'NOP','herb jones':'NOP',
    // NYK
    'jalen brunson':'NYK','karl-anthony towns':'NYK','mikal bridges':'NYK','og anunoby':'NYK','josh hart':'NYK','donte divincenzo':'NYK',
    // OKC
    'shai gilgeous-alexander':'OKC','jalen williams':'OKC','chet holmgren':'OKC','lu dort':'OKC','isaiah joe':'OKC','alex caruso':'OKC',
    // ORL
    'paolo banchero':'ORL','franz wagner':'ORL','wendell carter jr.':'ORL','wendell carter jr':'ORL','cole anthony':'ORL','jalen suggs':'ORL','markelle fultz':'ORL','desmond bane':'ORL','tristan da silva':'ORL','jett howard':'ORL','jevon carter':'ORL',
    // PHI
    'joel embiid':'PHI','tyrese maxey':'PHI','paul george':'PHI','kelly oubre jr.':'PHI','tobias harris':'PHI','quentin grimes':'PHI','adem bona':'PHI','dominick barlow':'PHI','vj edgecombe':'PHI','cameron payne':'PHI','justin edwards':'PHI',
    // PHX
    'devin booker':'PHX','kevin durant':'PHX','bradley beal':'PHX','grayson allen':'PHX','jusuf nurkic':'PHX','eric gordon':'PHX',
    // POR
    'anfernee simons':'POR','scoot henderson':'POR','jerami grant':'POR','deandre ayton':'LAL','shaedon sharpe':'POR',
    // SAC
    "de'aaron fox":'SAC','domantas sabonis':'SAC','keegan murray':'SAC','malik monk':'SAC','harrison barnes':'SAC','russell westbrook':'SAC','precious achiuwa':'SAC','dylan cardwell':'SAC','nique clifford':'SAC','demar derozan':'SAC','maxime raynaud':'SAC',
    // SAS
    'victor wembanyama':'SAS','keldon johnson':'SAS','devin vassell':'SAS','jeremy sochan':'SAS','stephon castle':'SAS','dylan harper':'SAS','julian champagnie':'SAS',
    // TOR
    'scottie barnes':'TOR','rj barrett':'TOR','immanuel quickley':'TOR','gradey dick':'TOR','jakob poeltl':'TOR',
    // UTA
    'lauri markkanen':'UTA','jordan clarkson':'UTA','collin sexton':'UTA','john collins':'UTA','walker kessler':'UTA','keyonte george':'UTA',
    // WAS
    'kyle kuzma':'WAS','bilal coulibaly':'WAS','tyus jones':'WAS','deni avdija':'WAS','marvin bagley iii':'WAS','trae young':'WAS','alex sarr':'WAS','carlton carrington':'WAS','will riley':'WAS','tre johnson':'WAS',
    // BOS (additions)
    'neemias queta':'BOS',
  };
  return map[name.toLowerCase()] || '???';
}


// ============================================================
// INJURY REPORT — fetch & parse NBA official PDF
// ============================================================
const pdfParse = require('pdf-parse');

// NBA team name → abbreviation map
const NBA_TEAM_ABBR = {
  'Atlanta Hawks':'ATL','Boston Celtics':'BOS','Brooklyn Nets':'BKN','Charlotte Hornets':'CHA',
  'Chicago Bulls':'CHI','Cleveland Cavaliers':'CLE','Dallas Mavericks':'DAL','Denver Nuggets':'DEN',
  'Detroit Pistons':'DET','Golden State Warriors':'GSW','Houston Rockets':'HOU','Indiana Pacers':'IND',
  'LA Clippers':'LAC','Los Angeles Clippers':'LAC','Los Angeles Lakers':'LAL','Memphis Grizzlies':'MEM',
  'Miami Heat':'MIA','Milwaukee Bucks':'MIL','Minnesota Timberwolves':'MIN','New Orleans Pelicans':'NOP',
  'New York Knicks':'NYK','Oklahoma City Thunder':'OKC','Orlando Magic':'ORL','Philadelphia 76ers':'PHI',
  'Phoenix Suns':'PHX','Portland Trail Blazers':'POR','Sacramento Kings':'SAC','San Antonio Spurs':'SAS',
  'Toronto Raptors':'TOR','Utah Jazz':'UTA','Washington Wizards':'WAS',
};
const NBA_TEAM_NAMES = Object.keys(NBA_TEAM_ABBR);
// Spaceless versions for pdf-parse v1 (strips spaces)
const NBA_TEAM_NAMES_NOSPACE = NBA_TEAM_NAMES.map(t => t.replace(/\s+/g, ''));
const NBA_TEAM_NOSPACE_MAP = {};
NBA_TEAM_NAMES.forEach(t => { NBA_TEAM_NOSPACE_MAP[t.replace(/\s+/g, '')] = t; });
const INJURY_STATUSES = ['Questionable','Doubtful','Probable','Available','Out'];

function generateInjuryPdfUrl() {
  const now = new Date();
  const et = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const y = et.getFullYear(), m = String(et.getMonth()+1).padStart(2,'0'), d = String(et.getDate()).padStart(2,'0');
  const dateStr = `${y}-${m}-${d}`;
  const times = ['05_00PM','04_30PM','04_00PM','03_30PM','03_00PM','02_30PM','02_00PM','01_30PM','01_00PM','12_30PM','12_00PM','05_30PM'];
  return times.map(t => `https://ak-static.cms.nba.com/referee/injury/Injury-Report_${dateStr}_${t}.pdf`);
}

function parseInjuryText(text) {
  const injuries = [];
  const rawLines = text.split('\n').map(l => l.trim()).filter(Boolean);
  // Pre-process: split lines with embedded matchups (page break merges)
  const lines = [];
  for (const raw of rawLines) {
    const emb = raw.match(/^(.+?)([A-Z]{2,4}@[A-Z]{2,4})(?=[A-Z][a-z])(.+)$/);
    if (emb && !raw.match(/^\d{2}\/\d{2}\/\d{4}/) && emb[1].length > 3) {
      lines.push(emb[1].trim());
      lines.push(emb[2] + emb[3].trim());
    } else {
      lines.push(raw);
    }
  }

  let currentTeam = null, currentTeamAbbr = null, currentMatchup = null;
  let currentGameDate = null, currentGameTime = null;
  let pendingReason = null; // for multi-line reasons

  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    // Skip page headers (spaceless)
    if (line.match(/^InjuryReport:/)) continue;
    if (line.match(/^Page\d+of\d+/)) continue;
    if (line.match(/^GameDateGameTime/)) continue;

    // Attach continuation lines to previous injury (multi-line reasons)
    if (pendingReason !== null && injuries.length > 0) {
      // If line doesn't start a new entry (no comma for name, no team, no date), it's a continuation
      const isNewEntry = line.includes(',') && findStatusInLine(line);
      const isTeamLine = findTeamInLine(line);
      const isDateLine = line.match(/^\d{2}\/\d{2}\/\d{4}/);
      const isMatchupLine = line.match(/^[A-Z]{2,4}@[A-Z]{2,4}/);
      if (!isNewEntry && !isTeamLine && !isDateLine && !isMatchupLine) {
        injuries[injuries.length - 1].reason += ' ' + reinsertSpaces(line);
        continue;
      }
      pendingReason = null;
    }

    // Date line: 03/13/202607:30(ET)CLE@DALClevelandCavaliers...
    const dateMatch = line.match(/^(\d{2}\/\d{2}\/\d{4})(\d{2}:\d{2})\(ET\)([A-Z]{2,4}@[A-Z]{2,4})(?=[A-Z][a-z])(.*)/);
    if (dateMatch) {
      currentGameDate = dateMatch[1];
      currentGameTime = dateMatch[2];
      currentMatchup = dateMatch[3];
      line = dateMatch[4]; // rest has team + possibly player
      // Fall through to team detection
    }

    // Time line: 07:30(ET)CLE@DAL...
    if (!dateMatch) {
      const timeMatch = line.match(/^(\d{2}:\d{2})\(ET\)([A-Z]{2,4}@[A-Z]{2,4})(?=[A-Z][a-z])(.*)/);
      if (timeMatch) {
        currentGameTime = timeMatch[1];
        currentMatchup = timeMatch[2];
        line = timeMatch[3];
      }
    }

    // Matchup line: MEM@DETMemphisGrizzlies...
    if (!dateMatch) {
      const mMatch = line.match(/^([A-Z]{2,4}@[A-Z]{2,4})(?=[A-Z][a-z])(.*)/);
      if (mMatch && line === lines[i]) { // only if not already consumed
        currentMatchup = mMatch[1];
        line = mMatch[2];
      }
    }

    // Try to find a team name (spaceless) at the start of the remaining line
    const teamResult = findTeamInLine(line);
    if (teamResult) {
      currentTeam = teamResult.fullName;
      currentTeamAbbr = NBA_TEAM_ABBR[currentTeam];
      line = teamResult.rest;
      if (!line) continue;
    }

    // Try to parse player entry from remaining text
    if (currentTeam && line) {
      const parsed = parsePlayerEntry(line);
      if (parsed) {
        injuries.push({ ...parsed, team: currentTeamAbbr, teamFull: currentTeam, matchup: currentMatchup, gameDate: currentGameDate, gameTime: currentGameTime });
        pendingReason = parsed.reason;
      }
    }
  }
  return injuries;
}

function findTeamInLine(line) {
  for (const ns of NBA_TEAM_NAMES_NOSPACE) {
    if (line.startsWith(ns)) {
      return { fullName: NBA_TEAM_NOSPACE_MAP[ns], rest: line.substring(ns.length) };
    }
  }
  return null;
}

function findStatusInLine(line) {
  for (const status of INJURY_STATUSES) {
    if (line.includes(status)) return status;
  }
  return null;
}

function reinsertSpaces(text) {
  // Basic re-spacing: insert space before capitals that follow lowercase
  return text.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/([;,])(\w)/g, '$1 $2');
}

function parsePlayerEntry(text) {
  // Text is spaceless like: "Allen,JarrettOutInjury/Illness-RightKnee;Tendonitis"
  // Find status keyword
  for (const status of INJURY_STATUSES) {
    const idx = text.indexOf(status);
    if (idx === -1) continue;
    const namePart = text.substring(0, idx);
    const reasonPart = text.substring(idx + status.length);
    if (!namePart || !namePart.includes(',')) continue;
    // Convert spaceless "Last,First" → "First Last"
    const commaIdx = namePart.indexOf(',');
    const last = namePart.substring(0, commaIdx);
    const first = namePart.substring(commaIdx + 1);
    // Re-insert spaces in names (e.g. "LivelyII" → "Lively II", "Gilgeous-Alexander" stays)
    const cleanLast = last.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/([A-Z])([A-Z][a-z])/g, '$1 $2');
    const cleanFirst = first.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/([A-Z])([A-Z][a-z])/g, '$1 $2');
    // Fix common "Mc" prefix split: "Mc Bride" → "McBride", "Mc Clung" → "McClung"
    const fixMc = (n) => n.replace(/\bMc (\w)/g, (_, c) => 'Mc' + c);
    const displayName = fixMc(`${cleanFirst.trim()} ${cleanLast.trim()}`);
    // Clean up reason: re-insert spaces, strip duplicate status prefix, trailing time patterns
    let reason = reinsertSpaces(reasonPart).trim();
    // Remove leading duplicate status (e.g. "Out Injury/..." → "Injury/...")
    if (reason.startsWith(status + ' ')) reason = reason.substring(status.length + 1).trim();
    if (reason.startsWith(status)) reason = reason.substring(status.length).trim();
    // Remove trailing time patterns like "08:00(ET)" leaked from next game
    reason = reason.replace(/\s*\d{2}:\d{2}\(ET\).*$/, '').trim();
    if (!reason && status === 'Out' && namePart.length < 3) continue; // false positive
    // Skip G-League / Two-Way players
    if (/gleague|g[\s-]*league|two[\s-]*way/i.test(reason)) return null;
    return { player: displayName, playerRaw: `${last}, ${first}`, status, reason: reason || status };
  }
  return null;
}

// In-memory cache for injury data
let _injuryCache = { data: null, fetchedAt: 0 };
const INJURY_CACHE_TTL = 30 * 60 * 1000; // 30 minutes

async function fetchInjuryReport() {
  const now = Date.now();
  if (_injuryCache.data && (now - _injuryCache.fetchedAt) < INJURY_CACHE_TTL) {
    return _injuryCache.data;
  }
  const urls = generateInjuryPdfUrl();
  // Fetch all URLs in parallel with short timeout, take first valid result
  const attempts = urls.map(async (url) => {
    try {
      const resp = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }, timeout: 5000 });
      if (!resp.ok) return null;
      const buf = await resp.buffer();
      const result = await pdfParse(buf);
      const injuries = parseInjuryText(result.text);
      if (injuries.length > 0) {
        console.log(`[injuries] Parsed ${injuries.length} entries from ${url.split('/').pop()}`);
        return injuries;
      }
    } catch (e) {
      console.warn(`[injuries] Failed ${url.split('/').pop()}: ${e.message}`);
    }
    return null;
  });
  const results = await Promise.all(attempts);
  // Use the result with the most entries (latest/most complete report)
  const best = results.filter(Boolean).sort((a, b) => b.length - a.length)[0];
  if (best) {
    _injuryCache = { data: best, fetchedAt: now };
    return best;
  }
  // Return stale cache if available
  if (_injuryCache.data) return _injuryCache.data;
  return [];
}

app.get('/api/injuries', async (req, res) => {
  try {
    const injuries = await fetchInjuryReport();
    const team = req.query.team; // optional filter
    const filtered = team ? injuries.filter(inj => inj.team === team.toUpperCase()) : injuries;
    res.json({ data: filtered, total: injuries.length, filtered: filtered.length });
  } catch (e) {
    console.error('[injuries] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ============================================================
// ROUTE: GET /api/injury-impact — compute injury impact for a player
// Returns: { teammate, ratio, withoutGames, withGames, adjAvg, origAvg }
// ============================================================
app.get('/api/injury-impact', async (req, res) => {
  const { player, team, market } = req.query;
  if (!player || !team || !market) return res.status(400).json({ error: 'player, team, market required' });
  try {
    const injuries = await fetchInjuryReport();
    // Resolve injury report names through alias system
    const resolvedPlayer = resolvePlayerName(player);
    const outTeammates = injuries
      .filter(inj => inj.team === team.toUpperCase() && inj.status === 'Out' && inj.player.toLowerCase() !== player.toLowerCase() && inj.player.toLowerCase() !== resolvedPlayer.toLowerCase())
      .map(inj => ({ ...inj, resolvedName: resolvePlayerName(inj.player) }));
    if (!outTeammates.length) return res.json({ impact: false, reason: 'no OUT teammates' });

    // Fetch player game log (memory → Supabase → live BDL)
    // Try both original name and resolved name
    const playerNames = [resolvedPlayer, player].filter((v, i, a) => a.indexOf(v) === i);
    let playerLog = null;
    for (const pn of playerNames) {
      const ckMem = `gl_name_${NBA_SEASON}_${pn.toLowerCase().replace(/\s+/g,'_')}`;
      playerLog = cacheGet(ckMem);
      if (playerLog) break;
      playerLog = await sbGetGameLog(pn);
      if (playerLog) { cacheSet(ckMem, playerLog, STATS_TTL); break; }
    }
    if (!playerLog) {
      for (const pn of playerNames) {
        try {
          const { id: bdlId, position: bdlPos } = await getBDLPlayerId(pn);
          if (bdlId) {
            playerLog = await fetchBDLGameLog(bdlId);
            if (playerLog?.length) {
              const ckMem = `gl_name_${NBA_SEASON}_${pn.toLowerCase().replace(/\s+/g,'_')}`;
              cacheSet(ckMem, playerLog, STATS_TTL);
              await sbSetGameLog(pn, bdlId, playerLog, bdlPos);
              break;
            }
          }
        } catch (e) { console.warn('[injury-impact] BDL fetch for player failed:', e.message); }
      }
    }
    if (!playerLog?.length) return res.json({ impact: false, reason: 'no player game log' });

    // Fetch game logs for out teammates (Supabase → live BDL fallback)
    // Use resolved names for DB/BDL lookups, map back by injury report name
    const tmLogMap = new Map(); // injury report name → game log
    if (supabase) {
      // Try both original and resolved names
      const allTmNames = [];
      const tmNameMapping = new Map(); // resolved/original → injury report name
      for (const inj of outTeammates) {
        const names = [inj.resolvedName, inj.player].filter((v, i, a) => a.indexOf(v) === i);
        names.forEach(n => { allTmNames.push(n); tmNameMapping.set(n, inj.player); });
      }
      const { data } = await supabase.from('player_stats').select('player_name, game_log, season').eq('season', NBA_SEASON).in('player_name', allTmNames);
      if (data) data.forEach(r => {
        const injName = tmNameMapping.get(r.player_name) || r.player_name;
        tmLogMap.set(injName, r.game_log);
      });
    }
    // For any teammate not found, try live BDL fetch
    for (const inj of outTeammates) {
      if (tmLogMap.has(inj.player)) continue;
      const names = [inj.resolvedName, inj.player].filter((v, i, a) => a.indexOf(v) === i);
      for (const tmName of names) {
        try {
          const { id: tmBdlId, position: tmPos } = await getBDLPlayerId(tmName);
          if (tmBdlId) {
            const tmGames = await fetchBDLGameLog(tmBdlId);
            if (tmGames?.length) {
              tmLogMap.set(inj.player, tmGames);
              await sbSetGameLog(tmName, tmBdlId, tmGames, tmPos);
              break;
            }
          }
        } catch (e) { console.warn(`[injury-impact] BDL fetch for teammate ${tmName} failed:`, e.message); }
      }
    }

    // Stack ALL out teammates — compute independent rate ratios and multiply
    const teamFilter = team.toUpperCase();
    const playerPlayed = playerLog.filter(g => parseInt(g.min || '0') > 0 && (!g.team || g.team === teamFilter));
    const teammateImpacts = [];

    for (const inj of outTeammates) {
      const tmLog = tmLogMap.get(inj.player);
      if (!tmLog?.length) continue;
      const tmPlayed = tmLog.filter(g => parseInt(g.min || '0') > 0);
      if (tmPlayed.length < 5) continue;

      const tmDnpDates = new Set(), tmPlayedDates = new Set();
      for (const g of tmLog) {
        if (g.team && g.team !== teamFilter) continue;
        if (parseInt(g.min || '0') === 0) tmDnpDates.add(g.date);
        else tmPlayedDates.add(g.date);
      }

      // Skip if teammate has been out long enough that recent stats already reflect it
      const l10 = playerPlayed.slice(0, 10);
      const l10Without = l10.filter(g => tmDnpDates.has(g.date)).length;
      if (l10Without >= 7) continue; // 7+ of last 10 already without → impact baked in

      const wo = [], wi = [];
      for (const g of playerPlayed) {
        if (tmDnpDates.has(g.date)) wo.push(g);
        else if (tmPlayedDates.has(g.date)) wi.push(g);
      }

      const tmAvg = tmPlayed.reduce((s, g) => s + _statVal(g, market), 0) / tmPlayed.length;

      if (wo.length >= 3 && wi.length >= 3) {
        // Enough split data — use actual with/without rate comparison
        const wiRates = wi.map(g => _statVal(g, market) / (parseFloat(g.min) || 1));
        const woRates = wo.map(g => _statVal(g, market) / (parseFloat(g.min) || 1));
        const avgWi = wiRates.reduce((a, b) => a + b, 0) / wiRates.length;
        const avgWo = woRates.reduce((a, b) => a + b, 0) / woRates.length;
        if (avgWi === 0) continue;

        const ratio = Math.max(0.70, Math.min(1.30, avgWo / avgWi));
        if (Math.abs(ratio - 1.0) < 0.03) continue;

        teammateImpacts.push({ name: inj.player, ratio: +ratio.toFixed(3), tmAvg: +tmAvg.toFixed(1), wo: wo.length, wi: wi.length, speculative: false });
      } else {
        // Fallback: production share estimate (speculative)
        const playerAvg = playerPlayed.reduce((s, g) => s + _statVal(g, market), 0) / playerPlayed.length;
        if (!tmAvg || !playerAvg) continue;

        // Estimate team total from all available teammate logs
        let teamTotal = playerAvg;
        for (const [tmName, tmGl] of tmLogMap) {
          const played = (tmGl || []).filter(g => parseInt(g.min || '0') > 0);
          if (!played.length) continue;
          const avg = played.reduce((s, g) => s + _statVal(g, market), 0) / played.length;
          if (avg > 0) teamTotal += avg;
        }

        const remainingTotal = teamTotal - tmAvg;
        const playerShare = remainingTotal > 0 ? playerAvg / remainingTotal : 0;
        const boost = tmAvg * 0.35 * playerShare;
        const ratio = (playerAvg + boost) / playerAvg;
        const capped = Math.max(0.70, Math.min(1.20, ratio));
        if (Math.abs(capped - 1.0) < 0.03) continue;

        teammateImpacts.push({ name: inj.player, ratio: +capped.toFixed(3), tmAvg: +tmAvg.toFixed(1), wo: wo.length, wi: wi.length, speculative: true });
      }
    }

    if (!teammateImpacts.length) {
      // Return debug info: which teammates were found and why they were skipped
      const debug = [];
      for (const inj of outTeammates) {
        const tmLog = tmLogMap.get(inj.player);
        if (!tmLog?.length) { debug.push({ name: inj.player, skip: 'no game log in DB' }); continue; }
        const tmPlayed = tmLog.filter(g => parseInt(g.min || '0') > 0);
        if (tmPlayed.length < 5) { debug.push({ name: inj.player, skip: `only ${tmPlayed.length} games played (need 5)` }); continue; }
        // Check if already reflected in recent stats
        const tmDnp = new Set();
        for (const g of tmLog) { if (g.team && g.team !== teamFilter) continue; if (parseInt(g.min || '0') === 0) tmDnp.add(g.date); }
        const l10wo = playerPlayed.slice(0, 10).filter(g => tmDnp.has(g.date)).length;
        if (l10wo >= 7) { debug.push({ name: inj.player, skip: `already reflected in recent stats (${l10wo}/10 recent games without)` }); continue; }
        debug.push({ name: inj.player, skip: 'insufficient split data or ratio < 3%' });
      }
      return res.json({ impact: false, reason: 'no teammate with sufficient data', outTeammates: outTeammates.map(t => t.player), debug });
    }

    // Multiply all individual ratios, cap combined at ±40%
    const combinedRatio = Math.max(0.60, Math.min(1.40,
      teammateImpacts.reduce((prod, t) => prod * t.ratio, 1)
    ));

    const hasSpeculative = teammateImpacts.some(t => t.speculative);
    res.json({
      impact: Math.abs(combinedRatio - 1.0) >= 0.03,
      teammate: teammateImpacts.map(t => t.name).join(' + '),
      teammates: teammateImpacts,
      ratio: +combinedRatio.toFixed(3),
      pctChange: +((combinedRatio - 1) * 100).toFixed(1),
      speculative: hasSpeculative,
    });
  } catch (e) {
    console.error('[injury-impact] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

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

  // Single-player refresh: ?player=Brandon%20Miller
  const singlePlayer = req.query.player;
  if (singlePlayer) {
    try {
      const { id: bdlId, position: bdlPos } = await getBDLPlayerId(singlePlayer);
      if (!bdlId) return res.status(404).json({ error: `${singlePlayer} not found in BDL` });
      const games = await fetchBDLGameLog(bdlId);
      await sbSetGameLog(singlePlayer, bdlId, games, bdlPos);
      const latestTeam = games.sort((a, b) => new Date(b.date) - new Date(a.date))[0]?.team;
      if (latestTeam) _playerTeamCache[singlePlayer] = latestTeam;
      return res.json({ success: true, player: singlePlayer, bdlId, position: bdlPos, games: games.length });
    } catch (e) {
      return res.status(500).json({ error: e.message, player: singlePlayer });
    }
  }

  const results = { ok: [], failed: [], skipped: [], newPlayers: [] };
  try {
    // Get player names from Supabase odds_cache (populated by refresh-odds cron)
    const oddsPlayers = await sbGetOdds('combined');
    if (!oddsPlayers || !oddsPlayers.length) {
      return res.status(503).json({ error: 'No odds data in Supabase — run refresh-odds first' });
    }
    const playerSet = new Set(oddsPlayers.map(p => p.name).filter(Boolean));
    // Note: ungraded bet players no longer included here — getActualStat has BDL fallback for grading
    const playerNames = [...playerSet];

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
        // Skip if already updated within last 12 hours (unless ?force=true)
        if (req.query.force !== 'true' && rec.last_fetched && (now - new Date(rec.last_fetched)) < 12 * 60 * 60 * 1000) {
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

    // Time guard: bail before Vercel's 300s timeout
    const startTime = Date.now();
    const MAX_RUNTIME_MS = 250_000; // 250s — leave 50s buffer
    const isTimedOut = () => (Date.now() - startTime) > MAX_RUNTIME_MS;

    // Process incremental updates — 5 concurrent
    // Each fetches only last 3 days of games, then merges with stored data
    await processBatch(incremental, async ({ name, bdlId, position }) => {
      if (isTimedOut()) { results.skipped.push(name); return; }
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
      if (isTimedOut()) { results.skipped.push(name); return; }
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

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    res.json({ success: true, total: playerNames.length, incremental: incremental.length, newFetched: newBatch.length, skipped: results.skipped.length, elapsed: `${elapsed}s`, timedOut: isTimedOut(), ...results });
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
    case 'player_rebounds_assists':          return (game.reb||0)+(game.ast||0);
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
  let game = record?.game_log?.find(g => g.date === gameDate);

  // BDL fallback: if Supabase doesn't have this date, fetch fresh from BDL
  if (!game) {
    try {
      const bdl = await getBDLPlayerId(playerName);
      if (bdl?.id) {
        const freshGames = await fetchBDLGameLog(bdl.id, gameDate);
        game = freshGames?.find(g => g.date === gameDate);
        // Also update Supabase cache so future lookups don't need BDL
        if (game && record?.game_log) {
          const merged = [...record.game_log.filter(g => g.date !== gameDate), game]
            .sort((a, b) => a.date.localeCompare(b.date));
          await sbSetGameLog(playerName, bdl.id, merged, bdl.position);
        } else if (game) {
          await sbSetGameLog(playerName, bdl.id, freshGames, bdl.position);
        }
      }
    } catch (e) {
      console.warn(`BDL fallback failed for ${playerName}: ${e.message}`);
    }
  }

  if (!game) return { status: 'missing' };
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

    // Fetch all games in parallel (fast — fits within 10s Hobby timeout)
    const rawProps = (await Promise.allSettled(upcoming.map(async evt => {
      const pUrl = `${ODDS_BASE}/sports/basketball_nba/events/${evt.id}/odds?apiKey=${ODDS_KEY}&regions=us&markets=${ALL_PROP_MARKETS}&oddsFormat=american`;
      const pResp = await fetch(pUrl);
      return pResp.ok ? pResp.json() : null;
    }))).filter(r => r.status === 'fulfilled' && r.value).map(r => r.value);

    // Aggregate for all book modes, fetch previous data in parallel
    const books = ['combined', 'draftkings', 'fanduel', 'betmgm', 'caesars', 'pointsbet'];
    const prevData = await Promise.all(books.map(b => sbGetOdds(b)));
    const prevByBook = {};
    books.forEach((b, i) => { prevByBook[b] = prevData[i]; });

    const stored = [];
    const writePromises = [];
    for (const book of books) {
      const players = aggregatePlayers(rawProps, book);
      if (!players.length) continue;
      // Preserve previous lines for movement tracking
      const prevPlayers = prevByBook[book];
      if (prevPlayers && prevPlayers.length) {
        const prevMap = {};
        for (const pp of prevPlayers) {
          prevMap[`${pp.name}|${pp.market}`] = pp;
        }
        for (const p of players) {
          const prev = prevMap[`${p.name}|${p.market}`];
          if (prev) {
            if (prev.line !== p.line || prev.overOdds !== p.overOdds || prev.underOdds !== p.underOdds) {
              p.prev_line = prev.line;
              p.prev_overOdds = prev.overOdds;
              p.prev_underOdds = prev.underOdds;
            } else if (prev.prev_line != null) {
              p.prev_line = prev.prev_line;
              p.prev_overOdds = prev.prev_overOdds;
              p.prev_underOdds = prev.prev_underOdds;
            }
          }
        }
      }
      writePromises.push(sbSetOdds(book, players));
      stored.push({ book, players: players.length });
    }

    // Write all books in parallel
    await Promise.all(writePromises);

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
    const { error: upsertErr } = await supabase.from('user_profiles').upsert(updates, { onConflict: 'id' });
    if (upsertErr) return res.status(500).json({ error: upsertErr.message });
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
        const MKT_SHORT = { player_points:'Pts', player_rebounds:'Reb', player_assists:'Ast', player_threes:'3PM', player_steals:'Stl', player_blocks:'Blk', player_turnovers:'TO', player_points_rebounds_assists:'PRA', player_rebounds_assists:'R+A' };
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
//     placed_at DATE NOT NULL,
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
//   CREATE INDEX IF NOT EXISTS idx_agent_bets_date ON agent_bets(placed_at);
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
  const top20 = valueBets.slice(0, 20);
  const fmtOdds = o => o > 0 ? `+${o}` : `${o}`;
  const dateStr = new Date(date + 'T12:00:00').toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });
  const dirIcon = d => d === 'over' ? '🟢 OVER' : '🔴 UNDER';
  const mktLabel = m => MKT_LABELS[m] || m;

  // Summary embed
  const embeds = [{
    title: `🏀 Value Alert Agent — ${dateStr}`,
    description: `Showing **top 20** of ${valueBets.length} value picks (sorted by Value Score).`,
    color: 0x6366f1,
    timestamp: new Date().toISOString(),
  }];

  // Top 20 picks split into two embeds (10 each) to stay within Discord limits
  const pickLines = (picks, startRank) => picks.map((b, i) => {
    const rank = startRank + i + 1;
    const medal = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : `**#${rank}**`;
    return `${medal} **${b.player_name}** — ${dirIcon(b.direction)} ${b.line} ${mktLabel(b.market)}\n` +
      `　VS: \`${b.value_score}\` · EV: \`${b.dir_ev > 0 ? '+' : ''}${b.dir_ev}%\` · Odds: \`${fmtOdds(b.odds)}\` · Stake: \`$${b.stake.toFixed(2)}\``;
  }).join('\n\n');

  const first10 = top20.slice(0, 10);
  const second10 = top20.slice(10, 20);

  embeds.push({
    title: '⭐ Top Picks #1–10',
    description: pickLines(first10, 0),
    color: 0xf59e0b,
  });

  if (second10.length) {
    embeds.push({
      title: '⭐ Top Picks #11–20',
      description: pickLines(second10, 10),
      color: 0xf59e0b,
    });
  }

  await postDiscord(DISCORD_PICKS_WEBHOOK, {
    content: '@everyone 🚨 **New picks are in!** Top 20 value plays for today.',
    embeds,
  });
}

async function sendDiscordResults(allBets, latestDate) {
  if (!DISCORD_RESULTS_WEBHOOK || !allBets?.length) return;

  const fmtOdds = o => o > 0 ? `+${o}` : `${o}`;
  const pnlFmt = v => `${v >= 0 ? '+' : '-'}$${Math.abs(v).toFixed(2)}`;
  const dateStr = new Date(latestDate + 'T12:00:00').toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });

  const yesterdayBets = allBets.filter(b => b.placed_at === latestDate);
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

  // Top 20 pick-by-pick results (by VS)
  const settled = yesterdayValue.filter(b => b.status === 'won' || b.status === 'lost');
  const top20 = [...settled].sort((a, b) => b.value_score - a.value_score).slice(0, 20);
  const dirIcon = d => d === 'over' ? '🟢 OVER' : '🔴 UNDER';
  const pnlIcon = v => v >= 0 ? `🟢 ${pnlFmt(v)}` : `🔴 ${pnlFmt(v)}`;
  const fmtPick = (b, i) => {
    const actual = b.actual_result != null ? b.actual_result : '—';
    const resultIcon = b.status === 'won' ? '✅' : '❌';
    return `${resultIcon} **#${i+1} ${b.player_name}** — ${dirIcon(b.direction)} ${b.line} ${(MKT_LABELS[b.market]||b.market)}\n` +
      `　VS: \`${b.value_score}\` · Actual: \`${actual}\` · ${fmtOdds(b.odds)} → **${pnlIcon(b.pnl || 0)}**`;
  };
  if (top20.length) {
    const first10 = top20.slice(0, 10);
    const second10 = top20.slice(10, 20);
    const top20Won = top20.filter(b => b.status === 'won').length;
    const top20Pnl = top20.reduce((s, b) => s + (b.pnl || 0), 0);
    embeds.push({
      title: `🏆 Top 20 Picks — ${top20Won}-${top20.length - top20Won} (${pnlFmt(top20Pnl)})`,
      description: first10.map((b, i) => fmtPick(b, i)).join('\n\n'),
      color: 0xf59e0b,
    });
    if (second10.length) {
      embeds.push({
        description: second10.map((b, i) => fmtPick(b, i + 10)).join('\n\n'),
        color: 0xf59e0b,
      });
    }
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
    const mk = (MKT_LABELS[b.market]||b.market) || 'PTS';
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

  // Value Score tier breakdown
  if (settledValue.length >= 10) {
    const vsTiers = [
      { label: 'VS 50+', min: 50, max: Infinity },
      { label: 'VS 40–49', min: 40, max: 49 },
      { label: 'VS 30–39', min: 30, max: 39 },
    ];
    const tierLines = vsTiers.map(t => {
      const inTier = settledValue.filter(b => b.value_score >= t.min && b.value_score <= t.max);
      if (!inTier.length) return null;
      const won = inTier.filter(b => b.status === 'won').length;
      const lost = inTier.length - won;
      const wr = (won / inTier.length * 100).toFixed(1);
      const tierPnl = inTier.reduce((s, b) => s + (b.pnl || 0), 0);
      const tierStaked = inTier.reduce((s, b) => s + b.stake, 0);
      const roi = tierStaked ? (tierPnl / tierStaked * 100).toFixed(1) : '0.0';
      const icon = parseFloat(roi) >= 0 ? '🟢' : '🔴';
      return `${icon} **${t.label}**: ${won}-${lost} (${wr}%) · ROI ${roi}% · ${pnlFmt(tierPnl)}`;
    }).filter(Boolean).join('\n');

    if (tierLines) {
      embeds.push({
        title: '📊 Value Score Tiers',
        description: tierLines,
        color: 0x8b5cf6,
        footer: { text: 'Higher VS tiers should show stronger edge over time' },
      });
    }
  }

  // Flagged vs clean player breakdown
  if (settledValue.length >= 10) {
    const flagged = settledValue.filter(b => b.has_flags);
    const clean = settledValue.filter(b => !b.has_flags);
    const bucketLine = (label, bets, emoji) => {
      if (!bets.length) return null;
      const won = bets.filter(b => b.status === 'won').length;
      const lost = bets.length - won;
      const wr = (won / bets.length * 100).toFixed(1);
      const bPnl = bets.reduce((s, b) => s + (b.pnl || 0), 0);
      const bStaked = bets.reduce((s, b) => s + b.stake, 0);
      const roi = bStaked ? (bPnl / bStaked * 100).toFixed(1) : '0.0';
      const icon = parseFloat(roi) >= 0 ? '🟢' : '🔴';
      return `${emoji} **${label}**: ${won}-${lost} (${wr}%) · ROI ${roi}% · ${pnlFmt(bPnl)}`;
    };
    const lines = [
      bucketLine('Clean Players', clean, '✅'),
      bucketLine('Flagged Players', flagged, '⚠️'),
    ].filter(Boolean).join('\n');
    if (lines) {
      embeds.push({
        title: '🚩 Clean vs Flagged Players',
        description: lines + '\n\n_Flags: 5+ DNPs in last 10, ±20% minutes shift, or returning from injury_',
        color: 0xef4444,
      });
    }
  }

  // Margin analysis — how much are we winning/losing by?
  if (settledValue.length >= 5) {
    const withMargin = settledValue.filter(b => b.actual_result != null && b.line != null);
    if (withMargin.length) {
      const wonBets = withMargin.filter(b => b.status === 'won');
      const lostBets = withMargin.filter(b => b.status === 'lost');

      const calcMargin = (b) => {
        const actual = +b.actual_result;
        const line = +b.line;
        return b.direction === 'over' ? actual - line : line - actual;
      };

      const avgWonMargin = wonBets.length ? (wonBets.reduce((s, b) => s + calcMargin(b), 0) / wonBets.length).toFixed(1) : '0';
      const avgLostMargin = lostBets.length ? (lostBets.reduce((s, b) => s + calcMargin(b), 0) / lostBets.length).toFixed(1) : '0';

      // Cover distribution
      const coverBy = (threshold) => wonBets.filter(b => calcMargin(b) >= threshold).length;
      const cover1 = coverBy(1), cover3 = coverBy(3), cover5 = coverBy(5);

      // Best and worst bets of the day (from yesterday)
      const yWithMargin = yesterdayValue.filter(b => (b.status === 'won' || b.status === 'lost') && b.actual_result != null);
      const sorted = [...yWithMargin].sort((a, b) => calcMargin(b) - calcMargin(a));
      const best3 = sorted.slice(0, 3);
      const worst3 = sorted.slice(-3).reverse();

      let marginDesc = `📏 **Avg Win Margin**: +${avgWonMargin} past the line\n` +
        `📏 **Avg Loss Margin**: ${avgLostMargin} short of the line\n\n` +
        `**Cover Distribution** (${wonBets.length} wins):\n` +
        `　Won by 1+: \`${cover1}\` (${(cover1/Math.max(wonBets.length,1)*100).toFixed(0)}%)\n` +
        `　Won by 3+: \`${cover3}\` (${(cover3/Math.max(wonBets.length,1)*100).toFixed(0)}%)\n` +
        `　Won by 5+: \`${cover5}\` (${(cover5/Math.max(wonBets.length,1)*100).toFixed(0)}%)`;

      if (best3.length) {
        marginDesc += '\n\n**Best Bets Today:**\n' + best3.map(b => {
          const m = calcMargin(b);
          return `🔥 **${b.player_name}** ${b.direction === 'over' ? '▲' : '▼'} ${b.line} ${(MKT_LABELS[b.market]||b.market)} — Actual: \`${b.actual_result}\` (${m >= 0 ? '+' : ''}${m.toFixed(1)})`;
        }).join('\n');
      }
      if (worst3.length) {
        marginDesc += '\n\n**Worst Bets Today:**\n' + worst3.map(b => {
          const m = calcMargin(b);
          return `💀 **${b.player_name}** ${b.direction === 'over' ? '▲' : '▼'} ${b.line} ${(MKT_LABELS[b.market]||b.market)} — Actual: \`${b.actual_result}\` (${m >= 0 ? '+' : ''}${m.toFixed(1)})`;
        }).join('\n');
      }

      embeds.push({
        title: '📏 Margin Analysis',
        description: marginDesc,
        color: 0x06b6d4,
      });
    }
  }

  // Edge accuracy — do higher-edge bets actually win more?
  if (settledValue.length >= 10) {
    const edgeTiers = [
      { label: '10%+ Edge', min: 10 },
      { label: '5-10% Edge', min: 5, max: 10 },
      { label: '2-5% Edge', min: 2, max: 5 },
      { label: '<2% Edge', min: 0, max: 2 },
    ];
    const edgeLines = edgeTiers.map(t => {
      const inTier = settledValue.filter(b => {
        const e = Math.abs(b.edge || 0);
        return e >= t.min && (t.max == null || e < t.max);
      });
      if (!inTier.length) return null;
      const won = inTier.filter(b => b.status === 'won').length;
      const wr = (won / inTier.length * 100).toFixed(1);
      const tierPnl = inTier.reduce((s, b) => s + (b.pnl || 0), 0);
      const icon = parseFloat(wr) >= 52 ? '🟢' : '🔴';
      return `${icon} **${t.label}**: ${won}-${inTier.length - won} (${wr}%) · ${pnlFmt(tierPnl)}`;
    }).filter(Boolean).join('\n');

    if (edgeLines) {
      embeds.push({
        title: '🎯 Edge Accuracy',
        description: edgeLines + '\n\n_Higher edge should correlate with higher win rate_',
        color: 0x14b8a6,
      });
    }
  }

  // Over vs Under split
  if (settledValue.length >= 10) {
    const overBets = settledValue.filter(b => b.direction === 'over');
    const underBets = settledValue.filter(b => b.direction === 'under');
    const splitLine = (label, bets, emoji) => {
      if (!bets.length) return null;
      const won = bets.filter(b => b.status === 'won').length;
      const wr = (won / bets.length * 100).toFixed(1);
      const splitPnl = bets.reduce((s, b) => s + (b.pnl || 0), 0);
      const splitStaked = bets.reduce((s, b) => s + b.stake, 0);
      const roi = splitStaked ? (splitPnl / splitStaked * 100).toFixed(1) : '0.0';
      return `${emoji} **${label}**: ${won}-${bets.length - won} (${wr}%) · ROI ${roi}% · ${pnlFmt(splitPnl)}`;
    };
    const overLine = splitLine('OVER', overBets, '🟢');
    const underLine = splitLine('UNDER', underBets, '🔴');
    if (overLine || underLine) {
      embeds.push({
        title: '↕️ Over vs Under',
        description: [overLine, underLine].filter(Boolean).join('\n'),
        color: 0x6366f1,
      });
    }
  }

  // Home vs Away split
  if (settledValue.length >= 10) {
    // Infer from bet data — check if we stored home/away info
    const homeBets = settledValue.filter(b => b.is_home === true);
    const awayBets = settledValue.filter(b => b.is_home === false);
    if (homeBets.length >= 3 && awayBets.length >= 3) {
      const splitLine2 = (label, bets, emoji) => {
        const won = bets.filter(b => b.status === 'won').length;
        const wr = (won / bets.length * 100).toFixed(1);
        const splitPnl = bets.reduce((s, b) => s + (b.pnl || 0), 0);
        const splitStaked = bets.reduce((s, b) => s + b.stake, 0);
        const roi = splitStaked ? (splitPnl / splitStaked * 100).toFixed(1) : '0.0';
        return `${emoji} **${label}**: ${won}-${bets.length - won} (${wr}%) · ROI ${roi}% · ${pnlFmt(splitPnl)}`;
      };
      embeds.push({
        title: '🏠 Home vs Away',
        description: splitLine2('Home', homeBets, '🏠') + '\n' + splitLine2('Away', awayBets, '✈️'),
        color: 0xf97316,
      });
    }
  }

  // Injury Impact Performance
  {
    const injAdjusted = settledValue.filter(b => b.injury_adj_pct != null && b.injury_adj_pct !== 0);
    const injNone = settledValue.filter(b => b.injury_adj_pct == null || b.injury_adj_pct === 0);
    if (injAdjusted.length >= 3) {
      const injWon = injAdjusted.filter(b => b.status === 'won').length;
      const injWr = (injWon / injAdjusted.length * 100).toFixed(1);
      const injPnl = injAdjusted.reduce((s, b) => s + (b.pnl || 0), 0);
      const injStaked = injAdjusted.reduce((s, b) => s + b.stake, 0);
      const injRoi = injStaked ? (injPnl / injStaked * 100).toFixed(1) : '0.0';

      const noInjWon = injNone.length ? injNone.filter(b => b.status === 'won').length : 0;
      const noInjWr = injNone.length ? (noInjWon / injNone.length * 100).toFixed(1) : '0.0';
      const noInjPnl = injNone.reduce((s, b) => s + (b.pnl || 0), 0);
      const noInjStaked = injNone.reduce((s, b) => s + b.stake, 0);
      const noInjRoi = noInjStaked ? (noInjPnl / noInjStaked * 100).toFixed(1) : '0.0';

      let injDesc = `🏥 **Injury-Adjusted**: ${injWon}-${injAdjusted.length - injWon} (${injWr}%) · ROI ${injRoi}% · ${pnlFmt(injPnl)}\n` +
        `✅ **No Adjustment**: ${noInjWon}-${injNone.length - noInjWon} (${noInjWr}%) · ROI ${noInjRoi}% · ${pnlFmt(noInjPnl)}`;

      // Speculative vs data-backed
      const specBets = injAdjusted.filter(b => b.injury_speculative);
      const dataBets = injAdjusted.filter(b => !b.injury_speculative);
      if (specBets.length >= 2 && dataBets.length >= 2) {
        const specWr = (specBets.filter(b => b.status === 'won').length / specBets.length * 100).toFixed(1);
        const dataWr = (dataBets.filter(b => b.status === 'won').length / dataBets.length * 100).toFixed(1);
        injDesc += `\n\n📊 **Data-Backed** (3+ w/wo games): ${dataWr}% WR (${dataBets.length} bets)\n` +
          `🔮 **Speculative** (production share est): ${specWr}% WR (${specBets.length} bets)`;
      }

      // Directional accuracy — did adjustment move avg closer to actual?
      const withActual = injAdjusted.filter(b => b.actual_result != null && b.injury_adj_pct != null);
      if (withActual.length >= 3) {
        let correctDir = 0;
        for (const b of withActual) {
          const adjPct = b.injury_adj_pct / 100; // e.g. +8% = 0.08
          const origAvg = b.line; // rough proxy — line is close to unadjusted avg
          const adjDirection = adjPct > 0 ? 'boosted' : 'reduced';
          const actualVsLine = +b.actual_result - b.line;
          // Correct if boost and actual > line, or reduce and actual < line
          if ((adjPct > 0 && actualVsLine > 0) || (adjPct < 0 && actualVsLine < 0)) correctDir++;
        }
        const dirAccuracy = (correctDir / withActual.length * 100).toFixed(0);
        injDesc += `\n\n🎯 **Directional Accuracy**: ${dirAccuracy}% of injury adjustments moved the right way (${correctDir}/${withActual.length})`;
      }

      embeds.push({
        title: '🏥 Injury Impact Performance',
        description: injDesc,
        color: 0xec4899,
        footer: { text: 'Tracks whether teammate injury adjustments improve bet accuracy' },
      });
    }
  }

  // Pre-Tipoff vs Placement Injury Changes
  {
    const withTipoff = settledValue.filter(b => b.tipoff_injury_adj_pct !== null && b.tipoff_injury_adj_pct !== undefined);
    if (withTipoff.length >= 3) {
      // Bets where injury picture changed between placement and tipoff
      const changed = withTipoff.filter(b => {
        const placementAdj = b.injury_adj_pct || 0;
        const tipoffAdj = b.tipoff_injury_adj_pct || 0;
        return Math.abs(tipoffAdj - placementAdj) >= 1; // 1%+ change
      });
      const bigChange = withTipoff.filter(b => {
        const placementAdj = b.injury_adj_pct || 0;
        const tipoffAdj = b.tipoff_injury_adj_pct || 0;
        return Math.abs(tipoffAdj - placementAdj) >= 3; // 3%+ change
      });
      // New injuries: no adjustment at placement, adjustment at tipoff
      const newInjury = withTipoff.filter(b => (!b.injury_adj_pct || b.injury_adj_pct === 0) && b.tipoff_injury_adj_pct && b.tipoff_injury_adj_pct !== 0);

      const wrFmt = (bets) => {
        if (!bets.length) return 'N/A';
        const won = bets.filter(b => b.result === 'won').length;
        const pnl = bets.reduce((s, b) => s + (b.pnl || 0), 0);
        const staked = bets.reduce((s, b) => s + b.stake, 0);
        const roi = staked ? (pnl / staked * 100).toFixed(1) : '0.0';
        return `${won}-${bets.length - won} (${(won / bets.length * 100).toFixed(1)}%) · ROI ${roi}% · ${pnlFmt(pnl)}`;
      };

      let desc = `📊 **Bets tracked**: ${withTipoff.length} bets with pre-tipoff snapshots\n`;
      desc += `🔄 **Injury changed (1%+)**: ${changed.length} bets · ${wrFmt(changed)}\n`;
      desc += `⚠️ **Large shift (3%+)**: ${bigChange.length} bets · ${wrFmt(bigChange)}\n`;
      desc += `🆕 **New injury after placement**: ${newInjury.length} bets · ${wrFmt(newInjury)}\n`;

      // Compare: unchanged vs changed
      const unchanged = withTipoff.filter(b => {
        const placementAdj = b.injury_adj_pct || 0;
        const tipoffAdj = b.tipoff_injury_adj_pct || 0;
        return Math.abs(tipoffAdj - placementAdj) < 1;
      });
      desc += `\n✅ **Unchanged**: ${wrFmt(unchanged)}\n🔄 **Changed**: ${wrFmt(changed)}`;

      embeds.push({
        title: '⏰ Pre-Tipoff Injury Shift Analysis',
        description: desc,
        color: 0x8b5cf6,
        footer: { text: 'Compares injury state at bet placement vs near tip-off — helps decide if a 2nd cron run is needed' },
      });
    }
  }

  // VS Component Breakdown — median-split lift analysis
  {
    const vsAnalyzable = settledValue.filter(b => b.model_prob != null && b.confidence != null && b.dir_ev != null);
    if (vsAnalyzable.length >= 10) {
      const median = arr => { const s = [...arr].sort((a, b) => a - b); const m = Math.floor(s.length / 2); return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
      const wr = bets => bets.length ? (bets.filter(b => b.status === 'won').length / bets.length * 100) : 0;
      const components = [
        { name: 'Model Prob', weight: 20, key: 'model_prob', scale: v => v * 100 },
        { name: 'Confidence', weight: 40, key: 'confidence', scale: v => v },
        { name: 'EV', weight: 40, key: 'dir_ev', scale: v => v },
      ];
      let desc = `📊 **${vsAnalyzable.length} settled bets** — median split on each VS component\n\n`;
      const lifts = [];
      for (const c of components) {
        const vals = vsAnalyzable.map(b => c.scale(b[c.key]));
        const med = median(vals);
        const above = vsAnalyzable.filter(b => c.scale(b[c.key]) >= med);
        const below = vsAnalyzable.filter(b => c.scale(b[c.key]) < med);
        const aboveWr = wr(above);
        const belowWr = wr(below);
        const lift = aboveWr - belowWr;
        lifts.push({ ...c, lift, aboveWr, belowWr, med, aboveN: above.length, belowN: below.length });
        const liftIcon = lift > 5 ? '🟢' : lift < -5 ? '🔴' : '🟡';
        desc += `${liftIcon} **${c.name}** (${c.weight}% weight)\n`;
        desc += `  Above median: ${aboveWr.toFixed(1)}% WR (${above.length}) · Below: ${belowWr.toFixed(1)}% WR (${below.length})\n`;
        desc += `  Lift: **${lift > 0 ? '+' : ''}${lift.toFixed(1)}pp** · Median: ${med.toFixed(1)}\n\n`;
      }
      // Recommendation
      const sorted = [...lifts].sort((a, b) => b.lift - a.lift);
      const best = sorted[0];
      const worst = sorted[sorted.length - 1];
      if (best.lift - worst.lift > 5) {
        desc += `💡 **${best.name}** (lift +${best.lift.toFixed(1)}pp) is the most predictive — consider increasing its weight.\n`;
        desc += `**${worst.name}** (lift ${worst.lift > 0 ? '+' : ''}${worst.lift.toFixed(1)}pp) is least predictive — consider decreasing.`;
      } else {
        desc += `💡 Components are performing similarly — current weights look balanced.`;
      }
      embeds.push({
        title: '⚖️ Value Score Component Breakdown',
        description: desc,
        color: 0x6366f1,
        footer: { text: `Weights: MP ${components[0].weight}% · Conf ${components[1].weight}% · EV ${components[2].weight}% — higher lift = more predictive` },
      });
    }
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
    const dates = [...new Set(settledValue.map(b => b.placed_at))].sort().reverse();
    if (dates.length >= 7) {
      const recent = settledValue.filter(b => dates.slice(0, 7).includes(b.placed_at));
      const prior = settledValue.filter(b => !dates.slice(0, 7).includes(b.placed_at));
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

const MKT_LABELS = { player_points: 'PTS', player_rebounds: 'REB', player_assists: 'AST', player_threes: '3PM', player_points_rebounds_assists: 'PRA', player_rebounds_assists: 'R+A' };
const STAT_KEYS  = { player_points: 'pts', player_rebounds: 'reb', player_assists: 'ast', player_threes: 'fg3m', player_points_rebounds_assists: 'pra', player_rebounds_assists: 'ra' };

// Server-side value scoring (mirrors client logic exactly)
function serverValueScore(modelProb, confidence, dirEV) {
  const evScore = Math.min(Math.max(dirEV, -20), 20);
  const mpPts = modelProb * 100; // 0-100 scale
  return Math.round(mpPts * 0.20 + (evScore + 20) * 0.40 + confidence * 0.40);
}

function serverComputeStats(gameLog, line, market, overOdds, underOdds) {
  const statKey = STAT_KEYS[market] || 'pts';
  const getVal = g => statKey === 'pra' ? (+g.pts || 0) + (+g.reb || 0) + (+g.ast || 0)
    : statKey === 'ra' ? (+g.reb || 0) + (+g.ast || 0)
    : +(g[statKey] || 0);
  const sorted = [...gameLog].sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
  const played = sorted.filter(g => parseInt(g.min || '0') > 0);
  if (!played.length) return null;

  const vals = played.map(g => getVal(g));
  const l10 = vals.slice(0, 10);
  const l20 = vals.slice(0, 20);
  const l5 = vals.slice(0, 5);
  const avg = +(vals.reduce((a, b) => a + b, 0) / vals.length).toFixed(1);
  const hitRate = line ? Math.round(vals.filter(v => v >= line).length / vals.length * 100) : 50;
  const edge = line ? +((avg - line) / line * 100).toFixed(1) : 0;
  const l20avg = l20.reduce((a, b) => a + b, 0) / l20.length;
  const stdDev = +(Math.sqrt(l20.reduce((s, v) => s + Math.pow(v - l20avg, 2), 0) / l20.length)).toFixed(1);
  const cv = avg ? stdDev / avg : 0.4;

  const l10HitRate = line && l10.length ? Math.round(l10.filter(v => v >= line).length / l10.length * 100) : hitRate;
  const l5HitRate = (l5.length >= 3 && line) ? Math.round(l5.filter(v => v >= line).length / l5.length * 100) : l10HitRate;

  // Statistical probability via normal distribution (matches client-side formula)
  const statProb = line != null && stdDev > 0 ? normalCDF((avg - line) / stdDev) : 0.5;
  const rawProb = (l5HitRate * 0.25 + l10HitRate * 0.30 + hitRate * 0.25) / 100 + statProb * 0.20;
  const modelProb = Math.min(0.97, Math.max(0.03, (rawProb * vals.length + 5) / (vals.length + 10)));

  const evOver = calcEV(modelProb, overOdds);
  const evUnder = calcEV(1 - modelProb, underOdds);

  const isOverBet = edge >= 0;
  const dirModelProb = isOverBet ? modelProb : 1 - modelProb;
  const dirImplied = isOverBet ? impliedProb(overOdds) : impliedProb(underOdds);
  const evEdge = dirModelProb - dirImplied;

  // Confidence: model vs book (25pts) + alignment (25pts) + sharpness (20pts) + consistency (20pts) + sample (10pts)
  const evEdgePts = Math.min(25, Math.max(0, evEdge / 0.15 * 12.5 + 12.5));
  // Alignment: how well L5/L10/season hit rates agree (low spread = high alignment)
  const hrSpread = Math.max(l5HitRate, l10HitRate, hitRate) - Math.min(l5HitRate, l10HitRate, hitRate);
  const alignPts = Math.min(25, Math.max(0, 25 - hrSpread * 0.5));
  // Line sharpness: how lopsided are the odds? Bigger vig gap = sharper line, more points for disagreeing
  const overImpl = impliedProb(overOdds), underImpl = impliedProb(underOdds);
  const vigGap = Math.abs(overImpl - underImpl);
  const sharpPts = Math.min(20, Math.max(0, vigGap * 100));
  // Consistency: low CV = reliable player
  const cvPts = Math.min(20, Math.max(0, (0.6 - Math.min(cv, 0.6)) / 0.4 * 20));
  // Sample size: more games = more trust
  const samplePts = Math.min(10, Math.max(0, (vals.length - 5) / 50 * 10));
  const confidence = Math.max(0, Math.min(100, Math.round(evEdgePts + alignPts + sharpPts + cvPts + samplePts)));

  const direction = isOverBet ? 'over' : 'under';
  const odds = isOverBet ? overOdds : underOdds;
  const dirEV = isOverBet ? evOver : evUnder;
  const vs = serverValueScore(dirModelProb, confidence, dirEV);

  // Detect alert flags: DNPs in last 10, minutes deviation, return from injury
  const rawL10 = sorted.slice(0, 10);
  const dnpCount = rawL10.filter(g => parseInt(g.min || '0') === 0).length;
  const minVals = played.map(g => parseFloat(g.min) || 0).filter(m => m > 0);
  let minutesFlag = 0;
  if (minVals.length >= 5) {
    const seasonAvgMin = minVals.reduce((a, b) => a + b, 0) / minVals.length;
    const recent5Min = minVals.slice(0, 5).reduce((a, b) => a + b, 0) / 5;
    const minDiff = (recent5Min - seasonAvgMin) / seasonAvgMin;
    if (Math.abs(minDiff) >= 0.20) minutesFlag = minDiff;
  }
  const firstPlayedIdx = sorted.findIndex(g => parseInt(g.min || '0') > 0);
  const returnFromInjury = firstPlayedIdx >= 2 ? firstPlayedIdx : 0;
  const hasFlags = dnpCount >= 5 || Math.abs(minutesFlag) >= 0.20 || returnFromInjury >= 2;

  return { avg, hitRate, edge, modelProb, confidence, evOver, evUnder, direction, odds, dirEV, vs, stdDev, gamesPlayed: vals.length, hasFlags, minutesFlag, dnpCount };
}

// Server-side Smart Minutes adjustment (mirrors client applySmartMinutes)
// Recalculates stats as if player will play their L3 avg minutes instead of season avg
function serverApplySmartMinutes(gameLog, line, market, overOdds, underOdds, baseStats) {
  if (!baseStats.minutesFlag) return baseStats;
  const sorted = [...gameLog].sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
  const played = sorted.filter(g => parseInt(g.min || '0') > 0);
  if (played.length < 3) return baseStats;

  const l3 = played.slice(0, 3);
  const l3avg = Math.round(l3.reduce((s, g) => s + (parseFloat(g.min) || 0), 0) / l3.length);
  const minVals = played.map(g => parseFloat(g.min) || 0).filter(m => m > 0);
  const seasonAvg = minVals.reduce((a, b) => a + b, 0) / minVals.length;
  if (!seasonAvg || Math.abs(l3avg - seasonAvg) < 1) return baseStats;

  const ratio = l3avg / seasonAvg;
  const statKey = STAT_KEYS[market] || 'pts';
  const getVal = g => statKey === 'pra' ? (+g.pts || 0) + (+g.reb || 0) + (+g.ast || 0)
    : statKey === 'ra' ? (+g.reb || 0) + (+g.ast || 0) : +(g[statKey] || 0);

  // Compute per-minute rates and blend for adjusted avg
  const rates = played.map(g => { const m = parseFloat(g.min) || 1; return getVal(g) / m; });
  const l5r = rates.slice(0, 5), l10r = rates.slice(0, 10);
  const blendRate = (l5r.length ? l5r.reduce((a, b) => a + b, 0) / l5r.length : 0) * 0.3125
    + (l10r.length ? l10r.reduce((a, b) => a + b, 0) / l10r.length : 0) * 0.375
    + (rates.length ? rates.reduce((a, b) => a + b, 0) / rates.length : 0) * 0.3125;
  const adjAvg = +(blendRate * l3avg).toFixed(1);

  // Recompute model probability with adjusted projection
  const vals = played.map(g => getVal(g));
  const l20 = vals.slice(0, 20);
  const l20avg = l20.reduce((a, b) => a + b, 0) / l20.length;
  const stdDev = Math.sqrt(l20.reduce((s, v) => s + Math.pow(v - l20avg, 2), 0) / l20.length);

  // Adjust hit rates by ratio
  const adjHR = line ? Math.round(vals.filter(v => v * ratio >= line).length / vals.length * 100) : 50;
  const l10vals = vals.slice(0, 10);
  const l10HR = line && l10vals.length ? Math.round(l10vals.filter(v => v * ratio >= line).length / l10vals.length * 100) : adjHR;
  const l5vals = vals.slice(0, 5);
  const l5HR = l5vals.length >= 3 && line ? Math.round(l5vals.filter(v => v * ratio >= line).length / l5vals.length * 100) : l10HR;

  const adjStdDev = stdDev * ratio;
  const statProb = line != null && adjStdDev > 0 ? normalCDF((adjAvg - line) / adjStdDev) : 0.5;
  const rawProb = (l5HR * 0.25 + l10HR * 0.30 + adjHR * 0.25) / 100 + statProb * 0.20;
  const modelProb = Math.min(0.97, Math.max(0.03, (rawProb * vals.length + 5) / (vals.length + 10)));

  const evOver = calcEV(modelProb, overOdds);
  const evUnder = calcEV(1 - modelProb, underOdds);
  const edge = line ? +((adjAvg - line) / line * 100).toFixed(1) : 0;

  const isOverBet = edge >= 0;
  const dirModelProb = isOverBet ? modelProb : 1 - modelProb;
  const dirImplied = isOverBet ? impliedProb(overOdds) : impliedProb(underOdds);
  const evEdge = dirModelProb - dirImplied;
  const cv = adjAvg ? adjStdDev / adjAvg : 0.4;

  // Confidence (smart-min adjusted): same 5-component formula
  const _smEvEdgePts = Math.min(25, Math.max(0, evEdge / 0.15 * 12.5 + 12.5));
  const _smHrSpread = Math.max(l5HR, l10HR, adjHR) - Math.min(l5HR, l10HR, adjHR);
  const _smAlignPts = Math.min(25, Math.max(0, 25 - _smHrSpread * 0.5));
  const _smOverImpl = impliedProb(overOdds), _smUnderImpl = impliedProb(underOdds);
  const _smVigGap = Math.abs(_smOverImpl - _smUnderImpl);
  const _smSharpPts = Math.min(20, Math.max(0, _smVigGap * 100));
  const _smCvPts = Math.min(20, Math.max(0, (0.6 - Math.min(cv, 0.6)) / 0.4 * 20));
  const _smSamplePts = Math.min(10, Math.max(0, (vals.length - 5) / 50 * 10));
  const confidence = Math.max(0, Math.min(100, Math.round(_smEvEdgePts + _smAlignPts + _smSharpPts + _smCvPts + _smSamplePts)));

  const direction = isOverBet ? 'over' : 'under';
  const odds = isOverBet ? overOdds : underOdds;
  const dirEV = isOverBet ? evOver : evUnder;
  const vs = serverValueScore(dirModelProb, confidence, dirEV);

  return { ...baseStats, avg: adjAvg, hitRate: adjHR, edge, modelProb, confidence, evOver, evUnder, direction, odds, dirEV, vs, stdDev: adjStdDev, _smartMin: true, _adjMin: l3avg };
}

// Server-side DNP Adjustment (mirrors client applyDnpAdjustment)
// For players with 5+ DNPs in last 10 games, markdown minutes proportionally
// 5 DNPs = 20% cut, 10 DNPs = 40% cut, linear between
function serverApplyDNP(gameLog, line, market, overOdds, underOdds, baseStats) {
  if (!baseStats.dnpCount || baseStats.dnpCount < 5) return baseStats;
  const sorted = [...gameLog].sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
  const played = sorted.filter(g => parseInt(g.min || '0') > 0);
  if (!played.length) return baseStats;

  const seasonAvg = played.reduce((s, g) => s + (parseFloat(g.min) || 0), 0) / played.length;
  const dnpRatio = Math.min(baseStats.dnpCount, 10);
  const markdownPct = 0.20 + (dnpRatio - 5) * (0.20 / 5); // 20% at 5, 40% at 10
  const adjMin = Math.round(seasonAvg * (1 - markdownPct));

  // Recompute stats using adjusted minutes projection (same approach as Smart Minutes)
  const statKey = STAT_KEYS[market] || 'pts';
  const getVal = g => statKey === 'pra' ? (+g.pts || 0) + (+g.reb || 0) + (+g.ast || 0)
    : statKey === 'ra' ? (+g.reb || 0) + (+g.ast || 0) : +(g[statKey] || 0);

  const rates = played.map(g => { const m = parseFloat(g.min) || 1; return getVal(g) / m; });
  const l5r = rates.slice(0, 5), l10r = rates.slice(0, 10);
  const blendRate = (l5r.length ? l5r.reduce((a, b) => a + b, 0) / l5r.length : 0) * 0.3125
    + (l10r.length ? l10r.reduce((a, b) => a + b, 0) / l10r.length : 0) * 0.375
    + (rates.length ? rates.reduce((a, b) => a + b, 0) / rates.length : 0) * 0.3125;
  const adjAvg = +(blendRate * adjMin).toFixed(1);

  const vals = played.map(g => getVal(g));
  const l20 = vals.slice(0, 20);
  const l20avg = l20.reduce((a, b) => a + b, 0) / l20.length;
  const stdDev = Math.sqrt(l20.reduce((s, v) => s + Math.pow(v - l20avg, 2), 0) / l20.length);
  const ratio = adjMin / seasonAvg;

  const adjHR = line ? Math.round(vals.filter(v => v * ratio >= line).length / vals.length * 100) : 50;
  const l10vals = vals.slice(0, 10);
  const l10HR = line && l10vals.length ? Math.round(l10vals.filter(v => v * ratio >= line).length / l10vals.length * 100) : adjHR;
  const l5vals = vals.slice(0, 5);
  const l5HR = l5vals.length >= 3 && line ? Math.round(l5vals.filter(v => v * ratio >= line).length / l5vals.length * 100) : l10HR;

  const adjStdDev = stdDev * ratio;
  const statProb = line != null && adjStdDev > 0 ? normalCDF((adjAvg - line) / adjStdDev) : 0.5;
  const rawProb = (l5HR * 0.25 + l10HR * 0.30 + adjHR * 0.25) / 100 + statProb * 0.20;
  const modelProb = Math.min(0.97, Math.max(0.03, (rawProb * vals.length + 5) / (vals.length + 10)));

  const evOver = calcEV(modelProb, overOdds);
  const evUnder = calcEV(1 - modelProb, underOdds);
  const edge = line ? +((adjAvg - line) / line * 100).toFixed(1) : 0;

  const isOverBet = edge >= 0;
  const dirModelProb = isOverBet ? modelProb : 1 - modelProb;
  const dirImplied = isOverBet ? impliedProb(overOdds) : impliedProb(underOdds);
  const cv = adjAvg ? adjStdDev / adjAvg : 0.4;

  const _evEdge = dirModelProb - dirImplied;
  const _evEdgePts = Math.min(30, Math.max(0, _evEdge * 300));
  const _alignPts = (edge >= 0 && modelProb >= 0.5) || (edge < 0 && modelProb < 0.5) ? 20 : 0;
  const _vigGap = Math.abs(dirModelProb - dirImplied);
  const _sharpPts = Math.min(20, Math.max(0, _vigGap * 100));
  const _cvPts = Math.min(20, Math.max(0, (0.6 - Math.min(cv, 0.6)) / 0.4 * 20));
  const _samplePts = Math.min(10, Math.max(0, (vals.length - 5) / 50 * 10));
  const confidence = Math.max(0, Math.min(100, Math.round(_evEdgePts + _alignPts + _sharpPts + _cvPts + _samplePts)));

  const direction = isOverBet ? 'over' : 'under';
  const odds = isOverBet ? overOdds : underOdds;
  const dirEV = isOverBet ? evOver : evUnder;
  const vs = serverValueScore(dirModelProb, confidence, dirEV);

  return { ...baseStats, avg: adjAvg, hitRate: adjHR, edge, modelProb, confidence, evOver, evUnder, direction, odds, dirEV, vs, stdDev: adjStdDev, _dnpAdj: true, _adjMin: adjMin, _dnpMarkdown: markdownPct };
}

// ============================================================
// Injury Impact: "Without Teammate" split-based adjustment
// Finds the most impactful injured teammate for a given market,
// computes how the player performs without them, and adjusts stats.
// ============================================================

// Get stat value from a game log entry for a given market
function _statVal(g, market) {
  const sk = STAT_KEYS[market] || 'pts';
  return sk === 'pra' ? (+g.pts || 0) + (+g.reb || 0) + (+g.ast || 0)
    : sk === 'ra' ? (+g.reb || 0) + (+g.ast || 0) : +(g[sk] || 0);
}

/**
 * Compute injury impact adjustment for a player.
 * @param {string} playerName - The player we're evaluating
 * @param {Array} playerGameLog - Player's game log
 * @param {string} playerTeam - Player's team abbreviation
 * @param {string} market - Stat market (player_points, etc.)
 * @param {number} line - The prop line
 * @param {number} overOdds - Over odds
 * @param {number} underOdds - Under odds
 * @param {Array} injuries - Today's injury report array
 * @param {Map} allGameLogs - Map of playerName → gameLog for teammates
 * @param {Object} baseStats - Output from serverComputeStats
 * @returns {Object} Adjusted stats or original baseStats if no impact
 */
function serverApplyInjuryImpact(playerName, playerGameLog, playerTeam, market, line, overOdds, underOdds, injuries, allGameLogs, baseStats) {
  if (!injuries?.length || !playerGameLog?.length) return baseStats;

  // 1. Find teammates who are OUT today
  const outTeammates = injuries.filter(inj =>
    inj.team === playerTeam &&
    inj.status === 'Out' &&
    inj.player !== playerName &&
    inj.player.toLowerCase() !== playerName.toLowerCase()
  );
  if (!outTeammates.length) return baseStats;

  // 2. Stack ALL out teammates — compute independent rate ratios and multiply
  const playerPlayed = playerGameLog.filter(g => parseInt(g.min || '0') > 0 && (!g.team || g.team === playerTeam));
  const teammateImpacts = [];

  for (const inj of outTeammates) {
    // Try resolved name, then original injury report name
    const resolved = resolvePlayerName(inj.player);
    const tmLog = allGameLogs.get(resolved) || allGameLogs.get(inj.player);
    if (!tmLog?.length) continue;
    const tmPlayed = tmLog.filter(g => parseInt(g.min || '0') > 0);
    if (tmPlayed.length < 5) continue;

    // Build DNP/played date sets for this teammate
    const tmDnpDates = new Set();
    const tmPlayedDates = new Set();
    for (const g of tmLog) {
      if (g.team && g.team !== playerTeam) continue;
      if (parseInt(g.min || '0') === 0) tmDnpDates.add(g.date);
      else tmPlayedDates.add(g.date);
    }

    // Skip if teammate has been out long enough that recent stats already reflect it
    // Check how many of the player's last 10 games were without this teammate
    const l10 = playerPlayed.slice(0, 10);
    const l10Without = l10.filter(g => tmDnpDates.has(g.date)).length;
    if (l10Without >= 7) continue; // 7+ of last 10 already without → impact baked in

    // Split player's games into with/without this teammate
    const wo = [], wi = [];
    for (const g of playerPlayed) {
      if (tmDnpDates.has(g.date)) wo.push(g);
      else if (tmPlayedDates.has(g.date)) wi.push(g);
    }

    if (wo.length >= 3 && wi.length >= 3) {
      // Enough split data — use actual with/without rate comparison
      const wiRates = wi.map(g => _statVal(g, market) / (parseFloat(g.min) || 1));
      const woRates = wo.map(g => _statVal(g, market) / (parseFloat(g.min) || 1));
      const avgWi = wiRates.reduce((a, b) => a + b, 0) / wiRates.length;
      const avgWo = woRates.reduce((a, b) => a + b, 0) / woRates.length;
      if (avgWi === 0) continue;

      const ratio = avgWo / avgWi;
      const capped = Math.max(0.70, Math.min(1.30, ratio));
      if (Math.abs(capped - 1.0) < 0.03) continue;

      teammateImpacts.push({ name: inj.player, ratio: capped, withoutGames: wo.length, withGames: wi.length, speculative: false });
    } else {
      // Fallback: production share estimate (speculative)
      const tmAvg = tmPlayed.reduce((s, g) => s + _statVal(g, market), 0) / tmPlayed.length;
      const playerAvg = playerPlayed.reduce((s, g) => s + _statVal(g, market), 0) / playerPlayed.length;
      if (!tmAvg || !playerAvg) continue;

      // Estimate team total from all available game logs on same team
      let teamTotal = 0, teamCount = 0;
      for (const [, gl] of allGameLogs) {
        const played = gl.filter(g => parseInt(g.min || '0') > 0 && (!g.team || g.team === playerTeam));
        if (!played.length) continue;
        const avg = played.reduce((s, g) => s + _statVal(g, market), 0) / played.length;
        if (avg > 0) { teamTotal += avg; teamCount++; }
      }
      if (!teamTotal) continue;

      // ~35% of missing production redistributes, weighted by player's share
      const remainingTotal = teamTotal - tmAvg;
      const playerShare = remainingTotal > 0 ? playerAvg / remainingTotal : 0;
      const boost = tmAvg * 0.35 * playerShare;
      const ratio = (playerAvg + boost) / playerAvg;
      const capped = Math.max(0.70, Math.min(1.20, ratio)); // tighter cap for speculative
      if (Math.abs(capped - 1.0) < 0.03) continue;

      teammateImpacts.push({ name: inj.player, ratio: capped, withoutGames: wo.length, withGames: wi.length, speculative: true });
    }
  }

  if (!teammateImpacts.length) return baseStats;

  // 3. Multiply all individual ratios, cap combined at ±40%
  const combinedRatio = Math.max(0.60, Math.min(1.40,
    teammateImpacts.reduce((prod, t) => prod * t.ratio, 1)
  ));
  if (Math.abs(combinedRatio - 1.0) < 0.03) return baseStats;
  const cappedRatio = combinedRatio;

  // 4. Apply the combined ratio to compute adjusted stats (similar to Smart Minutes)
  const vals = playerPlayed.map(g => _statVal(g, market));
  const adjAvg = +(baseStats.avg * cappedRatio).toFixed(1);

  const l20 = vals.slice(0, 20);
  const l20avg = l20.reduce((a, b) => a + b, 0) / l20.length;
  const stdDev = Math.sqrt(l20.reduce((s, v) => s + Math.pow(v - l20avg, 2), 0) / l20.length);
  const adjStdDev = stdDev * cappedRatio;

  // Recompute hit rates with adjustment
  const adjHR = line ? Math.round(vals.filter(v => v * cappedRatio >= line).length / vals.length * 100) : 50;
  const l10vals = vals.slice(0, 10);
  const l10HR = line && l10vals.length ? Math.round(l10vals.filter(v => v * cappedRatio >= line).length / l10vals.length * 100) : adjHR;
  const l5vals = vals.slice(0, 5);
  const l5HR = l5vals.length >= 3 && line ? Math.round(l5vals.filter(v => v * cappedRatio >= line).length / l5vals.length * 100) : l10HR;

  const statProb = line != null && adjStdDev > 0 ? normalCDF((adjAvg - line) / adjStdDev) : 0.5;
  const rawProb = (l5HR * 0.25 + l10HR * 0.30 + adjHR * 0.25) / 100 + statProb * 0.20;
  const modelProb = Math.min(0.97, Math.max(0.03, (rawProb * vals.length + 5) / (vals.length + 10)));

  const evOver = calcEV(modelProb, overOdds);
  const evUnder = calcEV(1 - modelProb, underOdds);
  const edge = line ? +((adjAvg - line) / line * 100).toFixed(1) : 0;

  const isOverBet = edge >= 0;
  const dirModelProb = isOverBet ? modelProb : 1 - modelProb;
  const dirImplied = isOverBet ? impliedProb(overOdds) : impliedProb(underOdds);
  const evEdge = dirModelProb - dirImplied;
  const cv = adjAvg ? adjStdDev / adjAvg : 0.4;

  // Confidence (injury-adjusted): same 5-component formula
  const _ievEdgePts = Math.min(25, Math.max(0, evEdge / 0.15 * 12.5 + 12.5));
  const _ihrSpread = Math.max(l5HR, l10HR, adjHR) - Math.min(l5HR, l10HR, adjHR);
  const _ialignPts = Math.min(25, Math.max(0, 25 - _ihrSpread * 0.5));
  const _ioverImpl = impliedProb(overOdds), _iunderImpl = impliedProb(underOdds);
  const _ivigGap = Math.abs(_ioverImpl - _iunderImpl);
  const _isharpPts = Math.min(20, Math.max(0, _ivigGap * 100));
  const _icvPts = Math.min(20, Math.max(0, (0.6 - Math.min(cv, 0.6)) / 0.4 * 20));
  const _isamplePts = Math.min(10, Math.max(0, (vals.length - 5) / 50 * 10));
  const confidence = Math.max(0, Math.min(100, Math.round(_ievEdgePts + _ialignPts + _isharpPts + _icvPts + _isamplePts)));

  const direction = isOverBet ? 'over' : 'under';
  const odds = isOverBet ? overOdds : underOdds;
  const dirEV = isOverBet ? evOver : evUnder;
  const vs = serverValueScore(dirModelProb, confidence, dirEV);

  return {
    ...baseStats, avg: adjAvg, hitRate: adjHR, edge, modelProb, confidence,
    evOver, evUnder, direction, odds, dirEV, vs, stdDev: adjStdDev,
    _injuryImpact: true,
    _injuryTeammates: teammateImpacts.map(t => t.name),
    _injuryTeammate: teammateImpacts.map(t => t.name).join(' + '),
    _injuryRatio: +cappedRatio.toFixed(3),
    _injuryBreakdown: teammateImpacts.map(t => ({ name: t.name, ratio: +t.ratio.toFixed(3), wo: t.withoutGames, wi: t.withGames, speculative: t.speculative })),
    _injurySpeculative: teammateImpacts.some(t => t.speculative),
  };
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

  const STARTING_BANKROLL = parseFloat(process.env.AGENT_BANKROLL) || 1000;
  const DAILY_ALLOCATION = 1.00; // 100% of bankroll per day
  const MIN_BET = 2;
  const CONTROL_STAKE = 5; // flat small stake for control bets
  const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });

  // Idempotency: skip if we already placed bets today (bypass with ?force=true)
  const force = req.query.force === 'true';
  if (!force) {
    const { count: existing } = await supabase.from('agent_bets').select('id', { count: 'exact', head: true }).eq('game_date', today);
    if (existing > 0) return res.json({ skipped: true, message: `Already placed ${existing} agent bets for ${today}` });
  }

  const summary = { value: 0, control: 0, skipped: 0, errors: [], bankroll: 0 };

  try {
    // 0. Compute current bankroll from settled bets
    const { data: settledBets } = await supabase.from('agent_bets').select('result, stake, to_win, is_control').not('result', 'is', null);
    let totalPnl = 0;
    for (const b of (settledBets || [])) {
      if (b.result === 'won') totalPnl += (b.to_win || 0);
      else if (b.result === 'lost') totalPnl -= b.stake;
      // void = 0
    }
    const currentBankroll = STARTING_BANKROLL + totalPnl;
    const dailyBudget = currentBankroll * DAILY_ALLOCATION;
    const maxBet = currentBankroll * 0.05;
    summary.bankroll = +currentBankroll.toFixed(2);

    // 1. Check odds cache freshness — refuse to bet on stale data
    const { data: cacheRow } = await supabase.from('odds_cache').select('last_fetched').eq('book', 'combined').single();
    const cacheAge = cacheRow?.last_fetched ? (Date.now() - new Date(cacheRow.last_fetched).getTime()) / (1000 * 60 * 60) : Infinity;
    if (cacheAge > 2) {
      return res.status(400).json({ error: `Odds cache is ${cacheAge === Infinity ? 'missing' : cacheAge.toFixed(1) + 'h old'}. Run refresh-odds first.`, cacheAge: +cacheAge.toFixed(1) });
    }

    const allPlayers = await sbGetOdds('combined');
    if (!allPlayers?.length) return res.status(404).json({ error: 'No odds data in cache' });

    // 2. Filter to only games that haven't started
    const now = new Date();
    const upcoming = allPlayers.filter(p => !p.gameTime || new Date(p.gameTime) > now);

    // 3. Batch-fetch all game logs in one query (avoids per-player Supabase calls)
    const uniqueNames = [...new Set(upcoming.map(p => p.name))];
    const gameLogMap = new Map();
    for (let i = 0; i < uniqueNames.length; i += 50) {
      const batch = uniqueNames.slice(i, i + 50);
      const { data } = await supabase.from('player_stats').select('player_name, game_log, season').eq('season', NBA_SEASON).in('player_name', batch);
      if (data) data.forEach(r => gameLogMap.set(r.player_name, r.game_log));
    }

    // 3b. Fetch injury report and injured teammates' game logs for injury impact
    const injuries = await fetchInjuryReport();
    const injuredTeammateNames = [...new Set(injuries.filter(i => i.status === 'Out').map(i => i.player))];
    // Resolve aliases and fetch game logs for injured players not already in gameLogMap
    const resolvedInjuredNames = injuredTeammateNames.map(n => ({ orig: n, resolved: resolvePlayerName(n) }));
    const missingInjured = resolvedInjuredNames.filter(n => !gameLogMap.has(n.resolved) && !gameLogMap.has(n.orig));
    // Try Supabase first
    if (missingInjured.length && supabase) {
      const namesToFetch = [...new Set(missingInjured.flatMap(n => [n.resolved, n.orig]))];
      for (let i = 0; i < namesToFetch.length; i += 50) {
        const batch = namesToFetch.slice(i, i + 50);
        const { data } = await supabase.from('player_stats').select('player_name, game_log, season').eq('season', NBA_SEASON).in('player_name', batch);
        if (data) data.forEach(r => gameLogMap.set(r.player_name, r.game_log));
      }
    }
    // BDL fallback for injured players still missing from Supabase (3 concurrent)
    const stillMissing = missingInjured.filter(n => !gameLogMap.has(n.resolved) && !gameLogMap.has(n.orig));
    let injuredBdlFetched = 0;
    if (stillMissing.length) {
      console.log(`[agent-bet] ${stillMissing.length} injured teammates missing from Supabase, fetching from BDL: ${stillMissing.map(n => n.resolved).join(', ')}`);
      const fetchBatch = async (items, fn, concurrency) => {
        for (let i = 0; i < items.length; i += concurrency) {
          await Promise.all(items.slice(i, i + concurrency).map(fn));
        }
      };
      await fetchBatch(stillMissing, async ({ resolved }) => {
        try {
          const bdl = await getBDLPlayerId(resolved);
          if (!bdl?.id) return;
          const games = await fetchBDLGameLog(bdl.id);
          if (games?.length) {
            gameLogMap.set(resolved, games);
            await sbSetGameLog(resolved, bdl.id, games, bdl.position);
            injuredBdlFetched++;
          }
        } catch (e) {
          console.warn(`BDL fallback for injured teammate ${resolved}: ${e.message}`);
        }
      }, 3);
      console.log(`[agent-bet] BDL fetched ${injuredBdlFetched}/${stillMissing.length} injured teammates`);
    }

    // 4. For each player/market combo, compute stats and value score
    const valueCandidates = [];
    const controlCandidates = [];
    const processedKeys = new Set();

    for (const p of upcoming) {
      const key = `${p.name}|${p.market}`;
      if (processedKeys.has(key)) continue;
      processedKeys.add(key);

      try {
        const gameLog = gameLogMap.get(p.name);
        if (!gameLog?.length) { summary.skipped++; continue; }

        const rawStats = serverComputeStats(gameLog, p.line, p.market, p.overOdds, p.underOdds);
        if (!rawStats || rawStats.gamesPlayed < 5) { summary.skipped++; continue; }

        // Apply Smart Minutes: recalculate flagged players using L3 avg minutes
        let stats = serverApplySmartMinutes(gameLog, p.line, p.market, p.overOdds, p.underOdds, rawStats);

        // Apply DNP Adjustment: markdown minutes for players with 5+ DNPs in last 10
        stats = serverApplyDNP(gameLog, p.line, p.market, p.overOdds, p.underOdds, stats);

        // Apply Injury Impact: adjust based on most impactful OUT teammate
        const playerTeam = _playerTeamCache[p.name] || guessTeam(p.name) || p.homeTeam;
        if (playerTeam && playerTeam !== '???') {
          stats = serverApplyInjuryImpact(p.name, gameLog, playerTeam, p.market, p.line, p.overOdds, p.underOdds, injuries, gameLogMap, stats);
        }

        const isValue = stats.vs >= 30 && Math.abs(stats.edge) > 4 && stats.confidence >= 55 && stats.dirEV > 3;
        const isControl = !isValue && stats.vs >= 20 && Math.abs(stats.edge) > 2 && stats.dirEV > 0;

        if (!isValue && !isControl) { summary.skipped++; continue; }

        const entry = { p, stats, isControl };
        if (isValue) valueCandidates.push(entry);
        else controlCandidates.push(entry);
      } catch (e) {
        summary.errors.push(`${p.name}: ${e.message}`);
      }
    }

    // 5. Size value bets proportionally by VS
    const totalVS = valueCandidates.reduce((s, c) => s + c.stats.vs, 0);
    const betsToInsert = [];

    for (const c of valueCandidates) {
      const proportion = c.stats.vs / (totalVS || 1);
      const rawStake = +(dailyBudget * proportion).toFixed(2);
      const stake = Math.min(maxBet, Math.max(MIN_BET, rawStake));
      const payout = c.stats.odds > 0 ? +(stake * c.stats.odds / 100).toFixed(2) : +(stake * 100 / Math.abs(c.stats.odds)).toFixed(2);

      betsToInsert.push({
        placed_at: today,
        player_name: c.p.name,
        team: guessTeam(c.p.name) || c.p.homeTeam || null,
        opponent: c.p.matchup || null,
        market: c.p.market,
        line: c.p.line,
        direction: c.stats.direction,
        odds: c.stats.odds,
        model_prob: +c.stats.modelProb.toFixed(4),
        confidence: c.stats.confidence,
        edge: c.stats.edge,
        dir_ev: +c.stats.dirEV.toFixed(1),
        value_score: c.stats.vs,
        is_control: false,
        has_flags: c.stats.hasFlags,
        stake,
        to_win: payout,
        result: null,
        game_date: today,
        game_time: c.p.gameTime || null,
        injury_adj_pct: c.stats._injuryImpact ? +((c.stats._injuryRatio - 1) * 100).toFixed(1) : null,
        injury_speculative: c.stats._injurySpeculative || false,
        injury_teammates: c.stats._injuryTeammate || null,
      });
      summary.value++;
    }

    // 6. Control bets get flat small stake
    for (const c of controlCandidates) {
      const payout = c.stats.odds > 0 ? +(CONTROL_STAKE * c.stats.odds / 100).toFixed(2) : +(CONTROL_STAKE * 100 / Math.abs(c.stats.odds)).toFixed(2);

      betsToInsert.push({
        placed_at: today,
        player_name: c.p.name,
        team: guessTeam(c.p.name) || c.p.homeTeam || null,
        opponent: c.p.matchup || null,
        market: c.p.market,
        line: c.p.line,
        direction: c.stats.direction,
        odds: c.stats.odds,
        model_prob: +c.stats.modelProb.toFixed(4),
        confidence: c.stats.confidence,
        edge: c.stats.edge,
        dir_ev: +c.stats.dirEV.toFixed(1),
        value_score: c.stats.vs,
        is_control: true,
        has_flags: c.stats.hasFlags,
        stake: CONTROL_STAKE,
        to_win: payout,
        result: null,
        game_date: today,
        game_time: c.p.gameTime || null,
        injury_adj_pct: c.stats._injuryImpact ? +((c.stats._injuryRatio - 1) * 100).toFixed(1) : null,
        injury_speculative: c.stats._injurySpeculative || false,
        injury_teammates: c.stats._injuryTeammate || null,
      });
      summary.control++;
    }

    // 7. Bulk insert
    if (betsToInsert.length) {
      const { error: insertErr } = await supabase.from('agent_bets').insert(betsToInsert);
      if (insertErr) return res.status(500).json({ error: insertErr.message, summary });
    }

    // 8. Send Discord notification
    await sendDiscordPicks(betsToInsert, today);

    const totalStaked = betsToInsert.filter(b => !b.is_control).reduce((s, b) => s + b.stake, 0);
    res.json({ success: true, date: today, ...summary, total: betsToInsert.length, bankroll: +currentBankroll.toFixed(2), dailyBudget: +dailyBudget.toFixed(2), totalStaked: +totalStaked.toFixed(2) });
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

  const forceDate = req.query.date; // e.g. ?date=2026-03-14 — filter ungraded bets by this game_date
  const lookupDate = req.query.lookup_date; // e.g. ?lookup_date=2026-03-13 — override date for stat lookup
  const GRADE_AFTER_MS = 10 * 60 * 60 * 1000;
  const now = Date.now();
  const summary = { graded: 0, skipped: 0, errors: [] };

  try {
    let query = supabase.from('agent_bets').select('*').is('result', null);
    if (forceDate) query = query.eq('game_date', forceDate);
    const { data: openBets, error: fetchErr } = await query;
    if (fetchErr) return res.status(500).json({ error: fetchErr.message });

    // Filter bets that are ready to grade
    const betsToGrade = [];
    for (const bet of (openBets || [])) {
      if (!forceDate) {
        if (!bet.game_time || (now - new Date(bet.game_time).getTime()) < GRADE_AFTER_MS) {
          summary.skipped++;
          continue;
        }
      }
      const gameDate = lookupDate || (bet.game_time ? getEtDate(bet.game_time) : null) || forceDate || bet.game_date;
      if (!gameDate) { summary.skipped++; continue; }
      bet._gameDate = gameDate;
      betsToGrade.push(bet);
    }

    // Check which players are missing the game date in Supabase, only BDL-fetch those
    const uniquePlayers = [...new Set(betsToGrade.map(b => b.player_name))];
    const gameDateForLookup = betsToGrade[0]?._gameDate;
    const missingPlayers = [];
    if (gameDateForLookup) {
      for (const name of uniquePlayers) {
        const record = await sbGetGameLogRecord(name);
        if (!record?.game_log?.find(g => g.date === gameDateForLookup)) {
          missingPlayers.push(name);
        }
      }
    }
    // Only pre-fetch missing players from BDL (3 concurrent to stay under 60/min rate limit)
    if (missingPlayers.length) {
      const prefetchBatch = async (items, fn, concurrency) => {
        for (let i = 0; i < items.length; i += concurrency) {
          await Promise.all(items.slice(i, i + concurrency).map(fn));
        }
      };
      await prefetchBatch(missingPlayers, async (name) => {
        try {
          await getActualStat(name, gameDateForLookup, 'player_points');
        } catch (e) { /* ignore — individual bet grading will handle errors */ }
      }, 3);
    }
    summary.prefetched = missingPlayers.length;

    // Now grade all bets — stats are cached, so this is fast
    for (const bet of betsToGrade) {
      try {
        const { status, value } = await getActualStat(bet.player_name, bet._gameDate, bet.market);

        if (status === 'missing') { summary.skipped++; continue; }

        const outcome = status === 'dnp' ? 'void' : gradeSingleOutcome(value, +bet.line, bet.direction);
        let pnl = 0;
        if (outcome === 'won') {
          pnl = bet.odds > 0 ? +(bet.stake * bet.odds / 100).toFixed(2) : +(bet.stake * 100 / Math.abs(bet.odds)).toFixed(2);
        } else if (outcome === 'lost') {
          pnl = -bet.stake;
        }

        const { error: updateErr } = await supabase.from('agent_bets').update({
          result: outcome,
          actual_stat: status === 'dnp' ? null : value,
          graded_at: new Date().toISOString(),
        }).eq('id', bet.id);

        if (updateErr) summary.errors.push(`${bet.player_name}: ${updateErr.message}`);
        else summary.graded++;
      } catch (e) {
        summary.errors.push(`${bet.player_name}: ${e.message}`);
      }
    }

    const elapsed = ((Date.now() - now) / 1000).toFixed(1);
    res.json({ success: true, playersPreFetched: missingPlayers.length, elapsed: `${elapsed}s`, ...summary });
  } catch (err) {
    res.status(500).json({ error: err.message, ...summary });
  }
});

// ============================================================
// CRON: Pre-tipoff injury snapshot — runs every 30 min (3:30-7pm PST)
// Only processes bets whose game tips off within the next 45 min
// and that haven't already been snapshotted. Re-checks injury report
// and stores updated tipoff values for newsletter comparison.
// ============================================================
app.get('/api/cron/tipoff-snapshot', async (req, res) => {
  const secret = (req.headers.authorization || '').replace('Bearer ', '') || req.headers['x-cron-secret'] || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!supabase) return res.status(503).json({ error: 'Supabase not configured' });

  const startTime = Date.now();
  const now = new Date();
  const today = req.query.date || now.toISOString().slice(0, 10);
  const WINDOW_MS = 45 * 60 * 1000; // process bets tipping off within 45 min
  const summary = { updated: 0, unchanged: 0, skipped: 0, alreadySnapshotted: 0, notInWindow: 0, errors: [] };

  try {
    // 1. Fetch today's ungraded agent bets
    const { data: bets, error: fetchErr } = await supabase.from('agent_bets')
      .select('*').eq('game_date', today).is('result', null);
    if (fetchErr) return res.status(500).json({ error: fetchErr.message });
    if (!bets?.length) return res.json({ success: true, message: 'No ungraded bets for today', ...summary });

    // 2. Filter to bets tipping off within the window and not already snapshotted
    const eligible = bets.filter(b => {
      // Skip bets already snapshotted
      if (b.tipoff_injury_adj_pct !== null && b.tipoff_injury_adj_pct !== undefined) {
        summary.alreadySnapshotted++;
        return false;
      }
      // Skip bets with no game time (can't determine window)
      if (!b.game_time) return true; // no game time = process anyway
      const tipoff = new Date(b.game_time);
      const msUntilTip = tipoff.getTime() - now.getTime();
      // Process if game tips off within the next 45 min or has already started (up to 30 min ago)
      if (msUntilTip <= WINDOW_MS && msUntilTip >= -30 * 60 * 1000) return true;
      summary.notInWindow++;
      return false;
    });

    if (!eligible.length) {
      return res.json({ success: true, message: 'No bets in current tipoff window', totalBets: bets.length, ...summary });
    }

    // 3. Invalidate injury cache so we get the latest report
    _injuryCache = { data: null, fetchedAt: 0 };
    const injuries = await fetchInjuryReport();
    if (!injuries?.length) return res.json({ success: true, message: 'No injury data available', ...summary });

    // 4. Fetch game logs for eligible bet players + injured teammates
    const uniqueNames = [...new Set(eligible.map(b => b.player_name))];
    const gameLogMap = new Map();
    for (let i = 0; i < uniqueNames.length; i += 50) {
      const batch = uniqueNames.slice(i, i + 50);
      const { data } = await supabase.from('player_stats').select('player_name, game_log, season').eq('season', NBA_SEASON).in('player_name', batch);
      if (data) data.forEach(r => gameLogMap.set(r.player_name, r.game_log));
    }

    // Fetch injured teammates' game logs
    const injuredTeammateNames = [...new Set(injuries.filter(i => i.status === 'Out').map(i => i.player))];
    const resolvedInjuredNames = injuredTeammateNames.map(n => ({ orig: n, resolved: resolvePlayerName(n) }));
    const missingInjured = resolvedInjuredNames.filter(n => !gameLogMap.has(n.resolved) && !gameLogMap.has(n.orig));
    if (missingInjured.length && supabase) {
      const namesToFetch = [...new Set(missingInjured.flatMap(n => [n.resolved, n.orig]))];
      for (let i = 0; i < namesToFetch.length; i += 50) {
        const batch = namesToFetch.slice(i, i + 50);
        const { data } = await supabase.from('player_stats').select('player_name, game_log, season').eq('season', NBA_SEASON).in('player_name', batch);
        if (data) data.forEach(r => gameLogMap.set(r.player_name, r.game_log));
      }
    }
    // BDL fallback for still-missing injured teammates (3 concurrent)
    const stillMissingInj = missingInjured.filter(n => !gameLogMap.has(n.resolved) && !gameLogMap.has(n.orig));
    if (stillMissingInj.length) {
      for (let i = 0; i < stillMissingInj.length; i += 3) {
        await Promise.all(stillMissingInj.slice(i, i + 3).map(async ({ resolved }) => {
          try {
            const bdl = await getBDLPlayerId(resolved);
            if (!bdl?.id) return;
            const games = await fetchBDLGameLog(bdl.id);
            if (games?.length) {
              gameLogMap.set(resolved, games);
              await sbSetGameLog(resolved, bdl.id, games, bdl.position);
            }
          } catch (e) { console.warn(`[tipoff-snapshot] BDL fallback for ${resolved}: ${e.message}`); }
        }));
      }
    }

    // 5. Re-run injury impact for each eligible bet
    for (const bet of eligible) {
      try {
        const gameLog = gameLogMap.get(bet.player_name);
        if (!gameLog?.length) { summary.skipped++; continue; }

        const playerTeam = bet.team || guessTeam(bet.player_name);
        if (!playerTeam || playerTeam === '???') { summary.skipped++; continue; }

        // Compute base stats then apply injury impact with fresh injury report
        const rawStats = serverComputeStats(gameLog, bet.line, bet.market, bet.odds, bet.odds);
        if (!rawStats) { summary.skipped++; continue; }

        let stats = serverApplySmartMinutes(gameLog, bet.line, bet.market, bet.odds, bet.odds, rawStats);
        stats = serverApplyDNP(gameLog, bet.line, bet.market, bet.odds, bet.odds, stats);
        stats = serverApplyInjuryImpact(bet.player_name, gameLog, playerTeam, bet.market, bet.line, bet.odds, bet.odds, injuries, gameLogMap, stats);

        const newAdjPct = stats._injuryImpact ? +((stats._injuryRatio - 1) * 100).toFixed(1) : null;
        const newTeammates = stats._injuryTeammate || null;

        // Always write the snapshot (even if same as placement) so we know it was captured
        const { error: upErr } = await supabase.from('agent_bets').update({
          tipoff_injury_adj_pct: newAdjPct,
          tipoff_injury_teammates: newTeammates,
        }).eq('id', bet.id);

        if (upErr) {
          summary.errors.push(`${bet.player_name}: ${upErr.message}`);
        } else if (newAdjPct !== bet.injury_adj_pct || newTeammates !== bet.injury_teammates) {
          summary.updated++;
        } else {
          summary.unchanged++;
        }
      } catch (e) {
        summary.errors.push(`${bet.player_name}: ${e.message}`);
      }
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[tipoff-snapshot] ${eligible.length} eligible | ${summary.updated} changed, ${summary.unchanged} same, ${summary.skipped} skipped, ${summary.alreadySnapshotted} already done, ${summary.notInWindow} not in window | ${elapsed}s`);
    res.json({ success: true, date: today, elapsed: `${elapsed}s`, totalBets: bets.length, eligible: eligible.length, ...summary });
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
    const { data: rawBets } = await supabase.from('agent_bets').select('*').not('result', 'is', null).order('placed_at', { ascending: false });
    // Compute pnl from result/stake/to_win since table doesn't store it
    const allBets = (rawBets || []).map(b => ({...b, status: b.result, actual_result: b.actual_stat, pnl: b.result === 'won' ? (b.to_win || 0) : b.result === 'lost' ? -b.stake : 0}));
    if (!allBets?.length) return res.json({ message: 'No graded bets yet' });

    // Yesterday's bets (most recent placed_at with graded results)
    const latestDate = allBets[0].placed_at;
    const yesterdayBets = allBets.filter(b => b.placed_at === latestDate);
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
      const mk = (MKT_LABELS[b.market]||b.market) || 'PTS';
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

    // Margin analysis for email
    const emailSettled = allValue.filter(b => b.status === 'won' || b.status === 'lost');
    const emailWithMargin = emailSettled.filter(b => b.actual_result != null && b.line != null);
    const emailCalcMargin = (b) => {
      const actual = +b.actual_result, line = +b.line;
      return b.direction === 'over' ? actual - line : line - actual;
    };
    const emailWonBets = emailWithMargin.filter(b => b.status === 'won');
    const emailLostBets = emailWithMargin.filter(b => b.status === 'lost');
    const emailAvgWonMargin = emailWonBets.length ? (emailWonBets.reduce((s, b) => s + emailCalcMargin(b), 0) / emailWonBets.length).toFixed(1) : '0';
    const emailAvgLostMargin = emailLostBets.length ? (emailLostBets.reduce((s, b) => s + emailCalcMargin(b), 0) / emailLostBets.length).toFixed(1) : '0';
    const emailCover1 = emailWonBets.filter(b => emailCalcMargin(b) >= 1).length;
    const emailCover3 = emailWonBets.filter(b => emailCalcMargin(b) >= 3).length;
    const emailCover5 = emailWonBets.filter(b => emailCalcMargin(b) >= 5).length;

    // Best/worst bets of the day for email
    const ySettledMargin = yesterdayValue.filter(b => (b.status === 'won' || b.status === 'lost') && b.actual_result != null);
    const ySorted = [...ySettledMargin].sort((a, b) => emailCalcMargin(b) - emailCalcMargin(a));
    const emailBest3 = ySorted.slice(0, 3);
    const emailWorst3 = ySorted.slice(-3).reverse();

    // Edge accuracy for email
    const emailEdgeTiers = [
      { label: '10%+ Edge', min: 10, max: Infinity },
      { label: '5-10% Edge', min: 5, max: 10 },
      { label: '2-5% Edge', min: 2, max: 5 },
      { label: '<2% Edge', min: 0, max: 2 },
    ];
    const edgeRows = emailEdgeTiers.map(t => {
      const inTier = emailSettled.filter(b => { const e = Math.abs(b.edge || 0); return e >= t.min && e < t.max; });
      if (!inTier.length) return '';
      const w = inTier.filter(b => b.status === 'won').length;
      const tPnl = inTier.reduce((s, b) => s + (b.pnl || 0), 0);
      return `<tr><td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">${t.label}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${w}-${inTier.length - w}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${(w / inTier.length * 100).toFixed(0)}%</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:right;color:${tPnl >= 0 ? '#16a34a' : '#dc2626'}">${tPnl >= 0 ? '+' : ''}$${tPnl.toFixed(2)}</td></tr>`;
    }).filter(Boolean).join('');

    // Over vs Under for email
    const emailOver = emailSettled.filter(b => b.direction === 'over');
    const emailUnder = emailSettled.filter(b => b.direction === 'under');
    const dirRow = (label, bets) => {
      if (!bets.length) return '';
      const w = bets.filter(b => b.status === 'won').length;
      const dPnl = bets.reduce((s, b) => s + (b.pnl || 0), 0);
      const dStaked = bets.reduce((s, b) => s + b.stake, 0);
      const roi = dStaked ? (dPnl / dStaked * 100).toFixed(1) : '0.0';
      return `<tr><td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">${label}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${w}-${bets.length - w}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${(w / bets.length * 100).toFixed(0)}%</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${roi}%</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:right;color:${dPnl >= 0 ? '#16a34a' : '#dc2626'}">${dPnl >= 0 ? '+' : ''}$${dPnl.toFixed(2)}</td></tr>`;
    };

    // Injury impact analysis for email
    const emailInjAdj = emailSettled.filter(b => b.injury_adj_pct != null && b.injury_adj_pct !== 0);
    const emailInjNone = emailSettled.filter(b => b.injury_adj_pct == null || b.injury_adj_pct === 0);
    const emailInjSpec = emailInjAdj.filter(b => b.injury_speculative);
    const emailInjData = emailInjAdj.filter(b => !b.injury_speculative);
    const injRow = (label, bets) => {
      if (!bets.length) return '';
      const w = bets.filter(b => b.status === 'won').length;
      const iPnl = bets.reduce((s, b) => s + (b.pnl || 0), 0);
      const iStaked = bets.reduce((s, b) => s + b.stake, 0);
      const roi = iStaked ? (iPnl / iStaked * 100).toFixed(1) : '0.0';
      return `<tr><td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">${label}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${w}-${bets.length - w}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${(w / bets.length * 100).toFixed(0)}%</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${roi}%</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:right;color:${iPnl >= 0 ? '#16a34a' : '#dc2626'}">${iPnl >= 0 ? '+' : ''}$${iPnl.toFixed(2)}</td></tr>`;
    };
    // Directional accuracy for email
    const emailInjWithActual = emailInjAdj.filter(b => b.actual_result != null);
    let emailInjCorrectDir = 0;
    for (const b of emailInjWithActual) {
      const adjPct = b.injury_adj_pct / 100;
      const actualVsLine = +b.actual_result - b.line;
      if ((adjPct > 0 && actualVsLine > 0) || (adjPct < 0 && actualVsLine < 0)) emailInjCorrectDir++;
    }
    const emailInjDirAcc = emailInjWithActual.length >= 3 ? (emailInjCorrectDir / emailInjWithActual.length * 100).toFixed(0) : null;

    // Pre-tipoff injury shift analysis for email
    const emailTipoffBets = emailSettled.filter(b => b.tipoff_injury_adj_pct !== null && b.tipoff_injury_adj_pct !== undefined);
    const emailTipoffChanged = emailTipoffBets.filter(b => Math.abs((b.tipoff_injury_adj_pct || 0) - (b.injury_adj_pct || 0)) >= 1);
    const emailTipoffBigChange = emailTipoffBets.filter(b => Math.abs((b.tipoff_injury_adj_pct || 0) - (b.injury_adj_pct || 0)) >= 3);
    const emailTipoffNewInj = emailTipoffBets.filter(b => (!b.injury_adj_pct || b.injury_adj_pct === 0) && b.tipoff_injury_adj_pct && b.tipoff_injury_adj_pct !== 0);
    const emailTipoffUnchanged = emailTipoffBets.filter(b => Math.abs((b.tipoff_injury_adj_pct || 0) - (b.injury_adj_pct || 0)) < 1);
    const tipoffWrFmt = (bets) => {
      if (!bets.length) return { record: 'N/A', wr: '—', roi: '—', pnl: '—' };
      const w = bets.filter(b => b.result === 'won').length;
      const p = bets.reduce((s, b) => s + (b.pnl || 0), 0);
      const st = bets.reduce((s, b) => s + b.stake, 0);
      return { record: `${w}-${bets.length - w}`, wr: `${(w / bets.length * 100).toFixed(0)}%`, roi: `${st ? (p / st * 100).toFixed(1) : '0.0'}%`, pnl: `${p >= 0 ? '+' : ''}$${p.toFixed(2)}`, pnlColor: p >= 0 ? '#16a34a' : '#dc2626' };
    };
    const tipoffRow = (label, bets) => {
      const d = tipoffWrFmt(bets);
      if (d.record === 'N/A') return '';
      return `<tr><td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">${label}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${d.record}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${d.wr}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${d.roi}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:right;color:${d.pnlColor}">${d.pnl}</td></tr>`;
    };

    // Yesterday's pick-by-pick results
    const fmtOdds = o => o > 0 ? `+${o}` : `${o}`;
    const pickRows = yesterdayValue.filter(b => b.status !== 'open').map(b => {
      const sc = b.status === 'won' ? '#16a34a' : b.status === 'lost' ? '#dc2626' : '#94a3b8';
      const icon = b.status === 'won' ? '✅' : b.status === 'lost' ? '❌' : '—';
      const actual = b.actual_result != null ? b.actual_result : '—';
      return `<tr>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">${icon} ${b.player_name}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;">${b.direction === 'over' ? '▲' : '▼'} ${b.line} ${(MKT_LABELS[b.market]||b.market)}</td>
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

  <!-- MARGIN ANALYSIS -->
  <div style="padding:24px;background:#f8fafc;border-top:1px solid #e2e8f0;">
    <div style="font-size:16px;font-weight:700;color:#1e293b;margin-bottom:12px;">Margin Analysis</div>
    <table style="width:100%;border-collapse:collapse;font-size:13px;margin-bottom:16px;">
      <tr>
        <td style="padding:8px 0;color:#64748b;">Avg Win Margin</td>
        <td style="padding:8px 0;text-align:right;font-weight:700;color:#16a34a;">+${emailAvgWonMargin}</td>
      </tr>
      <tr>
        <td style="padding:8px 0;color:#64748b;">Avg Loss Margin</td>
        <td style="padding:8px 0;text-align:right;font-weight:700;color:#dc2626;">${emailAvgLostMargin}</td>
      </tr>
      <tr><td style="padding:8px 0;color:#64748b;">Won by 1+</td><td style="padding:8px 0;text-align:right;font-weight:700;">${emailCover1} (${(emailCover1/Math.max(emailWonBets.length,1)*100).toFixed(0)}%)</td></tr>
      <tr><td style="padding:8px 0;color:#64748b;">Won by 3+</td><td style="padding:8px 0;text-align:right;font-weight:700;">${emailCover3} (${(emailCover3/Math.max(emailWonBets.length,1)*100).toFixed(0)}%)</td></tr>
      <tr><td style="padding:8px 0;color:#64748b;">Won by 5+</td><td style="padding:8px 0;text-align:right;font-weight:700;">${emailCover5} (${(emailCover5/Math.max(emailWonBets.length,1)*100).toFixed(0)}%)</td></tr>
    </table>
    ${emailBest3.length ? `<div style="font-size:13px;font-weight:600;color:#1e293b;margin-bottom:6px;">Best Bets Today</div>
    <div style="font-size:12px;color:#475569;margin-bottom:12px;">${emailBest3.map(b => {
      const m = emailCalcMargin(b);
      return `🔥 ${b.player_name} ${b.direction === 'over' ? '▲' : '▼'} ${b.line} ${(MKT_LABELS[b.market]||b.market)} — Actual: ${b.actual_result} (${m >= 0 ? '+' : ''}${m.toFixed(1)})`;
    }).join('<br>')}</div>` : ''}
    ${emailWorst3.length ? `<div style="font-size:13px;font-weight:600;color:#1e293b;margin-bottom:6px;">Worst Bets Today</div>
    <div style="font-size:12px;color:#475569;">${emailWorst3.map(b => {
      const m = emailCalcMargin(b);
      return `💀 ${b.player_name} ${b.direction === 'over' ? '▲' : '▼'} ${b.line} ${(MKT_LABELS[b.market]||b.market)} — Actual: ${b.actual_result} (${m >= 0 ? '+' : ''}${m.toFixed(1)})`;
    }).join('<br>')}</div>` : ''}
  </div>

  <!-- EDGE ACCURACY -->
  ${edgeRows ? `<div style="padding:24px;border-top:1px solid #e2e8f0;">
    <div style="font-size:16px;font-weight:700;color:#1e293b;margin-bottom:4px;">Edge Accuracy</div>
    <div style="font-size:12px;color:#94a3b8;margin-bottom:12px;">Higher edge should correlate with higher win rate</div>
    <table style="width:100%;border-collapse:collapse;font-size:12px;">
      <thead><tr style="background:#f8fafc;">
        <th style="padding:8px 12px;text-align:left;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Edge Tier</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Record</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Win %</th>
        <th style="padding:8px 12px;text-align:right;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">P&L</th>
      </tr></thead>
      <tbody>${edgeRows}</tbody>
    </table>
  </div>` : ''}

  <!-- OVER VS UNDER -->
  <div style="padding:24px;background:#f8fafc;border-top:1px solid #e2e8f0;">
    <div style="font-size:16px;font-weight:700;color:#1e293b;margin-bottom:12px;">Over vs Under</div>
    <table style="width:100%;border-collapse:collapse;font-size:12px;">
      <thead><tr>
        <th style="padding:8px 12px;text-align:left;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Direction</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Record</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Win %</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">ROI</th>
        <th style="padding:8px 12px;text-align:right;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">P&L</th>
      </tr></thead>
      <tbody>${dirRow('OVER ▲', emailOver)}${dirRow('UNDER ▼', emailUnder)}</tbody>
    </table>
  </div>

  <!-- INJURY IMPACT PERFORMANCE -->
  ${emailInjAdj.length >= 3 ? `<div style="padding:24px;border-top:1px solid #e2e8f0;">
    <div style="font-size:16px;font-weight:700;color:#1e293b;margin-bottom:4px;">Injury Impact Performance</div>
    <div style="font-size:12px;color:#94a3b8;margin-bottom:12px;">Do teammate injury adjustments improve bet accuracy?</div>
    <table style="width:100%;border-collapse:collapse;font-size:12px;">
      <thead><tr style="background:#f8fafc;">
        <th style="padding:8px 12px;text-align:left;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Group</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Record</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Win %</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">ROI</th>
        <th style="padding:8px 12px;text-align:right;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">P&L</th>
      </tr></thead>
      <tbody>
        ${injRow('🏥 Injury-Adjusted', emailInjAdj)}
        ${injRow('✅ No Adjustment', emailInjNone)}
        ${emailInjData.length >= 2 ? injRow('📊 Data-Backed', emailInjData) : ''}
        ${emailInjSpec.length >= 2 ? injRow('🔮 Speculative', emailInjSpec) : ''}
      </tbody>
    </table>
    ${emailInjDirAcc ? `<div style="margin-top:12px;padding:12px;background:#f0fdf4;border-radius:8px;font-size:13px;">
      <span style="font-weight:600;">🎯 Directional Accuracy:</span> ${emailInjDirAcc}% of injury adjustments moved the right way (${emailInjCorrectDir}/${emailInjWithActual.length})
    </div>` : ''}
  </div>` : ''}

  <!-- PRE-TIPOFF INJURY SHIFT ANALYSIS -->
  ${emailTipoffBets.length >= 3 ? `<div style="padding:24px;border-top:1px solid #e2e8f0;">
    <div style="font-size:16px;font-weight:700;color:#1e293b;margin-bottom:4px;">Pre-Tipoff Injury Shift Analysis</div>
    <div style="font-size:12px;color:#94a3b8;margin-bottom:12px;">Did injury changes between bet placement and tip-off affect outcomes?</div>
    <table style="width:100%;border-collapse:collapse;font-size:12px;">
      <thead><tr style="background:#f8fafc;">
        <th style="padding:8px 12px;text-align:left;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Group</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Record</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Win %</th>
        <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">ROI</th>
        <th style="padding:8px 12px;text-align:right;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">P&L</th>
      </tr></thead>
      <tbody>
        ${tipoffRow('✅ Unchanged', emailTipoffUnchanged)}
        ${tipoffRow('🔄 Changed (1%+)', emailTipoffChanged)}
        ${tipoffRow('⚠️ Large Shift (3%+)', emailTipoffBigChange)}
        ${tipoffRow('🆕 New Injury Post-Placement', emailTipoffNewInj)}
      </tbody>
    </table>
    <div style="margin-top:8px;font-size:11px;color:#94a3b8;">Tracked: ${emailTipoffBets.length} bets with pre-tipoff snapshots</div>
  </div>` : ''}

  <!-- VS COMPONENT BREAKDOWN -->
  ${(() => {
    const vsA = emailSettled.filter(b => b.model_prob != null && b.confidence != null && b.dir_ev != null);
    if (vsA.length < 10) return '';
    const median = arr => { const s = [...arr].sort((a, b) => a - b); const m = Math.floor(s.length / 2); return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
    const wr = bets => bets.length ? (bets.filter(b => b.status === 'won').length / bets.length * 100) : 0;
    const comps = [
      { name: 'Model Prob', weight: 20, key: 'model_prob', scale: v => v * 100 },
      { name: 'Confidence', weight: 40, key: 'confidence', scale: v => v },
      { name: 'EV', weight: 40, key: 'dir_ev', scale: v => v },
    ];
    const results = comps.map(c => {
      const vals = vsA.map(b => c.scale(b[c.key]));
      const med = median(vals);
      const above = vsA.filter(b => c.scale(b[c.key]) >= med);
      const below = vsA.filter(b => c.scale(b[c.key]) < med);
      const aboveWr = wr(above);
      const belowWr = wr(below);
      const lift = aboveWr - belowWr;
      return { ...c, lift, aboveWr, belowWr, med, aboveN: above.length, belowN: below.length };
    });
    const sorted = [...results].sort((a, b) => b.lift - a.lift);
    const best = sorted[0];
    const worst = sorted[sorted.length - 1];
    const rec = best.lift - worst.lift > 5
      ? `<strong>${best.name}</strong> (lift +${best.lift.toFixed(1)}pp) is the most predictive — consider increasing its weight. <strong>${worst.name}</strong> (lift ${worst.lift > 0 ? '+' : ''}${worst.lift.toFixed(1)}pp) is least predictive — consider decreasing.`
      : 'Components are performing similarly — current weights look balanced.';
    const rows = results.map(r => {
      const liftClr = r.lift > 5 ? '#16a34a' : r.lift < -5 ? '#dc2626' : '#d97706';
      return `<tr>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;font-weight:600;">${r.name} <span style="color:#94a3b8;font-weight:400;">(${r.weight}%)</span></td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${r.aboveWr.toFixed(1)}% <span style="color:#94a3b8;">(${r.aboveN})</span></td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;">${r.belowWr.toFixed(1)}% <span style="color:#94a3b8;">(${r.belowN})</span></td>
        <td style="padding:6px 12px;border-bottom:1px solid #e2e8f0;text-align:center;font-weight:700;color:${liftClr}">${r.lift > 0 ? '+' : ''}${r.lift.toFixed(1)}pp</td>
      </tr>`;
    }).join('');
    return `<div style="padding:24px;border-top:1px solid #e2e8f0;">
      <div style="font-size:16px;font-weight:700;color:#1e293b;margin-bottom:4px;">⚖️ Value Score Component Breakdown</div>
      <div style="font-size:12px;color:#94a3b8;margin-bottom:12px;">Which VS components are most predictive? Higher lift = better at separating winners from losers.</div>
      <table style="width:100%;border-collapse:collapse;font-size:12px;">
        <thead><tr style="background:#f8fafc;">
          <th style="padding:8px 12px;text-align:left;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Component</th>
          <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Above Median WR</th>
          <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Below Median WR</th>
          <th style="padding:8px 12px;text-align:center;font-weight:600;color:#64748b;border-bottom:2px solid #e2e8f0;">Lift</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
      <div style="margin-top:12px;padding:12px;background:#eef2ff;border-radius:8px;font-size:12px;color:#4338ca;">
        💡 ${rec}
      </div>
      <div style="margin-top:8px;font-size:11px;color:#94a3b8;">Based on ${vsA.length} settled value bets · Current weights: MP 20%, Conf 40%, EV 40%</div>
    </div>`;
  })()}

  <!-- FOOTER -->
  <div style="padding:24px;background:#0f172a;text-align:center;">
    <div style="color:#64748b;font-size:11px;">Value Alert Agent · Automated Tracking</div>
    <div style="color:#475569;font-size:10px;margin-top:4px;">Flat $$5 units · All bets placed at 3:30 PM PST daily</div>
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

// ---- Resend Discord picks for a specific date ----
app.get('/api/discord/resend-picks', async (req, res) => {
  const secret = (req.headers.authorization || '').replace('Bearer ', '') || req.headers['x-cron-secret'] || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!supabase) return res.status(503).json({ error: 'Supabase not configured' });
  const date = req.query.date || new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
  const { data: bets, error } = await supabase.from('agent_bets').select('*').eq('game_date', date);
  if (error) return res.status(500).json({ error: error.message });
  if (!bets || !bets.length) return res.status(404).json({ error: `No bets found for ${date}` });
  await sendDiscordPicks(bets, date);
  res.json({ success: true, date, bets: bets.length });
});

// Debug: see ungraded bet dates
app.get('/api/debug/ungraded-bets', async (req, res) => {
  const secret = (req.headers.authorization || '').replace('Bearer ', '') || req.query.secret;
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  if (!supabase) return res.status(503).json({ error: 'Not configured' });
  const { data } = await supabase.from('agent_bets').select('id, player_name, game_date, game_time, market, result').is('result', null).limit(20);
  const { data: dates } = await supabase.from('agent_bets').select('game_date').is('result', null);
  const dateCounts = {};
  (dates || []).forEach(d => { dateCounts[d.game_date || 'null'] = (dateCounts[d.game_date || 'null'] || 0) + 1; });
  res.json({ total: (dates || []).length, byDate: dateCounts, sample: data });
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
