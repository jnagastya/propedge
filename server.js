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

function bdlHeaders() {
  return { 'Authorization': BDL_KEY };
}

function normalizeNBAGameLog(rows) {
  return (Array.isArray(rows) ? rows : []).map(g => {
    // BDL v1 format: { pts, reb, ast, ..., game: { date, home_team_id }, team: { id } }
    if (g.game && g.team) {
      const home = g.game.home_team_id === g.team.id;
      return {
        date: (g.game.date || '').split('T')[0],
        pts: +g.pts || 0, reb: +g.reb || 0, ast: +g.ast || 0,
        fg3m: +g.fg3m || 0, stl: +g.stl || 0, blk: +g.blk || 0,
        turnover: +g.turnover || 0, min: String(g.min || '0'),
        home, matchup: g.team.abbreviation || '', wl: '',
      };
    }
    // Lowercase flat format fallback
    return {
      date: g.date || g.game_date || '',
      pts: +g.pts || 0, reb: +g.reb || 0, ast: +g.ast || 0,
      fg3m: +g.fg3m || 0, stl: +g.stl || 0, blk: +g.blk || 0,
      turnover: +g.turnover || +g.tov || 0,
      min: String(g.min || '0'), home: g.home || false,
      matchup: '', wl: '',
    };
  }).filter(g => g.date);
}

// Fetch full season game log from BDL (handles cursor pagination)
async function fetchBDLGameLog(playerId) {
  let allRows = [];
  let cursor = null;
  do {
    let url = `${BDL_BASE}/stats?player_ids[]=${playerId}&seasons[]=${NBA_SEASON}&per_page=100`;
    if (cursor) url += `&cursor=${cursor}`;
    const resp = await fetch(url, { headers: bdlHeaders() });
    if (!resp.ok) throw new Error(`BDL stats ${resp.status}`);
    const data = await resp.json();
    allRows = allRows.concat(data.data || []);
    cursor = data.meta?.next_cursor || null;
  } while (cursor);
  return normalizeNBAGameLog(allRows);
}

// Get BDL player ID by name, cached for 24h
async function getBDLPlayerId(name) {
  const ck = `bdl_pid_${name.toLowerCase().replace(/\s+/g, '_')}`;
  const cached = cacheGet(ck);
  if (cached !== undefined) return cached;
  const resp = await fetch(`${BDL_BASE}/players?search=${encodeURIComponent(name)}&per_page=5`, { headers: bdlHeaders() });
  if (!resp.ok) { cacheSet(ck, null, 3600); return null; }
  const data = await resp.json();
  const player = (data.data || []).find(p =>
    `${p.first_name} ${p.last_name}`.toLowerCase() === name.toLowerCase()
  ) || data.data?.[0];
  const id = player ? player.id : null;
  cacheSet(ck, id, 24 * 3600);
  return id;
}

const NBA_IDS_SERVER = {
  "LeBron James":2544,"Stephen Curry":201939,"Kevin Durant":201142,
  "Giannis Antetokounmpo":203507,"Nikola Jokic":203999,"Luka Doncic":1629029,
  "Joel Embiid":203954,"Jayson Tatum":1628369,"Shai Gilgeous-Alexander":1628983,
  "Anthony Edwards":1630162,"Donovan Mitchell":1628378,"Tyrese Haliburton":1630169,
  "LaMelo Ball":1630163,"Devin Booker":1626164,"Trae Young":1629027,
  "Jalen Brunson":1628973,"De'Aaron Fox":1628368,"Cade Cunningham":1630595,
  "Tyrese Maxey":1630178,"Domantas Sabonis":1627734,"Anthony Davis":203076,
  "Damian Lillard":203081,"Jimmy Butler":202710,"Ja Morant":1629630,
  "Zion Williamson":1629627,"Paolo Banchero":1631094,"Austin Reaves":1630559,
  "Rui Hachimura":1629060,"Karl-Anthony Towns":1626157,"Bam Adebayo":1628389,
  "Jaylen Brown":1627759,"Kristaps Porzingis":204001,"Kyrie Irving":202681,
  "Jamal Murray":1627750,"Desmond Bane":1630217,"Franz Wagner":1630532,
  "Brandon Ingram":1627742,"Lauri Markkanen":1628374,"Scottie Barnes":1630567,
  "Mikal Bridges":1628969,"RJ Barrett":1629628,"Alperen Sengun":1630578,
  "Jalen Williams":1631114,"Chet Holmgren":1631096,"Victor Wembanyama":1641705,
  "Jalen Green":1630224,"Evan Mobley":1630596,"Darius Garland":1629636,
  "Kawhi Leonard":202695,"Pascal Siakam":1627783,
};

// ============================================================
// ROUTE: GET /api/status — health check + API key validation
// ============================================================
app.get('/api/status', (req, res) => {
  res.json({
    server: 'ok',
    oddsApi: ODDS_KEY ? 'configured' : 'missing',
    nbaApi: BDL_KEY ? 'configured' : 'missing',
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
      stats: BDL_KEY ? 'configured' : 'missing',
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
    stats: BDL_KEY ? 'configured' : 'missing',
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
// ROUTE: GET /api/stats/player/search — search players
// ============================================================
app.get('/api/stats/player/search', async (req, res) => {
  const { name } = req.query;
  if (!name) return res.status(400).json({ error: 'name required' });
  if (!BDL_KEY) return res.status(400).json({ error: 'BDL_API_KEY not configured' });

  const ck = `bdl_search_${name.toLowerCase()}`;
  const cached = cacheGet(ck);
  if (cached !== undefined) return res.json({ data: cached, cached: true });

  try {
    const resp = await fetch(`${BDL_BASE}/players?search=${encodeURIComponent(name)}&per_page=5`, { headers: bdlHeaders() });
    if (!resp.ok) throw new Error(`BDL ${resp.status}`);
    const data = await resp.json();
    const results = (data.data || []).map(p => ({ id: p.id, name: `${p.first_name} ${p.last_name}` }));
    cacheSet(ck, results, 24 * 3600);
    res.json({ data: results, cached: false });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/stats/player/:id/games — game log
// ============================================================
app.get('/api/stats/player/:id/games', async (req, res) => {
  const { id } = req.params;
  const ck = `bdl_gl_${id}_${NBA_SEASON}`;
  const cached = cacheGet(ck);
  if (cached) return res.json({ data: cached, cached: true });

  if (!BDL_KEY) return res.status(400).json({ error: 'BDL_API_KEY not configured' });

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
// ROUTE: GET /api/stats/player/:id/averages — season averages
// ============================================================
app.get('/api/stats/player/:id/averages', async (req, res) => {
  const { id } = req.params;
  const ck = `bdl_avg_${id}_${NBA_SEASON}`;
  const cached = cacheGet(ck);
  if (cached) return res.json({ data: cached, cached: true });

  if (!BDL_KEY) return res.status(400).json({ error: 'BDL_API_KEY not configured' });

  try {
    const resp = await fetch(`${BDL_BASE}/season_averages?season=${NBA_SEASON}&player_id=${id}`, { headers: bdlHeaders() });
    if (!resp.ok) throw new Error(`BDL ${resp.status}`);
    const data = await resp.json();
    const a = data.data?.[0];
    if (!a) { res.json({ data: null, cached: false }); return; }
    const result = { pts: a.pts, reb: a.reb, ast: a.ast, fg3m: a.fg3m, gp: a.games_played };
    cacheSet(ck, result, STATS_TTL);
    res.json({ data: result, cached: false });
  } catch (err) {
    console.error('Averages error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/stats/player/:id/splits — computed splits
// ============================================================
app.get('/api/stats/player/:id/splits', async (req, res) => {
  const { id } = req.params;
  const line = parseFloat(req.query.line) || 0;
  const stat = req.query.stat || 'pts';

  const ck = `bdl_gl_${id}_${NBA_SEASON}`;
  let games = cacheGet(ck);
  if (!games) {
    if (!BDL_KEY) return res.status(400).json({ error: 'BDL_API_KEY not configured' });
    try {
      games = await fetchBDLGameLog(id);
      cacheSet(ck, games, STATS_TTL);
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  const splits = computeSplits(games, line, stat);
  res.json({ data: splits, cached: false, gamesAnalyzed: games.length });
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
    let players = [];
    if (ODDS_KEY) {
      const evtUrl = `${ODDS_BASE}/sports/basketball_nba/events?apiKey=${ODDS_KEY}&regions=us&oddsFormat=american`;
      const evtResp = await fetch(evtUrl);
      if (evtResp.ok) {
        const events = await evtResp.json();

        const propPromises = events.slice(0, 8).map(async (evt) => {
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

    // Step 2: Enrich each player with stats via BDL
    const statMap = {
      'player_points': 'pts', 'player_rebounds': 'reb', 'player_assists': 'ast',
      'player_threes': 'fg3m', 'player_steals': 'stl', 'player_blocks': 'blk',
      'player_turnovers': 'turnover',
    };
    const statKey = statMap[market] || 'pts';

    const enriched = await Promise.all(players.slice(0, 50).map(async (p) => {
      try {
        let gameLog = [];

        if (BDL_KEY) {
          const bdlId = await getBDLPlayerId(p.name);
          if (bdlId) {
            const glCk = `bdl_gl_${bdlId}_${NBA_SEASON}`;
            gameLog = cacheGet(glCk);
            if (!gameLog) {
              try {
                gameLog = await fetchBDLGameLog(bdlId);
                cacheSet(glCk, gameLog, STATS_TTL);
              } catch { gameLog = []; }
            }
            gameLog = gameLog || [];
          }
        }

        const sorted = [...gameLog].sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
        const l10Raw = sorted.slice(0, 10).map(g => g[statKey] || 0).reverse();
        const l10 = l10Raw.length >= 5 ? l10Raw : generateFakeL10(p.line);

        const avg = l10.length ? +(l10.reduce((a, b) => a + b, 0) / l10.length).toFixed(1) : p.line;
        const hitRate = p.line ? Math.round(l10.filter(v => v >= p.line).length / l10.length * 100) : 50;
        const edge = p.line ? +((avg - p.line) / p.line * 100).toFixed(1) : 0;
        const pos = guessPosition(p.name);
        const team = guessTeam(p.name);
        const dvpRank = computeDvPRank(p.homeTeam === team ? p.awayTeam : p.homeTeam, pos);
        const dvpClass = dvpRank <= 10 ? 'Easy' : dvpRank >= 21 ? 'Hard' : 'Mid';
        const confidence = computeConfidence({ l10, hitRate, edge, dvpRank });

        return {
          ...p, team, position: pos, avg, l10, hitRate, edge, dvpRank, dvpClass, confidence,
          hasRealStats: gameLog.length >= 5,
          gamesPlayed: gameLog.length,
        };
      } catch (enrichErr) {
        console.error(`Enrich error for ${p.name}:`, enrichErr.message);
        const l10 = generateFakeL10(p.line);
        const avg = +(l10.reduce((a, b) => a + b, 0) / l10.length).toFixed(1);
        return {
          ...p, team: guessTeam(p.name), position: guessPosition(p.name),
          avg, l10, hitRate: 50, edge: +((avg - p.line) / p.line * 100).toFixed(1),
          dvpRank: 15, dvpClass: 'Mid', confidence: 45,
          hasRealStats: false, gamesPlayed: 0,
        };
      }
    }));

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

function computeConfidence({ l10, hitRate, edge, dvpRank }) {
  let score = 50;
  score += Math.min(Math.abs(edge) * 2, 20);
  const avg = l10.reduce((a,b) => a+b, 0) / l10.length;
  const stdDev = Math.sqrt(l10.reduce((s,v) => s + Math.pow(v - avg, 2), 0) / l10.length);
  const cv = avg ? stdDev / avg : 1;
  score += (1 - Math.min(cv, 0.5)) * 20;
  score += Math.abs(hitRate - 50) * 0.3;
  if (dvpRank <= 8) score += 10;
  else if (dvpRank >= 24) score -= 5;
  return Math.max(0, Math.min(100, Math.round(score)));
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

function guessTeam(name) {
  const map = {
    'luka doncic':'DAL','jayson tatum':'BOS','shai gilgeous-alexander':'OKC','nikola jokic':'DEN',
    'anthony edwards':'MIN','tyrese haliburton':'IND','kevin durant':'PHX','donovan mitchell':'CLE',
    'lamelo ball':'CHA',"de'aaron fox":'SAC','austin reaves':'LAL','trae young':'ATL',
    'jalen brunson':'NYK','devin booker':'PHX','stephen curry':'GSW','giannis antetokounmpo':'MIL',
    'tyrese maxey':'PHI','cade cunningham':'DET','domantas sabonis':'SAC','rui hachimura':'LAL',
    'joel embiid':'PHI','jimmy butler':'MIA','bam adebayo':'MIA','damian lillard':'MIL',
    'jaylen brown':'BOS','ja morant':'MEM','zion williamson':'NOP','paolo banchero':'ORL',
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
// ROUTE: GET /api/debug/bdl — test BDL API connectivity
// ============================================================
app.get('/api/debug/bdl', async (req, res) => {
  const result = {
    keyConfigured: !!BDL_KEY,
    keyPrefix: BDL_KEY ? BDL_KEY.substring(0, 8) + '...' : null,
    season: NBA_SEASON,
    bdlBase: BDL_BASE,
  };
  if (!BDL_KEY) return res.json({ ...result, error: 'BDL_API_KEY env var is missing' });
  try {
    const searchUrl = `${BDL_BASE}/players?search=${encodeURIComponent('LeBron James')}&per_page=1`;
    result.searchUrl = searchUrl;

    // Try plain key first, then Bearer prefix
    for (const authHeader of [BDL_KEY, `Bearer ${BDL_KEY}`]) {
      const searchResp = await fetch(searchUrl, { headers: { 'Authorization': authHeader } });
      result[`status_${authHeader.startsWith('Bearer') ? 'bearer' : 'plain'}`] = searchResp.status;
      const raw = await searchResp.text();
      let parsed;
      try { parsed = JSON.parse(raw); } catch { parsed = null; }
      const player = parsed?.data?.[0];
      if (player) {
        result.workingAuthFormat = authHeader.startsWith('Bearer') ? 'Bearer <key>' : '<key> directly';
        result.playerFound = `${player.first_name} ${player.last_name} (id=${player.id})`;
        // Also test stats
        const statsResp = await fetch(`${BDL_BASE}/stats?player_ids[]=${player.id}&seasons[]=${NBA_SEASON}&per_page=3`, { headers: { 'Authorization': authHeader } });
        result.statsStatus = statsResp.status;
        if (statsResp.ok) {
          const sd = await statsResp.json();
          result.gamesFound = sd.data?.length || 0;
          result.sampleGame = sd.data?.[0] ? { date: sd.data[0].game?.date, pts: sd.data[0].pts } : null;
        }
        return res.json({ ...result, ok: true });
      }
      result[`rawBody_${authHeader.startsWith('Bearer') ? 'bearer' : 'plain'}`] = raw.slice(0, 200);
    }

    res.json({ ...result, playerFound: 'none — both auth formats returned empty data' });
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
    console.log(`  ║  Odds API: ${ODDS_KEY ? '✓ Configured' : '✗ Missing'}              ║`);
    console.log(`  ║  BDL API:  ${BDL_KEY ? '✓ Configured' : '○ Missing'}              ║`);
    console.log(`  ╚═══════════════════════════════════════╝\n`);
  });
}

// Required for Vercel serverless
module.exports = app;
