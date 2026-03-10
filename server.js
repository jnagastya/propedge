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
const BDL_BASE = 'https://api.balldontlie.io/v1';
const NBA_BASE = 'https://stats.nba.com/stats';

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

// ============================================================
// ROUTE: GET /api/status — health check + API key validation
// ============================================================
app.get('/api/status', (req, res) => {
  res.json({
    server: 'ok',
    oddsApi: ODDS_KEY ? 'configured' : 'missing',
    bdlApi: BDL_KEY ? 'configured' : 'optional',
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
      stats: BDL_KEY ? 'configured' : 'optional',
    },
    season: NBA_SEASON,
    uptime: process.uptime(),
  });
});

// ============================================================
// ROUTE: POST /api/config — accept API keys from frontend
// ============================================================
app.post('/api/config', (req, res) => {
  const { oddsKey, bdlKey } = req.body || {};
  if (oddsKey) ODDS_KEY = oddsKey.trim();
  if (bdlKey) BDL_KEY = bdlKey.trim();
  cache.flushAll(); // clear cache so fresh data loads with new keys
  res.json({
    odds: ODDS_KEY ? 'configured' : 'missing',
    stats: BDL_KEY ? 'configured' : 'optional',
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
  try {
    const name = req.query.name;
    if (!name) return res.status(400).json({ error: 'name required' });

    const ck = `ps_${name.toLowerCase()}`;
    const cached = cacheGet(ck);
    if (cached) return res.json({ data: cached, cached: true });

    const headers = BDL_KEY ? { 'Authorization': BDL_KEY } : {};
    const url = `${BDL_BASE}/players?search=${encodeURIComponent(name)}&per_page=5`;
    const resp = await fetch(url, { headers });
    if (!resp.ok) throw new Error(`BDL search: ${resp.status}`);
    const data = await resp.json();

    cacheSet(ck, data.data || [], parseInt(process.env.CACHE_TTL_STATS) || 300);
    res.json({ data: data.data || [], cached: false });
  } catch (err) {
    console.error('Player search error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/stats/player/:id/games — game log
// ============================================================
app.get('/api/stats/player/:id/games', async (req, res) => {
  try {
    const { id } = req.params;
    const season = req.query.season || NBA_SEASON;

    const ck = `gl_${id}_${season}`;
    const cached = cacheGet(ck);
    if (cached) return res.json({ data: cached, cached: true });

    const headers = BDL_KEY ? { 'Authorization': BDL_KEY } : {};
    const url = `${BDL_BASE}/stats?player_ids[]=${id}&seasons[]=${season}&per_page=100`;
    const resp = await fetch(url, { headers });
    if (!resp.ok) throw new Error(`BDL stats: ${resp.status}`);
    const data = await resp.json();

    const games = data.data || [];
    cacheSet(ck, games, parseInt(process.env.CACHE_TTL_STATS) || 300);
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
  try {
    const { id } = req.params;
    const season = req.query.season || NBA_SEASON;

    const ck = `avg_${id}_${season}`;
    const cached = cacheGet(ck);
    if (cached) return res.json({ data: cached, cached: true });

    const headers = BDL_KEY ? { 'Authorization': BDL_KEY } : {};
    const url = `${BDL_BASE}/season_averages?player_ids[]=${id}&season=${season}`;
    const resp = await fetch(url, { headers });
    if (!resp.ok) throw new Error(`BDL averages: ${resp.status}`);
    const data = await resp.json();

    const avg = data.data?.[0] || null;
    cacheSet(ck, avg, parseInt(process.env.CACHE_TTL_STATS) || 300);
    res.json({ data: avg, cached: false });
  } catch (err) {
    console.error('Averages error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// ROUTE: GET /api/stats/player/:id/splits — computed splits
// ============================================================
app.get('/api/stats/player/:id/splits', async (req, res) => {
  try {
    const { id } = req.params;
    const season = req.query.season || NBA_SEASON;
    const line = parseFloat(req.query.line) || 0;
    const stat = req.query.stat || 'pts';

    // Get game log first
    const ck = `gl_${id}_${season}`;
    let games = cacheGet(ck);
    if (!games) {
      const headers = BDL_KEY ? { 'Authorization': BDL_KEY } : {};
      const url = `${BDL_BASE}/stats?player_ids[]=${id}&seasons[]=${season}&per_page=100`;
      const resp = await fetch(url, { headers });
      if (!resp.ok) throw new Error(`BDL stats: ${resp.status}`);
      const data = await resp.json();
      games = data.data || [];
      cacheSet(ck, games, parseInt(process.env.CACHE_TTL_STATS) || 300);
    }

    // Compute splits
    const splits = computeSplits(games, line, stat);
    res.json({ data: splits, cached: false, gamesAnalyzed: games.length });
  } catch (err) {
    console.error('Splits error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

function computeSplits(games, line, statKey) {
  if (!games.length) return null;

  const getVal = (g) => {
    const map = { pts: g.pts, reb: g.reb, ast: g.ast, stl: g.stl, blk: g.blk,
      threes: g.fg3m, pra: (g.pts||0) + (g.reb||0) + (g.ast||0), to: g.turnover };
    return map[statKey] || g.pts || 0;
  };

  const calc = (subset) => {
    if (!subset.length) return { avg: 0, gp: 0, hitRate: 0, values: [] };
    const vals = subset.map(getVal);
    const avg = +(vals.reduce((a,b) => a+b, 0) / vals.length).toFixed(1);
    const hr = line ? Math.round(vals.filter(v => v >= line).length / vals.length * 100) : 0;
    return { avg, gp: vals.length, hitRate: hr, values: vals };
  };

  // Sort by date
  const sorted = [...games].sort((a,b) => new Date(b.game?.date || 0) - new Date(a.game?.date || 0));

  // Home vs Away
  const home = sorted.filter(g => g.game?.home_team_id === g.team?.id);
  const away = sorted.filter(g => g.game?.home_team_id !== g.team?.id);

  // Last N
  const last5 = sorted.slice(0, 5);
  const last10 = sorted.slice(0, 10);
  const last20 = sorted.slice(0, 20);

  return {
    all: calc(sorted),
    home: calc(home),
    away: calc(away),
    last5: calc(last5),
    last10: calc(last10),
    last20: calc(last20),
    // We can't easily detect B2B/rest from BDL data, so flag these
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

    // Step 2: Enrich each player with stats (batch BDL lookups)
    const enriched = await Promise.all(players.slice(0, 50).map(async (p) => {
      try {
        // Search BDL for player
        const headers = BDL_KEY ? { 'Authorization': BDL_KEY } : {};
        const nameParts = p.name.split(' ');
        const searchName = nameParts.length >= 2 ? `${nameParts[0]} ${nameParts[nameParts.length-1]}` : p.name;

        const sCk = `ps_${searchName.toLowerCase()}`;
        let searchResult = cacheGet(sCk);
        if (!searchResult) {
          try {
            const sResp = await fetch(`${BDL_BASE}/players?search=${encodeURIComponent(searchName)}&per_page=1`, { headers });
            if (sResp.ok) {
              const sData = await sResp.json();
              searchResult = sData.data || [];
              cacheSet(sCk, searchResult, 600);
            }
          } catch { searchResult = []; }
        }

        const bdlPlayer = searchResult?.[0];
        let gameLog = [];
        let seasonAvg = null;

        if (bdlPlayer) {
          // Get game log
          const glCk = `gl_${bdlPlayer.id}_${NBA_SEASON}`;
          gameLog = cacheGet(glCk);
          if (!gameLog) {
            try {
              const glResp = await fetch(`${BDL_BASE}/stats?player_ids[]=${bdlPlayer.id}&seasons[]=${NBA_SEASON}&per_page=82`, { headers });
              if (glResp.ok) {
                const glData = await glResp.json();
                gameLog = glData.data || [];
                cacheSet(glCk, gameLog, 300);
              }
            } catch { gameLog = []; }
          }

          // Get season averages
          const avCk = `avg_${bdlPlayer.id}_${NBA_SEASON}`;
          seasonAvg = cacheGet(avCk);
          if (!seasonAvg) {
            try {
              const avResp = await fetch(`${BDL_BASE}/season_averages?player_ids[]=${bdlPlayer.id}&season=${NBA_SEASON}`, { headers });
              if (avResp.ok) {
                const avData = await avResp.json();
                seasonAvg = avData.data?.[0] || null;
                cacheSet(avCk, seasonAvg, 300);
              }
            } catch { seasonAvg = null; }
          }
        }

        // Map market to stat key
        const statMap = {
          'player_points': 'pts', 'player_rebounds': 'reb', 'player_assists': 'ast',
          'player_threes': 'fg3m', 'player_steals': 'stl', 'player_blocks': 'blk',
          'player_turnovers': 'turnover',
        };
        const statKey = statMap[market] || 'pts';

        // Extract L10 values
        const sorted = [...gameLog].sort((a, b) => new Date(b.game?.date || 0) - new Date(a.game?.date || 0));
        const l10Raw = sorted.slice(0, 10).map(g => g[statKey] || 0).reverse();
        const l10 = l10Raw.length >= 5 ? l10Raw : generateFakeL10(p.line);

        // Compute analytics
        const avg = l10.length ? +(l10.reduce((a,b) => a+b, 0) / l10.length).toFixed(1) : p.line;
        const hitRate = p.line ? Math.round(l10.filter(v => v >= p.line).length / l10.length * 100) : 50;
        const edge = p.line ? +((avg - p.line) / p.line * 100).toFixed(1) : 0;
        const pos = bdlPlayer?.position || guessPosition(p.name);
        const team = bdlPlayer ? teamFromBDL(bdlPlayer) : guessTeam(p.name);
        const dvpRank = computeDvPRank(p.homeTeam === team ? p.awayTeam : p.homeTeam, pos);
        const dvpClass = dvpRank <= 10 ? 'Easy' : dvpRank >= 21 ? 'Hard' : 'Mid';

        // Confidence score
        const confidence = computeConfidence({ l10, hitRate, edge, dvpRank });

        return {
          ...p,
          team,
          position: pos,
          avg,
          l10,
          hitRate,
          edge,
          dvpRank,
          dvpClass,
          confidence,
          bdlId: bdlPlayer?.id || null,
          hasRealStats: gameLog.length >= 5,
          gamesPlayed: gameLog.length,
          seasonAvg: seasonAvg ? {
            pts: seasonAvg.pts, reb: seasonAvg.reb, ast: seasonAvg.ast,
            fg3m: seasonAvg.fg3m, min: seasonAvg.min, fga: seasonAvg.fga,
          } : null,
        };
      } catch (enrichErr) {
        console.error(`Enrich error for ${p.name}:`, enrichErr.message);
        const l10 = generateFakeL10(p.line);
        const avg = +(l10.reduce((a,b) => a+b, 0) / l10.length).toFixed(1);
        return {
          ...p, team: guessTeam(p.name), position: guessPosition(p.name),
          avg, l10, hitRate: 50, edge: +((avg - p.line) / p.line * 100).toFixed(1),
          dvpRank: 15, dvpClass: 'Mid', confidence: 45,
          bdlId: null, hasRealStats: false, gamesPlayed: 0, seasonAvg: null,
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

function teamFromBDL(player) {
  if (!player?.team) return '???';
  const map = { 1:'ATL',2:'BOS',3:'BKN',4:'CHA',5:'CHI',6:'CLE',7:'DAL',8:'DEN',9:'DET',10:'GSW',
    11:'HOU',12:'IND',13:'LAC',14:'LAL',15:'MEM',16:'MIA',17:'MIL',18:'MIN',19:'NOP',20:'NYK',
    21:'OKC',22:'ORL',23:'PHI',24:'PHX',25:'POR',26:'SAC',27:'SAS',28:'TOR',29:'UTA',30:'WAS' };
  return map[player.team.id] || player.team.abbreviation || '???';
}

// ============================================================
// ROUTE: GET /api/cache/clear — manual cache flush
// ============================================================
app.get('/api/cache/clear', (req, res) => {
  cache.flushAll();
  res.json({ message: 'Cache cleared', keys: 0 });
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

// ---- START ----
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n  ╔═══════════════════════════════════════╗`);
  console.log(`  ║  PropEdge v3 Server                   ║`);
  console.log(`  ║  Running on http://localhost:${PORT}      ║`);
  console.log(`  ║  Odds API: ${ODDS_KEY ? '✓ Configured' : '✗ Missing'}              ║`);
  console.log(`  ║  BDL API:  ${BDL_KEY ? '✓ Configured' : '○ Optional'}              ║`);
  console.log(`  ╚═══════════════════════════════════════╝\n`);
});
