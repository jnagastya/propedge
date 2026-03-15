// ============================================================
// Standalone script: Fetch NBA.com team stats → write to Supabase
// Run locally: node scripts/refresh-team-stats.js
// NBA.com blocks cloud IPs (Vercel, GitHub Actions), so this
// must run from a residential IP (your machine).
//
// Blended windows: 60% season + 25% last 15 games + 15% last 5 games
// This captures roster/injury shifts without losing seasonal stability.
// ============================================================

// Load .env from project root if available
try { require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') }); } catch {}

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_KEY');
  process.exit(1);
}

const NBA_STATS_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Referer': 'https://www.nba.com/',
  'Accept': 'application/json, text/plain, */*',
  'Origin': 'https://www.nba.com',
  'x-nba-stats-origin': 'stats',
  'x-nba-stats-token': 'true',
};

const NBA_COM_TEAM_MAP = {
  1610612737:'ATL',1610612738:'BOS',1610612751:'BKN',1610612766:'CHA',1610612741:'CHI',
  1610612739:'CLE',1610612742:'DAL',1610612743:'DEN',1610612765:'DET',1610612744:'GSW',
  1610612745:'HOU',1610612754:'IND',1610612746:'LAC',1610612747:'LAL',1610612763:'MEM',
  1610612748:'MIA',1610612749:'MIL',1610612750:'MIN',1610612740:'NOP',1610612752:'NYK',
  1610612760:'OKC',1610612753:'ORL',1610612755:'PHI',1610612756:'PHX',1610612757:'POR',
  1610612758:'SAC',1610612759:'SAS',1610612761:'TOR',1610612762:'UTA',1610612764:'WAS',
};

function currentNBASeason() {
  const now = new Date();
  const month = now.getMonth() + 1;
  const year = now.getFullYear();
  return month >= 10 ? year : year - 1;
}

async function fetchNBAStats(endpoint, params = {}) {
  const qs = new URLSearchParams(params).toString();
  const url = `https://stats.nba.com/stats/${endpoint}?${qs}`;
  console.log(`  Fetching ${endpoint}${params.LastNGames && params.LastNGames !== '0' ? ` (L${params.LastNGames})` : ''}${params.Location ? ` [${params.Location}]` : ''}...`);
  const resp = await fetch(url, { headers: NBA_STATS_HEADERS });
  if (!resp.ok) throw new Error(`NBA Stats ${endpoint} returned ${resp.status}`);
  return resp.json();
}

const delay = (ms) => new Promise(r => setTimeout(r, ms));

// ---- Fetch functions with LastNGames support ----

async function fetchTeamAdvancedStats(season, lastNGames = 0, location = '') {
  const seasonStr = `${season}-${String(season + 1).slice(2)}`;
  const json = await fetchNBAStats('leaguedashteamstats', {
    MeasureType: 'Advanced', PerMode: 'PerGame', Season: seasonStr,
    SeasonType: 'Regular Season', LeagueID: '00', TeamID: '0',
    OpponentTeamID: '0', Conference: '', Division: '', GameScope: '',
    GameSegment: '', DateFrom: '', DateTo: '', LastNGames: String(lastNGames),
    Location: location, Month: '0', Outcome: '', PORound: '0',
    PaceAdjust: 'N', Period: '0', PlayerExperience: '', PlayerPosition: '',
    PlusMinus: 'N', Rank: 'N', SeasonSegment: '', ShotClockRange: '',
    StarterBench: '', TwoWay: '0', VsConference: '', VsDivision: '',
  });
  const headers = json.resultSets?.[0]?.headers || [];
  const rows = json.resultSets?.[0]?.rowSet || [];
  const idx = (name) => headers.indexOf(name);
  const teams = {};
  for (const r of rows) {
    const abbr = NBA_COM_TEAM_MAP[r[idx('TEAM_ID')]] || r[idx('TEAM_NAME')]?.substring(0, 3)?.toUpperCase();
    if (!abbr) continue;
    teams[abbr] = {
      name: r[idx('TEAM_NAME')], gp: r[idx('GP')], w: r[idx('W')], l: r[idx('L')],
      pace: r[idx('PACE')], offRtg: r[idx('OFF_RATING')], defRtg: r[idx('DEF_RATING')],
      netRtg: r[idx('NET_RATING')], ts: r[idx('TS_PCT')], efg: r[idx('EFG_PCT')],
      astPct: r[idx('AST_PCT')], tovPct: r[idx('TM_TOV_PCT')], rebPct: r[idx('REB_PCT')],
      pie: r[idx('PIE')],
    };
  }
  return teams;
}

async function fetchOpponentStats(season, lastNGames = 0) {
  const seasonStr = `${season}-${String(season + 1).slice(2)}`;
  const json = await fetchNBAStats('leaguedashteamstats', {
    MeasureType: 'Opponent', PerMode: 'PerGame', Season: seasonStr,
    SeasonType: 'Regular Season', LeagueID: '00', TeamID: '0',
    OpponentTeamID: '0', Conference: '', Division: '', GameScope: '',
    GameSegment: '', DateFrom: '', DateTo: '', LastNGames: String(lastNGames),
    Location: '', Month: '0', Outcome: '', PORound: '0',
    PaceAdjust: 'N', Period: '0', PlayerExperience: '', PlayerPosition: '',
    PlusMinus: 'N', Rank: 'N', SeasonSegment: '', ShotClockRange: '',
    StarterBench: '', TwoWay: '0', VsConference: '', VsDivision: '',
  });
  const headers = json.resultSets?.[0]?.headers || [];
  const rows = json.resultSets?.[0]?.rowSet || [];
  const idx = (name) => headers.indexOf(name);
  const teams = {};
  for (const r of rows) {
    const abbr = NBA_COM_TEAM_MAP[r[idx('TEAM_ID')]] || r[idx('TEAM_NAME')]?.substring(0, 3)?.toUpperCase();
    if (!abbr) continue;
    teams[abbr] = {
      pts: r[idx('OPP_PTS')], reb: r[idx('OPP_REB')], ast: r[idx('OPP_AST')],
      fg3m: r[idx('OPP_FG3M')], stl: r[idx('OPP_STL')], blk: r[idx('OPP_BLK')],
      tov: r[idx('OPP_TOV')], fgPct: r[idx('OPP_FG_PCT')], fg3Pct: r[idx('OPP_FG3_PCT')],
      oreb: r[idx('OPP_OREB')], dreb: r[idx('OPP_DREB')],
    };
  }
  return teams;
}

// Fetch full game log — blowout windows computed from one dataset
async function fetchGameLog(season) {
  const seasonStr = `${season}-${String(season + 1).slice(2)}`;
  const json = await fetchNBAStats('leaguegamelog', {
    PlayerOrTeam: 'T', Season: seasonStr, SeasonType: 'Regular Season',
    LeagueID: '00', Counter: '0', Sorter: 'DATE', Direction: 'DESC',
    DateFrom: '', DateTo: '',
  });
  const headers = json.resultSets?.[0]?.headers || [];
  const rows = json.resultSets?.[0]?.rowSet || [];
  const idx = (name) => headers.indexOf(name);

  // Group games by team (already sorted DESC by date)
  const teamGames = {};
  for (const r of rows) {
    const abbr = r[idx('TEAM_ABBREVIATION')];
    if (!abbr) continue;
    if (!teamGames[abbr]) teamGames[abbr] = [];
    teamGames[abbr].push({ margin: r[idx('PLUS_MINUS')] || 0, wl: r[idx('WL')], date: r[idx('GAME_DATE')] });
  }
  return teamGames;
}

function computeBlowouts(games) {
  const BLOWOUT_MARGIN = 15;
  let blowoutWins = 0, blowoutLosses = 0, totalMargin = 0;
  for (const g of games) {
    totalMargin += g.margin;
    if (g.margin >= BLOWOUT_MARGIN && g.wl === 'W') blowoutWins++;
    else if (g.margin <= -BLOWOUT_MARGIN && g.wl === 'L') blowoutLosses++;
  }
  const gp = games.length;
  const total = blowoutWins + blowoutLosses;
  return {
    gp, blowoutWins, blowoutLosses,
    blowoutPct: gp ? +(total / gp * 100).toFixed(1) : 0,
    blowoutWinPct: gp ? +(blowoutWins / gp * 100).toFixed(1) : 0,
    blowoutLossPct: gp ? +(blowoutLosses / gp * 100).toFixed(1) : 0,
    avgMargin: gp ? +(totalMargin / gp).toFixed(1) : 0,
  };
}

// ---- Blending utility ----
// Blend numeric fields from 3 windows: 60% season + 25% L15 + 15% L5
function blendObj(season, l15, l5, keys) {
  const result = {};
  for (const k of keys) {
    const sv = season?.[k], m = l15?.[k], s = l5?.[k];
    if (sv == null) { result[k] = sv; continue; }
    // If L15/L5 data missing (team played fewer games), fall back to season
    const v15 = m != null ? m : sv;
    const v5 = s != null ? s : v15;
    result[k] = +(sv * 0.60 + v15 * 0.25 + v5 * 0.15).toFixed(2);
  }
  return result;
}

// ---- Player on/off impact ----
// Fetches team-level on/off summary for one team, returns top 8 players by minutes
const ONOFF_DAMPEN = 0.6; // on/off overstates individual impact (lineup correlation)
const MAX_IMPACT_PLAYERS = 8;

async function fetchTeamOnOff(season, teamId, teamAbbr) {
  const seasonStr = `${season}-${String(season + 1).slice(2)}`;
  const json = await fetchNBAStats('teamplayeronoffsummary', {
    TeamID: String(teamId), Season: seasonStr, SeasonType: 'Regular Season',
    MeasureType: 'Base', PerMode: 'PerGame', LastNGames: '0', Month: '0',
    OpponentTeamID: '0', PaceAdjust: 'N', Period: '0', PlusMinus: 'N',
    Rank: 'N', DateFrom: '', DateTo: '', GameSegment: '', LeagueID: '00',
    Location: '', Outcome: '', SeasonSegment: '', VsConference: '', VsDivision: '',
  });

  // resultSets: [0] = overall, [1] = on court, [2] = off court
  const onSet = json.resultSets?.[1], offSet = json.resultSets?.[2];
  if (!onSet?.headers || !offSet?.headers) return [];

  const onH = onSet.headers, offH = offSet.headers;
  const onIdx = (n) => onH.indexOf(n), offIdx = (n) => offH.indexOf(n);

  // Build map of off-court net ratings by player ID
  const offMap = {};
  for (const r of offSet.rowSet || []) {
    offMap[r[offIdx('VS_PLAYER_ID')]] = r[offIdx('NET_RATING')];
  }

  // Parse on-court rows, compute on/off diff
  const players = [];
  for (const r of onSet.rowSet || []) {
    const pid = r[onIdx('VS_PLAYER_ID')];
    const onNR = r[onIdx('NET_RATING')];
    const offNR = offMap[pid];
    const totalMin = r[onIdx('MIN')];
    const gp = r[onIdx('GP')];
    if (onNR == null || offNR == null || !totalMin || !gp) continue;
    const mpg = totalMin / gp; // MIN is total, convert to per game
    const onOffDiff = +(onNR - offNR).toFixed(1);
    // Minutes-weighted impact: (onOffDiff) * (mpg/48) * dampen
    const impact = +(onOffDiff * (mpg / 48) * ONOFF_DAMPEN).toFixed(1);
    // Convert "Last, First" to "First Last" for injury matching
    const rawName = r[onIdx('VS_PLAYER_NAME')] || '';
    const name = rawName.includes(',') ? rawName.split(',').map(s => s.trim()).reverse().join(' ') : rawName;
    players.push({
      name,
      playerId: pid,
      min: +mpg.toFixed(1),
      onNetRtg: +onNR.toFixed(1),
      offNetRtg: +offNR.toFixed(1),
      onOffDiff,
      impact,
    });
  }

  // Sort by minutes DESC, take top 8
  players.sort((a, b) => b.min - a.min);
  return players.slice(0, MAX_IMPACT_PLAYERS);
}

// Reverse map: abbr → teamId
const ABBR_TO_ID = {};
for (const [id, abbr] of Object.entries(NBA_COM_TEAM_MAP)) ABBR_TO_ID[abbr] = id;

const ADV_KEYS = ['pace', 'offRtg', 'defRtg', 'netRtg', 'ts', 'efg', 'astPct', 'tovPct', 'rebPct', 'pie'];
const OPP_KEYS = ['pts', 'reb', 'ast', 'fg3m', 'stl', 'blk', 'tov', 'fgPct', 'fg3Pct', 'oreb', 'dreb'];
const BLOW_KEYS = ['blowoutPct', 'blowoutWinPct', 'blowoutLossPct', 'avgMargin'];

async function writeToSupabase(data) {
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/odds_cache?on_conflict=book`, {
    method: 'POST',
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json',
      'Prefer': 'resolution=merge-duplicates',
    },
    body: JSON.stringify({
      book: 'team_stats',
      players: data,
      last_fetched: new Date().toISOString(),
    }),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Supabase write failed: ${resp.status} ${text}`);
  }
}

async function main() {
  const season = currentNBASeason();
  console.log(`Fetching NBA team stats for ${season}-${String(season + 1).slice(2)} (blended: 60% season + 25% L15 + 15% L5)...\n`);

  // --- Advanced stats: 3 windows ---
  console.log('Advanced stats:');
  const advSeason = await fetchTeamAdvancedStats(season, 0);
  console.log(`    Season: ${Object.keys(advSeason).length} teams`);
  await delay(1000);
  const advL15 = await fetchTeamAdvancedStats(season, 15);
  console.log(`    L15: ${Object.keys(advL15).length} teams`);
  await delay(1000);
  const advL5 = await fetchTeamAdvancedStats(season, 5);
  console.log(`    L5: ${Object.keys(advL5).length} teams`);
  await delay(1000);

  // --- Opponent stats: 3 windows ---
  console.log('Opponent stats allowed:');
  const oppSeason = await fetchOpponentStats(season, 0);
  console.log(`    Season: ${Object.keys(oppSeason).length} teams`);
  await delay(1000);
  const oppL15 = await fetchOpponentStats(season, 15);
  console.log(`    L15: ${Object.keys(oppL15).length} teams`);
  await delay(1000);
  const oppL5 = await fetchOpponentStats(season, 5);
  console.log(`    L5: ${Object.keys(oppL5).length} teams`);
  await delay(1000);

  // --- Home/Away splits: season only (small samples already) ---
  console.log('Home/Away splits:');
  const advHome = await fetchTeamAdvancedStats(season, 0, 'Home');
  console.log(`    Home: ${Object.keys(advHome).length} teams`);
  await delay(1000);
  const advAway = await fetchTeamAdvancedStats(season, 0, 'Road');
  console.log(`    Away: ${Object.keys(advAway).length} teams`);
  await delay(1000);

  // --- Blowout rates: 1 fetch, 3 windows computed ---
  console.log('Blowout rates:');
  const teamGames = await fetchGameLog(season);
  console.log(`    Game log: ${Object.keys(teamGames).length} teams`);

  // --- Player on/off impact: 30 calls (batched 5 at a time) ---
  console.log('Player on/off impact:');
  const allTeamAbbrs = Object.keys(advSeason);
  const playerImpactMap = {};
  const BATCH_SIZE = 5;
  for (let i = 0; i < allTeamAbbrs.length; i += BATCH_SIZE) {
    const batch = allTeamAbbrs.slice(i, i + BATCH_SIZE);
    const results = await Promise.all(
      batch.map(abbr => fetchTeamOnOff(season, ABBR_TO_ID[abbr], abbr).catch(err => {
        console.warn(`    ⚠ ${abbr} on/off failed: ${err.message}`);
        return [];
      }))
    );
    for (let j = 0; j < batch.length; j++) {
      playerImpactMap[batch[j]] = results[j];
    }
    console.log(`    Batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(allTeamAbbrs.length / BATCH_SIZE)}: ${batch.join(', ')}`);
    if (i + BATCH_SIZE < allTeamAbbrs.length) await delay(1500);
  }
  const totalPlayers = Object.values(playerImpactMap).reduce((s, p) => s + p.length, 0);
  console.log(`    ${totalPlayers} players across ${Object.keys(playerImpactMap).length} teams`);

  // --- Blend everything ---
  console.log('\nBlending windows...');
  const allTeams = Object.keys(advSeason);

  // League averages (computed from blended values)
  const avg = (obj, key) => {
    const vals = Object.values(obj).map(t => t[key]).filter(v => v != null);
    return vals.length ? +(vals.reduce((s, v) => s + v, 0) / vals.length).toFixed(1) : 0;
  };

  const combined = { _fetchedAt: new Date().toISOString() };
  for (const abbr of allTeams) {
    // Blend advanced
    const blendedAdv = blendObj(advSeason[abbr], advL15[abbr], advL5[abbr], ADV_KEYS);
    // Blend opponent allowed
    const blendedOpp = blendObj(oppSeason[abbr], oppL15[abbr], oppL5[abbr], OPP_KEYS);
    // Blend blowout rates from game log windows
    const games = teamGames[abbr] || [];
    const blowSeason = computeBlowouts(games);
    const blowL15 = computeBlowouts(games.slice(0, 15));
    const blowL5 = computeBlowouts(games.slice(0, 5));
    const blendedBlow = {
      ...blendObj(blowSeason, blowL15, blowL5, BLOW_KEYS),
      gp: blowSeason.gp,
      blowoutWins: blowSeason.blowoutWins,
      blowoutLosses: blowSeason.blowoutLosses,
    };

    // Home/Away splits (full season only — half-season samples are already small)
    const hSplit = advHome[abbr], aSplit = advAway[abbr];
    const homeSplit = hSplit ? { homeNetRtg: hSplit.netRtg, homeOffRtg: hSplit.offRtg, homeDefRtg: hSplit.defRtg, homePace: hSplit.pace } : {};
    const awaySplit = aSplit ? { awayNetRtg: aSplit.netRtg, awayOffRtg: aSplit.offRtg, awayDefRtg: aSplit.defRtg, awayPace: aSplit.pace } : {};

    // B2B detection: most recent game date (games sorted DESC)
    const lastGameDate = games.length ? games[0].date : null;

    combined[abbr] = {
      name: advSeason[abbr].name, gp: advSeason[abbr].gp,
      w: advSeason[abbr].w, l: advSeason[abbr].l,
      ...blendedAdv,
      ...homeSplit, ...awaySplit,
      lastGameDate,
      allows: blendedOpp,
      blowout: blendedBlow,
      playerImpact: playerImpactMap[abbr] || [],
    };
  }

  // League averages from blended data
  const blendedTeams = Object.values(combined).filter(t => t.pace != null);
  const blendedAllows = Object.values(combined).filter(t => t.allows?.pts != null).map(t => t.allows);
  combined._leagueAvg = {
    pace: avg(combined, 'pace') || +(blendedTeams.reduce((s, t) => s + t.pace, 0) / blendedTeams.length).toFixed(1),
    offRtg: +(blendedTeams.reduce((s, t) => s + t.offRtg, 0) / blendedTeams.length).toFixed(1),
    defRtg: +(blendedTeams.reduce((s, t) => s + t.defRtg, 0) / blendedTeams.length).toFixed(1),
    pts: +(blendedAllows.reduce((s, t) => s + t.pts, 0) / blendedAllows.length).toFixed(1),
    reb: +(blendedAllows.reduce((s, t) => s + t.reb, 0) / blendedAllows.length).toFixed(1),
    ast: +(blendedAllows.reduce((s, t) => s + t.ast, 0) / blendedAllows.length).toFixed(1),
    fg3m: +(blendedAllows.reduce((s, t) => s + t.fg3m, 0) / blendedAllows.length).toFixed(1),
    stl: +(blendedAllows.reduce((s, t) => s + t.stl, 0) / blendedAllows.length).toFixed(1),
    blk: +(blendedAllows.reduce((s, t) => s + t.blk, 0) / blendedAllows.length).toFixed(1),
    tov: +(blendedAllows.reduce((s, t) => s + t.tov, 0) / blendedAllows.length).toFixed(1),
  };

  console.log(`\nWriting to Supabase...`);
  await writeToSupabase(combined);
  console.log(`Done! ${allTeams.length} teams (blended) — ${9 + allTeams.length} API calls`);
  console.log(`League avg pace: ${combined._leagueAvg.pace}, pts allowed: ${combined._leagueAvg.pts}, reb allowed: ${combined._leagueAvg.reb}, ast allowed: ${combined._leagueAvg.ast}`);
  // Spot check
  const sampleTeam = allTeams[0];
  const s = combined[sampleTeam];
  console.log(`Sample ${sampleTeam}: homeNetRtg=${s.homeNetRtg}, awayNetRtg=${s.awayNetRtg}, lastGame=${s.lastGameDate}`);
  if (s.playerImpact?.length) {
    console.log(`  Top player: ${s.playerImpact[0].name} — ${s.playerImpact[0].min} mpg, on/off: ${s.playerImpact[0].onOffDiff > 0 ? '+' : ''}${s.playerImpact[0].onOffDiff}, impact: ${s.playerImpact[0].impact > 0 ? '+' : ''}${s.playerImpact[0].impact}`);
  }
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
