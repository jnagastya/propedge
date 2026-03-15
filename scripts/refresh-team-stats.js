// ============================================================
// Standalone script: Fetch NBA.com team stats → write to Supabase
// Run locally: node scripts/refresh-team-stats.js
// NBA.com blocks cloud IPs (Vercel, GitHub Actions), so this
// must run from a residential IP (your machine).
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
  console.log(`Fetching ${endpoint}...`);
  const resp = await fetch(url, { headers: NBA_STATS_HEADERS });
  if (!resp.ok) throw new Error(`NBA Stats ${endpoint} returned ${resp.status}`);
  return resp.json();
}

async function fetchTeamAdvancedStats(season) {
  const seasonStr = `${season}-${String(season + 1).slice(2)}`;
  const json = await fetchNBAStats('leaguedashteamstats', {
    MeasureType: 'Advanced', PerMode: 'PerGame', Season: seasonStr,
    SeasonType: 'Regular Season', LeagueID: '00', TeamID: '0',
    OpponentTeamID: '0', Conference: '', Division: '', GameScope: '',
    GameSegment: '', DateFrom: '', DateTo: '', LastNGames: '0',
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
      name: r[idx('TEAM_NAME')], gp: r[idx('GP')], w: r[idx('W')], l: r[idx('L')],
      pace: r[idx('PACE')], offRtg: r[idx('OFF_RATING')], defRtg: r[idx('DEF_RATING')],
      netRtg: r[idx('NET_RATING')], ts: r[idx('TS_PCT')], efg: r[idx('EFG_PCT')],
      astPct: r[idx('AST_PCT')], tovPct: r[idx('TM_TOV_PCT')], rebPct: r[idx('REB_PCT')],
      pie: r[idx('PIE')],
    };
  }
  return teams;
}

async function fetchDvPStats(season) {
  const seasonStr = `${season}-${String(season + 1).slice(2)}`;
  const json = await fetchNBAStats('leaguedashptdefend', {
    DefenseCategory: 'Overall', Season: seasonStr,
    SeasonType: 'Regular Season', PerMode: 'PerGame', LeagueID: '00',
    TeamID: '', PlayerPosition: '', OpponentTeamID: '',
    Conference: '', Division: '', DateFrom: '', DateTo: '',
    GameSegment: '', LastNGames: '0', Location: '', Month: '0',
    Outcome: '', PORound: '0', Period: '0',
  });
  const headers = json.resultSets?.[0]?.headers || [];
  const rows = json.resultSets?.[0]?.rowSet || [];
  const idx = (name) => headers.indexOf(name);

  const teamDefense = {};
  for (const r of rows) {
    const teamId = r[idx('PLAYER_LAST_TEAM_ID')];
    const abbr = NBA_COM_TEAM_MAP[teamId];
    if (!abbr) continue;
    let pos = (r[idx('PLAYER_POSITION')] || '').toUpperCase();
    if (pos === 'G' || pos === 'G-F') pos = 'G';
    else if (pos === 'F' || pos === 'F-G' || pos === 'F-C') pos = 'F';
    else if (pos === 'C' || pos === 'C-F') pos = 'C';
    else continue;

    if (!teamDefense[abbr]) teamDefense[abbr] = {};
    if (!teamDefense[abbr][pos]) teamDefense[abbr][pos] = { fga: 0, fgm: 0, normalFgPct: 0, count: 0 };
    const entry = teamDefense[abbr][pos];
    entry.fga += r[idx('D_FGA')] || 0;
    entry.fgm += r[idx('D_FGM')] || 0;
    entry.normalFgPct += r[idx('NORMAL_FG_PCT')] || 0;
    entry.count++;
  }

  const dvp = {};
  for (const [abbr, positions] of Object.entries(teamDefense)) {
    dvp[abbr] = {};
    for (const [pos, d] of Object.entries(positions)) {
      const actualFgPct = d.fga > 0 ? d.fgm / d.fga : 0;
      const avgNormalFgPct = d.count > 0 ? d.normalFgPct / d.count : 0;
      dvp[abbr][pos] = {
        fga: d.fga, fgm: d.fgm,
        fgPct: +(actualFgPct * 100).toFixed(1),
        normalFgPct: +(avgNormalFgPct * 100).toFixed(1),
        diffPct: +((actualFgPct - avgNormalFgPct) * 100).toFixed(1),
      };
    }
  }
  return dvp;
}

async function fetchBlowoutRates(season) {
  const seasonStr = `${season}-${String(season + 1).slice(2)}`;
  const json = await fetchNBAStats('leaguegamelog', {
    PlayerOrTeam: 'T', Season: seasonStr, SeasonType: 'Regular Season',
    LeagueID: '00', Counter: '0', Sorter: 'DATE', Direction: 'DESC',
    DateFrom: '', DateTo: '',
  });
  const headers = json.resultSets?.[0]?.headers || [];
  const rows = json.resultSets?.[0]?.rowSet || [];
  const idx = (name) => headers.indexOf(name);

  const BLOWOUT_MARGIN = 15;
  const teams = {};
  for (const r of rows) {
    const abbr = r[idx('TEAM_ABBREVIATION')];
    if (!abbr) continue;
    if (!teams[abbr]) teams[abbr] = { gp: 0, blowoutWins: 0, blowoutLosses: 0, totalMargin: 0 };
    const t = teams[abbr];
    const margin = r[idx('PLUS_MINUS')] || 0;
    const wl = r[idx('WL')];
    t.gp++;
    t.totalMargin += margin;
    if (margin >= BLOWOUT_MARGIN && wl === 'W') t.blowoutWins++;
    else if (margin <= -BLOWOUT_MARGIN && wl === 'L') t.blowoutLosses++;
  }

  const blowouts = {};
  for (const [abbr, t] of Object.entries(teams)) {
    const totalBlowouts = t.blowoutWins + t.blowoutLosses;
    blowouts[abbr] = {
      gp: t.gp, blowoutWins: t.blowoutWins, blowoutLosses: t.blowoutLosses,
      blowoutPct: t.gp ? +(totalBlowouts / t.gp * 100).toFixed(1) : 0,
      avgMargin: t.gp ? +(t.totalMargin / t.gp).toFixed(1) : 0,
    };
  }
  return blowouts;
}

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
  console.log(`Fetching NBA team stats for ${season}-${String(season + 1).slice(2)}...`);

  const advanced = await fetchTeamAdvancedStats(season);
  console.log(`  Advanced stats: ${Object.keys(advanced).length} teams`);
  await new Promise(r => setTimeout(r, 1000));

  const dvp = await fetchDvPStats(season);
  console.log(`  DvP stats: ${Object.keys(dvp).length} teams`);
  await new Promise(r => setTimeout(r, 1000));

  const blowouts = await fetchBlowoutRates(season);
  console.log(`  Blowout rates: ${Object.keys(blowouts).length} teams`);

  // League averages
  const allTeams = Object.values(advanced);
  const leagueAvg = {
    pace: +(allTeams.reduce((s, t) => s + (t.pace || 0), 0) / allTeams.length).toFixed(1),
    offRtg: +(allTeams.reduce((s, t) => s + (t.offRtg || 0), 0) / allTeams.length).toFixed(1),
    defRtg: +(allTeams.reduce((s, t) => s + (t.defRtg || 0), 0) / allTeams.length).toFixed(1),
  };

  // Combine
  const combined = { _leagueAvg: leagueAvg, _fetchedAt: new Date().toISOString() };
  for (const abbr of Object.keys(advanced)) {
    combined[abbr] = {
      ...advanced[abbr],
      dvp: dvp[abbr] || {},
      blowout: blowouts[abbr] || {},
    };
  }

  console.log(`Writing to Supabase...`);
  await writeToSupabase(combined);
  console.log(`Done! League avg pace: ${leagueAvg.pace}, ORTG: ${leagueAvg.offRtg}, DRTG: ${leagueAvg.defRtg}`);
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
