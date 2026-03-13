/**
 * One-time script to generate data/espn-players.json
 * Run: node scripts/generate-espn-players.mjs
 * Then commit: data/espn-players.json
 */
import { writeFileSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

async function main() {
  // Step 1: get all NBA team IDs
  console.log('Fetching NBA teams...');
  const teamsResp = await fetch('https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams?limit=30');
  const teamsData = await teamsResp.json();
  const teams = teamsData.sports[0].leagues[0].teams.map(t => t.team);
  console.log(`Found ${teams.length} teams`);

  // Step 2: fetch each team's roster
  const map = {};
  for (const team of teams) {
    try {
      const resp = await fetch(`https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/${team.id}/roster`);
      if (!resp.ok) { console.warn(`  ${team.abbreviation}: HTTP ${resp.status}`); continue; }
      const data = await resp.json();
      for (const athlete of (data.athletes || [])) {
        if (athlete.id && athlete.displayName) {
          map[athlete.displayName] = athlete.id;
        }
      }
      console.log(`  ${team.abbreviation}: ${(data.athletes||[]).length} players`);
    } catch (e) {
      console.warn(`  ${team.abbreviation}: ${e.message}`);
    }
  }

  const outDir = join(__dirname, '..', 'data');
  mkdirSync(outDir, { recursive: true });
  const outPath = join(outDir, 'espn-players.json');
  writeFileSync(outPath, JSON.stringify(map, null, 2));
  console.log(`\nWrote ${Object.keys(map).length} players to data/espn-players.json`);
}

main().catch(e => { console.error(e); process.exit(1); });
