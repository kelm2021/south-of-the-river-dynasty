#!/usr/bin/env node
/**
 * South of the River Dynasty — League Data Fetcher
 * ================================================
 * Pulls data from two sources:
 *   • MFL (MyFantasyLeague)  — seasons 2016–2020, league ID 75698  [ARCHIVED]
 *   • Sleeper               — seasons 2021–present, league ID 1199905589971390464
 *
 * Caching strategy:
 *   All completed seasons are saved to season_cache/<platform>_<year>.json
 *   and loaded from cache on subsequent runs — no re-fetching.
 *   Only the current live Sleeper season is fetched fresh each time.
 *
 * Output: data_fetched.js  (drop-in replacement for the app's data file)
 *
 * Usage:
 *   node fetch_league_data.js            # normal run (uses cache for old seasons)
 *   node fetch_league_data.js --force    # ignore cache, re-fetch everything
 *   node fetch_league_data.js --current  # only re-fetch current season (fastest)
 *
 * Dependencies: node-fetch (v2)
 *   npm install node-fetch@2
 */

'use strict';

const fs   = require('fs');
const path = require('path');

// ── CLI flags ────────────────────────────────────────────────────────────────
const FORCE_REFETCH   = process.argv.includes('--force');
const CURRENT_ONLY    = process.argv.includes('--current');

// ── Cache directory (sits next to this script) ───────────────────────────────
const CACHE_DIR = path.join(__dirname, 'season_cache');
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

function cachePath(platform, year) {
  return path.join(CACHE_DIR, `${platform}_${year}.json`);
}

function readCache(platform, year) {
  const p = cachePath(platform, year);
  if (fs.existsSync(p)) {
    try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch (_) {}
  }
  return null;
}

function writeCache(platform, year, data) {
  fs.writeFileSync(cachePath(platform, year), JSON.stringify(data), 'utf8');
}
let fetch;
try {
  fetch = require('node-fetch');
} catch (_) {
  console.error('\n⚠  Missing dependency. Run:  npm install node-fetch@2\n');
  process.exit(1);
}

// ─────────────────────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────────────────────
const MFL_LEAGUE_ID     = '75698';
const MFL_YEARS         = [2016, 2017, 2018, 2019, 2020];
const MFL_BASE          = 'https://www42.myfantasyleague.com';

const SLEEPER_LEAGUE_ID = '1199905589971390464';  // most recent known season (2025)
const SLEEPER_BASE      = 'https://api.sleeper.app/v1';

// Pre-discovered league chain (each season is a separate league on Sleeper).
// The fetcher walks prev_league_id automatically to detect new seasons,
// so when 2026 goes live it will be picked up without any manual changes here.
// Update this map after each season wraps up (for speed + correctness).
const SLEEPER_KNOWN_CHAIN = {
  2021: '723590872477827072',
  2022: '784485730809421824',
  2023: '919759148441493504',
  2024: '1060985798624894976',
  2025: '1199905589971390464',
  // 2026 will be added automatically when the season is created on Sleeper
};

const OUTPUT_FILE       = path.join(__dirname, 'data_fetched.js');

// Canonical name map — maps franchise IDs / usernames → real first name
const MFL_FRANCHISE_MAP = {
  '0001': 'Kent',
  '0002': 'Kris',
  '0003': 'Noah',
  '0004': 'Alex',   // Schamzy / Dirty Landry / Schamerz
  '0005': 'Jake',
  '0006': 'Ben',
  '0007': 'Dylan',
  '0008': 'Evan',
  '0009': 'Ryan',   // Straight Ca$h Homie
  '0010': 'Kyle',   // Steiners
  '0011': 'Nolan',
  '0012': 'Kevin',  // left after 2020; Bo took over for Sleeper era
};

const SLEEPER_USER_MAP = {
  '461655186021019648':  'Dylan',   // DCole08 — joined for 2021 startup, left after
  '723590717976420352':  'Kent',
  '723593197250486272':  'Jake',
  '723594258266832896':  'Nolan',
  '723604254102757376':  'Alex',
  '723612275360989184':  'Kyle',
  '723633175154225152':  'Ben',
  '723639098744586240':  'Ryan',
  '724002490193928192':  'Noah',
  '724010432502763520':  'Bo',
  '724018250857381888':  'Kris',    // krisegan22 — was in 2021 startup
  '724028510598971392':  'Evan',
  '861701837705809920':  'Gavin',
  '1248128961037209600': 'Sirac',
};

// Map roster_id → owner canonical name for Sleeper (built at runtime)
let sleeperRosterToName = {};

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────
async function getJSON(url, label = '') {
  const tag = label || url.split('/').slice(-2).join('/');
  process.stdout.write(`  Fetching ${tag}… `);
  try {
    const res = await fetch(url, { headers: { 'Accept': 'application/json' } });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    console.log('✓');
    return data;
  } catch (err) {
    console.log(`✗ (${err.message})`);
    return null;
  }
}

// Like getJSON but retries on 429 / 5xx with exponential backoff (for MFL)
async function getJSONWithRetry(url, label = '', maxRetries = 4) {
  const tag = label || url.split('/').slice(-2).join('/');
  let attempt = 0;
  while (attempt <= maxRetries) {
    if (attempt === 0) {
      process.stdout.write(`  Fetching ${tag}… `);
    } else {
      process.stdout.write(`  Retry ${attempt}/${maxRetries} ${tag}… `);
    }
    try {
      const res = await fetch(url, { headers: { 'Accept': 'application/json' } });
      if (res.status === 429 || res.status >= 500) {
        const wait = Math.min(3000 * Math.pow(1.8, attempt), 15000); // cap at 15s
        console.log(`✗ (HTTP ${res.status}) — waiting ${wait}ms`);
        await sleep(wait);
        attempt++;
        continue;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      console.log('✓');
      return data;
    } catch (err) {
      if (err.message.includes('429') || err.message.includes('503') || err.message.includes('502')) {
        const wait = Math.min(3000 * Math.pow(1.8, attempt), 15000);
        console.log(`✗ (${err.message}) — waiting ${wait}ms`);
        await sleep(wait);
        attempt++;
        continue;
      }
      console.log(`✗ (${err.message})`);
      return null;
    }
  }
  console.log(`  ✗ Gave up after ${maxRetries} retries: ${tag}`);
  return null;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function safeNum(v) {
  const n = parseFloat(v);
  return isNaN(n) ? 0 : n;
}

// ─────────────────────────────────────────────────────────────────────────────
// MFL HELPERS
// ─────────────────────────────────────────────────────────────────────────────
function mflUrl(year, type, extra = '') {
  return `${MFL_BASE}/${year}/export?TYPE=${type}&L=${MFL_LEAGUE_ID}&JSON=1${extra}`;
}

/**
 * Fetch MFL schedule + liveScoring for a season and build week-by-week matchup data.
 * Returns array of { week, matchups: [{fid1, score1, fid2, score2}] }
 */
async function fetchMFLWeeklyScores(year) {
  const weeks = [];
  const regSeasonWeeks = 13; // regular season ends week 13

  for (let w = 1; w <= regSeasonWeeks; w++) {
    const data = await getJSON(
      mflUrl(year, 'liveScoring', `&W=${w}`),
      `MFL ${year} liveScoring W${w}`
    );
    if (!data) continue;

    const matchupsRaw = data?.liveScoring?.matchup || data?.liveScoring?.matchups?.matchup || [];
    const matchupArr = Array.isArray(matchupsRaw) ? matchupsRaw : [matchupsRaw];

    const matchups = matchupArr.map(m => {
      const frs = Array.isArray(m.franchise) ? m.franchise : [m.franchise];
      const [f1, f2] = frs;
      return {
        fid1:   f1?.id,
        score1: safeNum(f1?.score),
        fid2:   f2?.id,
        score2: safeNum(f2?.score),
      };
    }).filter(m => m.fid1 && m.fid2);

    if (matchups.length) {
      weeks.push({ week: w, matchups });
    }

    await sleep(400); // be polite to MFL — they rate-limit aggressively
  }
  return weeks;
}

/**
 * Fetch MFL trades for a season.
 * Returns normalised trade objects: { timestamp, year, week, side1: {name, players, picks}, side2: ... }
 */
async function fetchMFLTrades(year) {
  const data = await getJSONWithRetry(mflUrl(year, 'transactions', '&TRANS_TYPE=TRADE'), `MFL ${year} trades`);
  if (!data) return [];

  const raw = data?.transactions?.transaction || [];
  const txArr = Array.isArray(raw) ? raw : [raw];

  // Also fetch player name lookup for this year
  const playerData = await getJSONWithRetry(mflUrl(year, 'players', '&DETAILS=1'), `MFL ${year} players`);
  const playerMap = {};
  const playerArr = playerData?.players?.player || [];
  (Array.isArray(playerArr) ? playerArr : [playerArr]).forEach(p => {
    if (p?.id) playerMap[p.id] = p.name || p.id;
  });

  function resolveAssets(idStr) {
    if (!idStr) return [];
    return idStr.split(',').filter(Boolean).map(id => {
      id = id.trim();
      if (!id) return null;
      // Future pick format: FP_<franchise>_<year>_<round>
      if (id.startsWith('FP_') || id.startsWith('DP_')) {
        const parts = id.split('_');
        const pickYear = parts[2] || '?';
        const round = parts[3] || '?';
        return { type: 'pick', desc: `${pickYear} Round ${round} Pick`, raw: id };
      }
      return { type: 'player', desc: playerMap[id] || `Player #${id}`, raw: id };
    }).filter(Boolean);
  }

  return txArr
    .filter(t => t?.type === 'TRADE' || t?.transaction?.type === 'TRADE')
    .map(t => {
      const tx = t?.transaction || t;
      const ts = parseInt(tx.timestamp || 0);
      const date = ts ? new Date(ts * 1000) : null;
      const week = date ? Math.ceil((date - new Date(`${year}-09-01`)) / (7 * 86400000)) : 1;

      return {
        year,
        week: Math.max(1, Math.min(week, 17)),
        timestamp: ts,
        side1: {
          franchise: MFL_FRANCHISE_MAP[tx.franchise]  || tx.franchise,
          assets:    resolveAssets(tx.franchise1_gave_up || ''),
        },
        side2: {
          franchise: MFL_FRANCHISE_MAP[tx.franchise2] || tx.franchise2,
          assets:    resolveAssets(tx.franchise2_gave_up || ''),
        },
        threeWay: false,
        platform: 'mfl',
      };
    });
}

/**
 * Fetch MFL waiver / FA transactions for a season.
 */
async function fetchMFLWaivers(year) {
  const data = await getJSONWithRetry(mflUrl(year, 'transactions', '&TRANS_TYPE=WAIVER'), `MFL ${year} waivers`);
  if (!data) return [];
  const raw = data?.transactions?.transaction || [];
  const txArr = Array.isArray(raw) ? raw : [raw];
  return txArr.map(t => ({
    year,
    franchise: MFL_FRANCHISE_MAP[t.franchise] || t.franchise,
    type: 'waiver',
    bid: safeNum(t.bid || 0),
    playerIn: t.player || '',
    platform: 'mfl',
  }));
}

// ─────────────────────────────────────────────────────────────────────────────
// SLEEPER HELPERS
// ─────────────────────────────────────────────────────────────────────────────
function sleeperUrl(path) {
  return `${SLEEPER_BASE}${path}`;
}

/**
 * Build the Sleeper league chain: season year → league ID.
 * Uses SLEEPER_KNOWN_CHAIN as the base, then walks prev_league_id from the
 * most recent season to pick up any NEW seasons that may have been added.
 * Returns map: { 2021: id, 2022: id, … CURRENT: id }
 */
async function buildSleeperLeagueChain() {
  // Start with known chain
  const chain = { ...SLEEPER_KNOWN_CHAIN };

  // Walk forward from the most recent known season to discover new ones
  const latestKnownYear = Math.max(...Object.keys(chain).map(Number));
  const latestId = chain[latestKnownYear];

  let leagueId = latestId;
  while (leagueId) {
    const data = await getJSON(sleeperUrl(`/league/${leagueId}`), `Sleeper league ${leagueId}`);
    if (!data) break;
    const season = parseInt(data.season);
    if (!chain[season]) {
      console.log(`  📅 Discovered new season: ${season} → ${leagueId}`);
      chain[season] = leagueId;
    }
    // Check if there's a *next* league (for future seasons - Sleeper doesn't expose this,
    // but we can re-check the latest league_id periodically)
    break; // Can't walk forward, only backward via prev_league_id
  }

  return chain;
}

/**
 * Fetch all weekly matchup scores for a Sleeper season.
 * Returns array of { week, matchups: [{roster1, score1, roster2, score2}] }
 */
async function fetchSleeperWeeklyMatchups(leagueId, totalWeeks = 14) {
  const weeks = [];
  for (let w = 1; w <= totalWeeks; w++) {
    const data = await getJSON(
      sleeperUrl(`/league/${leagueId}/matchups/${w}`),
      `Sleeper matchups W${w}`
    );
    if (!data || !Array.isArray(data)) continue;

    // Group by matchup_id
    const groups = {};
    data.forEach(entry => {
      const mid = entry.matchup_id;
      if (!mid) return;
      if (!groups[mid]) groups[mid] = [];
      groups[mid].push(entry);
    });

    const matchups = Object.values(groups)
      .filter(g => g.length === 2)
      .map(([a, b]) => ({
        roster1: a.roster_id,
        score1:  safeNum(a.points),
        starters1: a.starters || [],
        pts1:    a.players_points || {},
        roster2: b.roster_id,
        score2:  safeNum(b.points),
        starters2: b.starters || [],
        pts2:    b.players_points || {},
      }));

    if (matchups.length) weeks.push({ week: w, matchups });
    await sleep(80);
  }
  return weeks;
}

/**
 * Fetch all transactions for every week of a Sleeper season.
 * Returns trades, waivers, fa separately.
 */
async function fetchSleeperTransactions(leagueId, totalWeeks = 14) {
  const trades   = [];
  const waivers  = [];

  for (let w = 1; w <= totalWeeks; w++) {
    const data = await getJSON(
      sleeperUrl(`/league/${leagueId}/transactions/${w}`),
      `Sleeper transactions W${w}`
    );
    if (!data || !Array.isArray(data)) continue;

    data.forEach(tx => {
      if (tx.type === 'trade') {
        const rids = tx.roster_ids || [];
        const side1 = { rosterId: rids[0], assets: [], picks: [] };
        const side2 = { rosterId: rids[1], assets: [], picks: [] };
        const side3 = rids[2] ? { rosterId: rids[2], assets: [], picks: [] } : null;

        // Players
        (tx.adds ? Object.entries(tx.adds) : []).forEach(([playerId, toRoster]) => {
          const side = side1.rosterId === toRoster ? side2 :
                       side2.rosterId === toRoster ? side1 :
                       side3?.rosterId === toRoster ? (side3 || null) : null;
          if (side) side.assets.push({ type: 'player', id: playerId });
        });

        // Draft picks
        (tx.draft_picks || []).forEach(pick => {
          const to = pick.owner_id;
          const side = side1.rosterId === to ? side1 :
                       side2.rosterId === to ? side2 :
                       side3?.rosterId === to ? side3 : null;
          if (side) side.picks.push(pick);
        });

        // FAAB transfers
        const faabTransfers = tx.waiver_budget || [];

        trades.push({
          week: w,
          leagueId,
          rosterId1: side1.rosterId,
          rosterId2: side2.rosterId,
          rosterId3: side3?.rosterId || null,
          side1Assets: side1.assets,
          side2Assets: side2.assets,
          side3Assets: side3?.assets || [],
          side1Picks:  side1.picks,
          side2Picks:  side2.picks,
          side3Picks:  side3?.picks || [],
          faabTransfers,
          threeWay:    !!side3,
          platform:    'sleeper',
          timestamp:   tx.created,
          status:      tx.status,
        });
      } else if (tx.type === 'waiver' || tx.type === 'free_agent') {
        const bid   = tx.settings?.waiver_bid || 0;
        const adds  = tx.adds ? Object.keys(tx.adds) : [];
        const drops = tx.drops ? Object.keys(tx.drops) : [];
        waivers.push({
          week: w,
          leagueId,
          rosterId:  tx.roster_ids?.[0],
          type:      tx.type,
          bid:       safeNum(bid),
          adds,
          drops,
          timestamp: tx.created,
        });
      }
    });

    await sleep(80);
  }

  return { trades, waivers };
}

/**
 * Fetch all draft data for a Sleeper season:
 *   - draft metadata (type, rounds, order)
 *   - every pick made (round, pick_no, player, manager)
 *   - traded picks outstanding at end of season
 *
 * Returns { draftId, type, rounds, picks, tradedPicks, rosterMap }
 */
async function fetchSleeperDraftData(leagueId, year, rosterMap) {
  const drafts = await getJSON(sleeperUrl(`/league/${leagueId}/drafts`), `Sleeper ${year} drafts`);
  if (!drafts || !Array.isArray(drafts) || !drafts[0]) return null;

  // Rookie/annual draft is usually the most recent one
  const draft = drafts[0];
  const draftId = draft.draft_id;

  // Build draft-slot → manager map from the draft metadata
  // draft.slot_to_roster_id maps slot# (as string) → roster_id
  const slotToManager = {};
  if (draft.slot_to_roster_id) {
    for (const [slot, rosterId] of Object.entries(draft.slot_to_roster_id)) {
      slotToManager[parseInt(slot)] = rosterMap[rosterId] || `Roster ${rosterId}`;
    }
  }

  // All picks made in the draft
  const rawPicks = await getJSON(sleeperUrl(`/draft/${draftId}/picks`), `Sleeper ${year} draft picks`);
  await sleep(100);

  const picks = (rawPicks || []).map(p => {
    const manager = SLEEPER_USER_MAP[p.picked_by] || rosterMap[p.roster_id] || p.picked_by;
    const originalManager = slotToManager[p.draft_slot] || manager; // who originally held the draft slot
    const playerName = [p.metadata?.first_name, p.metadata?.last_name].filter(Boolean).join(' ')
                    || `Player #${p.player_id}`;
    const pos = p.metadata?.position || '';
    return {
      round:    p.round,
      pick:     p.pick_no,
      slotPick: p.draft_slot,  // pick position in the original draft order
      player:   pos ? `${playerName} (${pos})` : playerName,
      playerId: p.player_id,
      manager,
      originalManager: originalManager !== manager ? originalManager : null, // non-null = traded pick
      year,
    };
  });

  // Traded picks still outstanding (future picks held at end of this season's transactions)
  const tradedPicks = await getJSON(sleeperUrl(`/league/${leagueId}/traded_picks`), `Sleeper ${year} traded picks`);
  await sleep(100);

  const normalizedTradedPicks = (tradedPicks || []).map(p => ({
    season:       parseInt(p.season),
    round:        p.round,
    currentOwner: rosterMap[p.owner_id]          || SLEEPER_USER_MAP[p.owner_id]          || `Roster ${p.owner_id}`,
    originalOwner: rosterMap[p.previous_owner_id] || SLEEPER_USER_MAP[p.previous_owner_id] || `Roster ${p.previous_owner_id}`,
    rosterId:     p.roster_id,
  }));

  return {
    year,
    draftId,
    type:        draft.type,      // 'linear', 'snake', 'auction'
    rounds:      draft.settings?.rounds || picks.length / 12,
    status:      draft.status,
    picks,
    tradedPicks: normalizedTradedPicks,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// MFL DRAFT FETCHERS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Fetch all MFL player metadata for a given year (id → name lookup).
 * Cached across calls within a single run.
 */
const mflPlayerCache = {};
async function getMFLPlayerMap(year) {
  if (mflPlayerCache[year]) return mflPlayerCache[year];
  const data = await getJSONWithRetry(mflUrl(year, 'players', '&DETAILS=1'), `MFL ${year} players`);
  const arr  = data?.players?.player || [];
  const map  = {};
  (Array.isArray(arr) ? arr : [arr]).forEach(p => {
    if (p?.id) map[p.id] = { name: p.name || p.id, pos: p.position || '' };
  });
  mflPlayerCache[year] = map;
  await sleep(200);
  return map;
}

/**
 * Fetch the annual MFL rookie/free-agent draft results + future pick ownership.
 * Returns { year, picks, futurePicks }
 */
async function fetchMFLDraftData(year) {
  const playerMap = await getMFLPlayerMap(year);

  // Draft results
  const draftData = await getJSONWithRetry(mflUrl(year, 'draftResults'), `MFL ${year} draftResults`);
  await sleep(150);

  const rawPicks = draftData?.draftResults?.draftUnit?.draftPick;
  const pickArr  = Array.isArray(rawPicks) ? rawPicks : rawPicks ? [rawPicks] : [];

  const picks = pickArr.map(p => {
    const manager    = MFL_FRANCHISE_MAP[p.franchise]  || p.franchise;
    const playerInfo = playerMap[p.player] || { name: `Player #${p.player}`, pos: '' };
    const playerStr  = playerInfo.pos
      ? `${playerInfo.name} (${playerInfo.pos})`
      : playerInfo.name;
    return {
      round:   parseInt(p.round),
      pick:    parseInt(p.pick || p.overall || 0),
      player:  playerStr,
      playerId: p.player,
      manager,
      timestamp: p.timestamp ? parseInt(p.timestamp) : null,
      year,
    };
  });

  // Future (traded) picks owned by each franchise at this moment in time
  const fpData = await getJSONWithRetry(mflUrl(year, 'futureDraftPicks'), `MFL ${year} futureDraftPicks`);
  await sleep(150);

  const fpFranchises = fpData?.futureDraftPicks?.franchise;
  const fpArr = Array.isArray(fpFranchises) ? fpFranchises : fpFranchises ? [fpFranchises] : [];

  const futurePicks = [];
  fpArr.forEach(f => {
    const currentOwner = MFL_FRANCHISE_MAP[f.id] || f.id;
    const fps = f.futureDraftPick;
    const fpsArr = Array.isArray(fps) ? fps : fps ? [fps] : [];
    fpsArr.forEach(p => {
      const originalOwner = MFL_FRANCHISE_MAP[p.originalPickFor] || p.originalPickFor;
      futurePicks.push({
        season:       parseInt(p.year),
        round:        parseInt(p.round),
        currentOwner,
        originalOwner,
        traded:       currentOwner !== originalOwner,
      });
    });
  });

  // Only keep picks that have actually changed hands (traded picks)
  const tradedFuturePicks = futurePicks.filter(p => p.traded);

  return { year, picks, futurePicks, tradedFuturePicks };
}

/**
 * Summarise draft pick data across all seasons into useful stats per manager:
 *   - picks made per round (how early/late they usually pick)
 *   - total picks acquired via trade vs kept original
 *   - picks traded away
 */
function computeDraftStats(allDraftSeasons) {
  const stats = {};

  function ensure(manager) {
    if (!stats[manager]) {
      stats[manager] = {
        manager,
        totalPicks: 0,
        round1Picks: 0,
        round2Picks: 0,
        tradedPicksAcquired: 0,  // picked with a pick they received in a trade
        seasons: [],
      };
    }
  }

  allDraftSeasons.forEach(({ year, picks, tradedPicks }) => {
    // Who picked what
    (picks || []).forEach(p => {
      ensure(p.manager);
      stats[p.manager].totalPicks++;
      stats[p.manager].seasons.push(year);
      if (p.round === 1) stats[p.manager].round1Picks++;
      if (p.round === 2) stats[p.manager].round2Picks++;
      if (p.originalManager && p.originalManager !== p.manager) {
        stats[p.manager].tradedPicksAcquired++;
      }
    });
  });

  return Object.values(stats).map(s => ({
    ...s,
    seasons: [...new Set(s.seasons)].sort(),
  }));
}

// ─────────────────────────────────────────────────────────────────────────────
// SLEEPER PLAYER NAME LOOKUP
// ─────────────────────────────────────────────────────────────────────────────
let sleeperPlayers = null;

async function getSleeperPlayerName(playerId) {
  if (!sleeperPlayers) {
    console.log('  Downloading full Sleeper player database (~5MB)…');
    sleeperPlayers = await getJSON(sleeperUrl('/players/nfl'), 'Sleeper all players') || {};
  }
  const p = sleeperPlayers[playerId];
  if (!p) return `Player #${playerId}`;
  const pos  = p.position || '';
  const name = [p.first_name, p.last_name].filter(Boolean).join(' ');
  return pos ? `${name} (${pos})` : name;
}

// ─────────────────────────────────────────────────────────────────────────────
// COMPUTE STATS FROM RAW DATA
// ─────────────────────────────────────────────────────────────────────────────

/**
 * From weekly matchup data, compute per-manager:
 *   - all scores (for avg, high, low, blowouts)
 *   - H2H record vs each other manager
 */
function computeSleeperMatchupStats(allSeasonMatchups) {
  const scores    = [];  // { manager, score, week, year }
  const blowouts  = [];  // { winner, loser, margin, ... }
  const h2h       = {};  // h2h[m1][m2] = { wins, losses }

  function ensureH2H(m1, m2) {
    if (!h2h[m1]) h2h[m1] = {};
    if (!h2h[m1][m2]) h2h[m1][m2] = { wins: 0, losses: 0 };
    if (!h2h[m2]) h2h[m2] = {};
    if (!h2h[m2][m1]) h2h[m2][m1] = { wins: 0, losses: 0 };
  }

  allSeasonMatchups.forEach(({ year, weeks }) => {
    weeks.forEach(({ week, matchups }) => {
      matchups.forEach(({ roster1, score1, roster2, score2 }) => {
        const m1 = sleeperRosterToName[roster1];
        const m2 = sleeperRosterToName[roster2];
        if (!m1 || !m2) return;

        scores.push({ manager: m1, score: score1, week, year });
        scores.push({ manager: m2, score: score2, week, year });

        ensureH2H(m1, m2);
        if (score1 > score2) {
          h2h[m1][m2].wins++;
          h2h[m2][m1].losses++;
        } else if (score2 > score1) {
          h2h[m2][m1].wins++;
          h2h[m1][m2].losses++;
        }

        const margin = Math.abs(score1 - score2);
        const winner = score1 > score2 ? m1 : m2;
        const loser  = score1 > score2 ? m2 : m1;
        const wScore = Math.max(score1, score2);
        const lScore = Math.min(score1, score2);
        blowouts.push({
          winner, loser,
          margin:      parseFloat(margin.toFixed(2)),
          winnerScore: parseFloat(wScore.toFixed(2)),
          loserScore:  parseFloat(lScore.toFixed(2)),
          year, week,
        });
      });
    });
  });

  // Sort blowouts desc
  blowouts.sort((a, b) => b.margin - a.margin);

  // Highest / lowest scores
  const sortedScores = [...scores].sort((a, b) => b.score - a.score);
  const highestWeekScore = sortedScores.slice(0, 10).map(s => ({
    manager: s.manager,
    score:   parseFloat(s.score.toFixed(2)),
    year:    s.year,
    week:    s.week,
  }));
  const lowestWeekScore = sortedScores.slice(-5).reverse().map(s => ({
    manager: s.manager,
    score:   parseFloat(s.score.toFixed(2)),
    year:    s.year,
    week:    s.week,
  }));

  // Avg PPG per manager
  const byManager = {};
  scores.forEach(s => {
    if (!byManager[s.manager]) byManager[s.manager] = [];
    byManager[s.manager].push(s.score);
  });
  const avgScore = Object.entries(byManager)
    .map(([manager, arr]) => ({
      manager,
      avg:   parseFloat((arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(1)),
      count: arr.length,
    }))
    .sort((a, b) => b.avg - a.avg);

  // Points differential (PF - PA) per manager
  const pointsDiff = {};
  Object.entries(h2h).forEach(([m1, opponents]) => {
    let pf = 0, pa = 0;
    // We can't easily derive PA from the H2H matrix alone — use scores array
  });
  // Compute PF and PA from scores + matchups
  const pfMap = {}, paMap = {};
  allSeasonMatchups.forEach(({ weeks }) => {
    weeks.forEach(({ matchups }) => {
      matchups.forEach(({ roster1, score1, roster2, score2 }) => {
        const m1 = sleeperRosterToName[roster1];
        const m2 = sleeperRosterToName[roster2];
        if (!m1 || !m2) return;
        pfMap[m1] = (pfMap[m1] || 0) + score1;
        paMap[m1] = (paMap[m1] || 0) + score2;
        pfMap[m2] = (pfMap[m2] || 0) + score2;
        paMap[m2] = (paMap[m2] || 0) + score1;
      });
    });
  });
  Object.keys(pfMap).forEach(m => {
    pointsDiff[m] = parseFloat((pfMap[m] - paMap[m]).toFixed(1));
  });

  // Luck index: for each week, a manager "deserved" to win/lose based on whether
  // their score was above/below league median. Compare to actual W/L record.
  // Simplified: luckIndex = (actual wins) - (expected wins based on beating median each week)
  // Here we track: sum of (opponent score - league avg score that week)
  const luckIndex = {};
  allSeasonMatchups.forEach(({ weeks }) => {
    weeks.forEach(({ matchups }) => {
      const allScores = matchups.flatMap(m => [m.score1, m.score2]);
      const avg = allScores.reduce((a, b) => a + b, 0) / allScores.length;
      matchups.forEach(({ roster1, score1, roster2, score2 }) => {
        const m1 = sleeperRosterToName[roster1];
        const m2 = sleeperRosterToName[roster2];
        if (!m1 || !m2) return;
        // How much harder than average was each manager's opponent?
        luckIndex[m1] = (luckIndex[m1] || 0) + (score2 - avg);
        luckIndex[m2] = (luckIndex[m2] || 0) + (score1 - avg);
      });
    });
  });
  Object.keys(luckIndex).forEach(m => {
    luckIndex[m] = parseFloat(luckIndex[m].toFixed(1));
  });

  return { highestWeekScore, lowestWeekScore, biggestBlowouts: blowouts.slice(0, 10), avgScore, h2hMatrix: h2h, pointsDiff, luckIndex };
}

/**
 * From waiver data, compute per-manager stats.
 */
function computeWaiverStats(allWaivers, year, rosters) {
  const stats = {
    faabTotal:        {},
    faabAvgPerSeason: {},
    waiverClaims:     {},
    faPickups:        {},
    totalMoves:       {},
    biggestBid:       [],
  };

  allWaivers.forEach(({ season, waivers }) => {
    const seasonCounts = {};
    waivers.forEach(tx => {
      const manager = sleeperRosterToName[tx.rosterId] || tx.rosterId;
      if (!manager) return;

      if (tx.type === 'waiver') {
        stats.faabTotal[manager]    = (stats.faabTotal[manager]    || 0) + tx.bid;
        stats.waiverClaims[manager] = (stats.waiverClaims[manager] || 0) + tx.adds.length;
        seasonCounts[manager]       = (seasonCounts[manager]       || 0) + tx.bid;

        if (tx.bid > 0 && tx.adds[0]) {
          stats.biggestBid.push({
            mgr:    manager,
            amount: tx.bid,
            player: `Player #${tx.adds[0]}`, // resolved later
            year:   season,
            week:   tx.week,
          });
        }
      } else if (tx.type === 'free_agent') {
        stats.faPickups[manager] = (stats.faPickups[manager] || 0) + tx.adds.length;
      }

      stats.totalMoves[manager] = (stats.totalMoves[manager] || 0) + tx.adds.length;
    });

    Object.entries(seasonCounts).forEach(([m, total]) => {
      if (!stats.faabAvgPerSeason[m]) stats.faabAvgPerSeason[m] = { total: 0, seasons: 0 };
      stats.faabAvgPerSeason[m].total   += total;
      stats.faabAvgPerSeason[m].seasons += 1;
    });
  });

  // Convert faabAvgPerSeason to single number
  Object.keys(stats.faabAvgPerSeason).forEach(m => {
    const { total, seasons } = stats.faabAvgPerSeason[m];
    stats.faabAvgPerSeason[m] = Math.round(total / seasons);
  });

  // Sort biggest bids
  stats.biggestBid.sort((a, b) => b.amount - a.amount);
  stats.biggestBid = stats.biggestBid.slice(0, 10);

  return stats;
}

/**
 * Compute trade counts per manager and trade partner pairs.
 */
function computeTradeCounts(allTrades) {
  const perManager  = {};
  const perPair     = {};

  allTrades.forEach(trade => {
    const m1 = trade.platform === 'mfl'
      ? trade.side1?.franchise
      : sleeperRosterToName[trade.rosterId1];
    const m2 = trade.platform === 'mfl'
      ? trade.side2?.franchise
      : sleeperRosterToName[trade.rosterId2];
    const m3 = trade.threeWay
      ? (trade.platform === 'mfl' ? null : sleeperRosterToName[trade.rosterId3])
      : null;

    [m1, m2, m3].filter(Boolean).forEach(m => {
      perManager[m] = (perManager[m] || 0) + 1;
    });

    const pair12 = [m1, m2].filter(Boolean).sort().join('|');
    if (pair12.includes('|')) perPair[pair12] = (perPair[pair12] || 0) + 1;

    if (m3) {
      [[m1, m3], [m2, m3]].forEach(([a, b]) => {
        if (a && b) {
          const key = [a, b].sort().join('|');
          perPair[key] = (perPair[key] || 0) + 1;
        }
      });
    }
  });

  // Sort
  const sortedManagers = Object.fromEntries(
    Object.entries(perManager).sort((a, b) => b[1] - a[1])
  );
  const sortedPairs = Object.fromEntries(
    Object.entries(perPair).sort((a, b) => b[1] - a[1])
  );

  return { trades: sortedManagers, tradePartners: sortedPairs };
}

// ─────────────────────────────────────────────────────────────────────────────
// MFL SEASON BUILDER
// ─────────────────────────────────────────────────────────────────────────────
async function buildMFLSeason(year) {
  console.log(`\n── MFL ${year} ─────────────────────────────`);

  // Standings
  const standings = await getJSONWithRetry(mflUrl(year, 'standings'), `MFL ${year} standings`);
  await sleep(200);

  // Franchise list (to get names)
  const leagueData = await getJSONWithRetry(mflUrl(year, 'league'), `MFL ${year} league`);
  await sleep(200);

  const franchiseArr = leagueData?.league?.franchises?.franchise || [];
  const nameMap = {}; // id → franchise name
  (Array.isArray(franchiseArr) ? franchiseArr : [franchiseArr]).forEach(f => {
    nameMap[f.id] = f.name || MFL_FRANCHISE_MAP[f.id] || f.id;
  });

  const rows = standings?.leagueStandings?.franchise || [];
  const standingArr = Array.isArray(rows) ? rows : [rows];

  const seasonStandings = standingArr
    .map(f => ({
      manager:  MFL_FRANCHISE_MAP[f.id] || f.id,
      team:     nameMap[f.id]  || MFL_FRANCHISE_MAP[f.id] || f.id,
      w:        parseInt(f.h2hw  || 0),
      l:        parseInt(f.h2hl  || 0),
      pf:       parseFloat(f.pf  || 0),
      pa:       parseFloat(f.pa  || 0),
    }))
    .sort((a, b) => b.w - a.w || b.pf - a.pf);

  // Weekly scores for computing individual matchup metrics
  const weeks = await fetchMFLWeeklyScores(year);

  // Trades
  const trades = await fetchMFLTrades(year);

  // Waivers
  const waivers = await fetchMFLWaivers(year);

  // Draft picks
  const draftData = await fetchMFLDraftData(year);

  return { year, platform: 'MFL', standings: seasonStandings, weeks, trades, waivers, draftData };
}

// ─────────────────────────────────────────────────────────────────────────────
// SLEEPER SEASON BUILDER
// ─────────────────────────────────────────────────────────────────────────────
async function buildSleeperSeason(year, leagueId) {
  console.log(`\n── Sleeper ${year} (${leagueId}) ───────────────`);

  // Users + rosters (to build roster→manager map for this season)
  const [users, rosters] = await Promise.all([
    getJSON(sleeperUrl(`/league/${leagueId}/users`),   `Sleeper ${year} users`),
    getJSON(sleeperUrl(`/league/${leagueId}/rosters`), `Sleeper ${year} rosters`),
  ]);
  await sleep(100);

  const userMap = {}; // user_id → canonical name
  (users || []).forEach(u => {
    userMap[u.user_id] = SLEEPER_USER_MAP[u.user_id] || u.display_name;
  });

  // Build roster_id → canonical manager name for THIS season
  const seasonRosterMap = {};
  (rosters || []).forEach(r => {
    const name = SLEEPER_USER_MAP[r.owner_id] || userMap[r.owner_id] || r.owner_id;
    seasonRosterMap[r.roster_id] = name;
  });

  // Merge into global map
  Object.assign(sleeperRosterToName, seasonRosterMap);

  // Standings from roster records
  const seasonStandings = (rosters || [])
    .map(r => {
      const manager = seasonRosterMap[r.roster_id] || r.owner_id;
      return {
        manager,
        w:  r.settings?.wins   || 0,
        l:  r.settings?.losses || 0,
        pf: parseFloat((r.settings?.fpts || 0) + '.' + (r.settings?.fpts_decimal || 0)),
        pa: parseFloat((r.settings?.fpts_against || 0) + '.' + (r.settings?.fpts_against_decimal || 0)),
      };
    })
    .sort((a, b) => b.w - a.w || b.pf - a.pf);

  // Determine total weeks (regular season usually 14 weeks in Sleeper)
  const leagueMeta = await getJSON(sleeperUrl(`/league/${leagueId}`), `Sleeper ${year} meta`);
  await sleep(80);
  const totalWeeks = leagueMeta?.settings?.playoff_week_start
    ? leagueMeta.settings.playoff_week_start - 1
    : 14;

  // Weekly matchups
  const weeks = await fetchSleeperWeeklyMatchups(leagueId, totalWeeks);

  // Transactions
  const { trades, waivers } = await fetchSleeperTransactions(leagueId, totalWeeks + 3);

  // Draft picks (rookie draft + traded picks)
  const draftData = await fetchSleeperDraftData(leagueId, year, seasonRosterMap);

  // Playoff bracket
  const [winnersBracket, losersBracket] = await Promise.all([
    getJSON(sleeperUrl(`/league/${leagueId}/winners_bracket`), `Sleeper ${year} playoff bracket`),
    getJSON(sleeperUrl(`/league/${leagueId}/losers_bracket`),  `Sleeper ${year} losers bracket`),
  ]);
  await sleep(80);

  // Find champion from bracket
  let champion = null;
  let runnerUp = null;
  if (Array.isArray(winnersBracket)) {
    // Championship game is the last round with 2 teams
    const finals = winnersBracket.filter(m => m.r === Math.max(...winnersBracket.map(x => x.r)));
    if (finals.length) {
      const champGame = finals[0];
      const winnerRosterId = champGame.w;
      const loserRosterId  = champGame.l;
      champion = seasonRosterMap[winnerRosterId] || `Roster ${winnerRosterId}`;
      runnerUp = seasonRosterMap[loserRosterId]  || `Roster ${loserRosterId}`;
    }
  }

  return {
    year,
    platform: 'Sleeper',
    leagueId,
    champion,
    runnerUp,
    standings: seasonStandings,
    weeks,
    trades,
    waivers,
    draftData,
    rosterMap: seasonRosterMap,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// LINEUP EFFICIENCY (Sleeper era only — requires individual player points)
// ─────────────────────────────────────────────────────────────────────────────
function computeLineupEfficiency(sleeperSeasons) {
  const starterTotal = {};
  const benchTotal   = {};
  const starterCount = {};

  sleeperSeasons.forEach(({ weeks, rosterMap }) => {
    weeks.forEach(({ matchups }) => {
      matchups.forEach(({ roster1, starters1, pts1, roster2, starters2, pts2 }) => {
        [[roster1, starters1, pts1], [roster2, starters2, pts2]].forEach(([rid, starters, pts]) => {
          const manager = rosterMap?.[rid] || sleeperRosterToName[rid];
          if (!manager || !pts) return;

          Object.entries(pts).forEach(([pid, score]) => {
            if (starters.includes(pid)) {
              starterTotal[manager] = (starterTotal[manager] || 0) + score;
              starterCount[manager] = (starterCount[manager] || 0) + 1;
            } else {
              benchTotal[manager] = (benchTotal[manager] || 0) + score;
            }
          });
        });
      });
    });
  });

  return Object.keys(starterTotal).map(manager => {
    const avgStart = starterTotal[manager] / (starterCount[manager] || 1);
    const benchCt  = starterCount[manager]; // rough approximation
    const avgBench = (benchTotal[manager] || 0) / (benchCt || 1);
    const ratio    = avgBench > 0 ? avgStart / avgBench : 0;
    return {
      manager,
      ratio:    parseFloat(ratio.toFixed(2)),
      avgStart: parseFloat(avgStart.toFixed(2)),
      avgBench: parseFloat(avgBench.toFixed(2)),
    };
  }).sort((a, b) => b.ratio - a.ratio);
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────
async function main() {
  console.log('\n🏈  South of the River Dynasty — Data Fetcher');
  console.log('═══════════════════════════════════════════\n');

  // ── Phase 1: MFL (2016–2020) — all archived, always load from cache ────────
  console.log('PHASE 1 — MFL (2016–2020)  [archived seasons]');
  const mflSeasons = [];
  for (const year of MFL_YEARS) {
    const cached = !FORCE_REFETCH && readCache('mfl', year);
    if (cached) {
      console.log(`  ✓ MFL ${year} loaded from cache`);
      mflSeasons.push(cached);
      continue;
    }
    // No cache yet (first run) — fetch live and save
    try {
      console.log(`  Fetching MFL ${year} (will be cached after this)…`);
      const season = await buildMFLSeason(year);
      mflSeasons.push(season);
      writeCache('mfl', year, season);
      console.log(`  ✓ MFL ${year} cached to season_cache/mfl_${year}.json`);
    } catch (err) {
      console.error(`  ✗ MFL ${year} failed: ${err.message}`);
    }
    await sleep(3000); // longer pause between MFL seasons to avoid 429s
  }

  // ── Phase 2: Sleeper (2021–present) ─────────────────────────────────────
  console.log('\nPHASE 2 — Sleeper (2021–present)');
  const leagueChain = await buildSleeperLeagueChain();
  console.log('  League chain:');
  Object.entries(leagueChain).sort((a,b) => a[0]-b[0]).forEach(([y,id]) => {
    console.log(`    ${y} → ${id}`);
  });

  const sleeperSeasonYears = Object.keys(leagueChain).map(Number).sort();
  // The current (live) season is the highest year in the chain that has a league ID.
  // All seasons with a cache file are treated as completed regardless.
  const currentSleeperYear = Math.max(...sleeperSeasonYears);
  const sleeperSeasons = [];

  for (const year of sleeperSeasonYears) {
    // A season is "completed" if it has a cache file (written after its championship week)
    const hasCache = !!readCache('sleeper', year);
    const isCurrent = !hasCache && (year === currentSleeperYear);

    // Completed seasons: load from cache unless --force
    if (!isCurrent) {
      const cached = !FORCE_REFETCH && readCache('sleeper', year);
      if (cached) {
        console.log(`  ✓ Sleeper ${year} loaded from cache`);
        // Restore the rosterMap into the global lookup (needed for metrics computation)
        if (cached.rosterMap) {
          Object.assign(sleeperRosterToName, cached.rosterMap);
        }
        sleeperSeasons.push(cached);
        continue;
      }
    }

    // Current season (or cache miss / --force): fetch live
    const label = isCurrent ? `${year} [current — live fetch]` : `${year} (no cache yet — fetching)`;
    console.log(`\n── Sleeper ${label} ───────────────`);
    try {
      const season = await buildSleeperSeason(year, leagueChain[year]);
      sleeperSeasons.push(season);
      if (!isCurrent) {
        // Completed season — cache it so future runs skip the fetch
        writeCache('sleeper', year, season);
        console.log(`  ✓ Sleeper ${year} cached to season_cache/sleeper_${year}.json`);
      }
    } catch (err) {
      console.error(`  ✗ Sleeper ${year} failed: ${err.message}`);
    }
    await sleep(300);
  }

  // Note which year is actually live (for output header + done message)
  // If all known seasons are cached (off-season), report that clearly
  const liveYear = sleeperSeasonYears.find(y => !readCache('sleeper', y)) || currentSleeperYear;
  const isOffSeason = sleeperSeasonYears.every(y => !!readCache('sleeper', y));

  // ── Phase 3: Compile all draft data ──────────────────────────────────────
  console.log('\nPHASE 3 — Compiling draft data…');

  const allDraftSeasons = [
    ...mflSeasons.map(s => ({
      year:        s.year,
      platform:    'mfl',
      picks:       s.draftData?.picks || [],
      futurePicks: s.draftData?.futurePicks || [],
      tradedPicks: s.draftData?.tradedFuturePicks || [],
    })),
    ...sleeperSeasons.map(s => ({
      year:        s.year,
      platform:    'sleeper',
      draftId:     s.draftData?.draftId,
      type:        s.draftData?.type,
      rounds:      s.draftData?.rounds,
      picks:       s.draftData?.picks || [],
      tradedPicks: s.draftData?.tradedPicks || [],
    })),
  ];

  const draftStats = computeDraftStats(allDraftSeasons);

  // Print draft summary
  console.log('\n  Draft picks by season:');
  allDraftSeasons.forEach(d => {
    const r1 = d.picks.filter(p => p.round === 1).length;
    console.log(`    ${d.year} (${d.platform}): ${d.picks.length} picks total, ${r1} in Rd1, ${d.tradedPicks.length} traded picks tracked`);
  });

  // ── Phase 4: Compute aggregate metrics ──────────────────────────────────
  console.log('\nPHASE 4 — Computing aggregate metrics…');

  // Combined matchup stats (Sleeper era only for granular data)
  const sleeperMatchupData = sleeperSeasons.map(s => ({
    year:  s.year,
    weeks: s.weeks,
    rosterMap: s.rosterMap,
  }));

  const matchupStats = computeSleeperMatchupStats(sleeperMatchupData);

  // Trade stats (all eras)
  const allTrades  = [
    ...mflSeasons.flatMap(s => s.trades),
    ...sleeperSeasons.flatMap(s => s.trades),
  ];
  const tradeStats = computeTradeCounts(allTrades);

  // Waiver stats (Sleeper era)
  const allWaivers = sleeperSeasons.map(s => ({ season: s.year, waivers: s.waivers }));
  const waiverStats = computeWaiverStats(allWaivers, null, null);

  // Lineup efficiency
  const lineupEfficiency = computeLineupEfficiency(sleeperSeasons);

  // ── Phase 5: Build MFL-era standings champion lookup ────────────────────
  const MFL_CHAMPIONS = {
    2016: { champion: 'Kent',   runnerUp: 'Ben',  note: 'Kent def. Ben 151-148 in championship' },
    2017: { champion: 'Evan',   runnerUp: 'Ben',  note: 'Evan def. Ben 121-115 in championship — Ben went 11-2 regular season' },
    2018: { champion: 'Jake',   runnerUp: 'Kyle', note: 'Jake def. Steiners 148.9-136.8 in championship — Schamerz went 11-2 regular season' },
    2019: { champion: 'Jake',   runnerUp: 'Ben',  note: 'Jake def. Ben 141.8-122.3 in championship' },
    2020: { champion: 'Alex',   runnerUp: 'Evan', note: 'Schamzy def. Evan 188-125.6 in championship' },
  };

  // ── Phase 6: Assemble final LEAGUE_DATA structure ───────────────────────
  console.log('\nPHASE 6 — Assembling final data structure…');

  const allSeasons = [
    ...mflSeasons.map(s => ({
      year:          s.year,
      platform:      'MFL',
      champion:      MFL_CHAMPIONS[s.year]?.champion,
      runnerUp:      MFL_CHAMPIONS[s.year]?.runnerUp,
      note:          MFL_CHAMPIONS[s.year]?.note,
      championTeam:  MFL_CHAMPIONS[s.year]?.champion,
      standings:     s.standings,
    })),
    ...sleeperSeasons.map(s => ({
      year:          s.year,
      platform:      'Sleeper',
      champion:      s.champion,
      runnerUp:      s.runnerUp,
      championTeam:  s.champion,
      standings:     s.standings,
    })),
  ].sort((a, b) => a.year - b.year);

  // ── Phase 7: Write output file ──────────────────────────────────────────
  console.log('\nPHASE 7 — Writing output file…');

  const outputData = {
    LEAGUE_DATA: {
      name:    'South of the River Dynasty',
      founded: 2015,
      note:    'Started on NFL.com in 2015, moved to MFL in 2016, moved to Sleeper in 2022',
      seasons: allSeasons,
    },
    VAULT_METRICS: {
      trades:           tradeStats.trades,
      tradePartners:    tradeStats.tradePartners,
      highestWeekScore: matchupStats.highestWeekScore,
      lowestWeekScore:  matchupStats.lowestWeekScore,
      biggestBlowouts:  matchupStats.biggestBlowouts,
      avgScore:         matchupStats.avgScore,
      waiverStats:      waiverStats,
      luckIndex:        matchupStats.luckIndex,
      pointsDiff:       matchupStats.pointsDiff,
      lineupEfficiency: lineupEfficiency,
      h2hMatrix:        matchupStats.h2hMatrix,
    },
    // All draft picks across every season — both MFL and Sleeper eras
    DRAFT_DATA: {
      // Summary stats per manager
      managerStats: draftStats,
      // Full pick-by-pick history, every season
      seasons: allDraftSeasons.map(d => ({
        year:        d.year,
        platform:    d.platform,
        draftId:     d.draftId || null,
        type:        d.type || 'linear',
        rounds:      d.rounds || null,
        // Every pick made: round, pick#, player, manager, originalManager (if traded)
        picks: d.picks.map(p => ({
          round:   p.round,
          pick:    p.pick,
          player:  p.player,
          manager: p.manager,
          // originalManager is set when the pick was acquired in a trade
          ...(p.originalManager ? { originalManager: p.originalManager } : {}),
        })),
        // Picks that changed hands via trade (as tracked at season end)
        tradedPicks: (d.tradedPicks || []).map(p => ({
          season:        p.season,
          round:         p.round,
          currentOwner:  p.currentOwner,
          originalOwner: p.originalOwner,
        })),
      })),
    },
    // Raw per-season data for additional processing
    RAW: {
      mflSeasons:     mflSeasons.map(s => ({ year: s.year, standings: s.standings })),
      sleeperSeasons: sleeperSeasons.map(s => ({
        year:      s.year,
        leagueId:  s.leagueId,
        champion:  s.champion,
        runnerUp:  s.runnerUp,
        standings: s.standings,
      })),
      allTrades:   allTrades.length,
      allWaiverTx: allWaivers.reduce((n, s) => n + s.waivers.length, 0),
      totalDraftPicks: allDraftSeasons.reduce((n, d) => n + d.picks.length, 0),
      totalTradedPicks: allDraftSeasons.reduce((n, d) => n + (d.tradedPicks?.length || 0), 0),
    },
  };

  // Serialise
  const js = `// South of the River Dynasty — Auto-generated Data File
// Generated: ${new Date().toISOString()}
// Sources: MFL league 75698 (2016-2020, archived) + Sleeper league ${SLEEPER_LEAGUE_ID} (2021-present)
// Cached seasons: MFL 2016-2020, Sleeper 2021-${currentSleeperYear}  (all archived — off-season)
// Live-fetched:   none — run again once the ${currentSleeperYear + 1} season starts on Sleeper
//
// To refresh once new season is live: node fetch_league_data.js
// To rebuild everything from scratch:  node fetch_league_data.js --force

'use strict';

const LEAGUE_DATA = ${JSON.stringify(outputData.LEAGUE_DATA, null, 2)};

const VAULT_METRICS = ${JSON.stringify(outputData.VAULT_METRICS, null, 2)};

const DRAFT_DATA = ${JSON.stringify(outputData.DRAFT_DATA, null, 2)};

// Raw fetch summary
const FETCH_SUMMARY = ${JSON.stringify(outputData.RAW, null, 2)};

window.LEAGUE_DATA    = LEAGUE_DATA;
window.VAULT_METRICS  = VAULT_METRICS;
window.DRAFT_DATA     = DRAFT_DATA;
window.FETCH_SUMMARY  = FETCH_SUMMARY;
`;

  fs.writeFileSync(OUTPUT_FILE, js, 'utf8');
  console.log(`\n✅  Done! Output written to: ${OUTPUT_FILE}`);
  console.log(`    Seasons: ${allSeasons.length} total  (${MFL_YEARS.length} MFL archived + ${sleeperSeasonYears.length} Sleeper)`);
  if (isOffSeason) {
    console.log(`    Status: OFF-SEASON — all seasons archived, no live data`);
    console.log(`    Next: run again once the ${currentSleeperYear + 1} Sleeper season is created`);
  } else {
    console.log(`    Live-fetched: Sleeper ${liveYear} only`);
  }
  console.log(`    Total trades tracked: ${allTrades.length}`);
  console.log(`    Total draft picks: ${allDraftSeasons.reduce((n,d) => n + d.picks.length, 0)}`);
  console.log(`\n  💾  Cached seasons: season_cache/  (${fs.readdirSync(CACHE_DIR).length} files)`);
  console.log('  ℹ  To force a full re-fetch: node fetch_league_data.js --force\n');
}

main().catch(err => {
  console.error('\n💥 Fatal error:', err);
  process.exit(1);
});
