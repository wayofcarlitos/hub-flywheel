#!/usr/bin/env python3
"""
flywheel_status.py — builds /var/www/hub/flywheel.json for the Hub "FLYWHEEL" tab (Ballena, 2026-10-09). Cron */2.
Per cron job: schedule, last real start (from syslog CRON lines), expected last slot, MISSED detection, and error scan of its log.
Plus: daemons, shadow/proposal pipeline, last pulse, LLM pool usage, remediator actions.
"""
import re, os, json, sqlite3, subprocess, time
from datetime import datetime, timezone, timedelta

OUT = '/var/www/hub/flywheel.json'
NOW = datetime.now(timezone.utc)

# script keyword -> (stage, label, what it does)
META = [
 ('news-ingest.py',        '1 SENSE',  'News ingest',            'Pulls headlines into news_feed'),
 ('macro-ingest-v2.py',    '1 SENSE',  'Macro ingest',           'Fear/greed, macro score'),
 ('macro-ingest-cmc.py',   '1 SENSE',  'CMC ingest',             'CoinMarketCap macro data'),
 ('orderbook-imbalance.py','1 SENSE',  'Orderbook imbalance',    'Bid/ask pressure'),
 ('whale-alerts.py',       '1 SENSE',  'Whale alerts',           'Large on-chain moves'),
 ('volume-profile.py',     '1 SENSE',  'Volume profile',         'Volume-at-price levels'),
 ('liquidation-heatmap.py','1 SENSE',  'Liquidation heatmap',    'Liquidation clusters'),
 ('correlation-calculator.py','1 SENSE','Correlations',          'Cross-asset correlation'),
 ('full-backfill.py',      '1 SENSE',  'Candle backfill',        'Fills missing price history'),
 ('hmm_regime.py',         '1 SENSE',  'HMM regime model',       'Statistical regime detector'),
 ('legend-intelligence.py','1 SENSE',  'Legend intel + patterns','Tracked-trader pattern mining'),
 ('ai-brain.py score',     '2 SCORE',  'AI signal scorer',       'Scores signals 0-100 (Groq free pools)'),
    ('legend_copier.py',       '2 SCORE',  'Legend copier',          'Mirrors top 4 profitable legends'),
    ('trading_journal_ingest.py','3 LEARN', 'Trading journal', 'Extract + score profitable patterns'),
 ('signal-outcomes.py',    '2 SCORE',  'Signal outcomes + intel','Did past signals work? feeds scorecards'),
 ('ground_truth.py',       '2 SCORE',  'Ground truth (PnL)',     'Real PnL calendar for the Hub'),
 ('ai-brain.py debrief',   '3 LEARN',  'Daily AI debrief',       'Why did we win/lose yesterday?'),
 ('daily-pulse.py',        '3 LEARN',  'Daily pulse',            'Market regime + bounded param tweaks'),
 ('bot-self-analyzer.py',  '3 LEARN',  'Bot self-analyzer',      'Per-bot what-went-wrong analysis'),
 ('weekly-mutation.py',    '4 MUTATE', 'Weekly mutation',        'AI proposes strategy changes (Mon)'),
 ('proposal-reviewer.py',  '4 MUTATE', 'Proposal reviewer',      'Approves/rejects proposals'),
 ('proposal-applier.py',   '4 MUTATE', 'Proposal applier',       'Spawns shadow bots from approved proposals'),
 ('shadow_wr_calculator.py','5 JUDGE', 'Shadow judge',           'PASS->promote / FAIL->kill (single judge)'),
 ('function-monitor.py',   '6 HEAL',   'Function monitor',       '8 live health checks'),
 ('flywheel_remediator.py','6 HEAL',   'Auto-remediator',        'Restarts dead shadows, adopts orphans'),
 ('health-monitor.sh',     '6 HEAL',   'Health monitor',         'Service health'),
 ('deep-health-check.sh',  '6 HEAL',   'Deep health check',      'Deeper service/DB checks'),
 ('master-alerts.py',      '6 HEAL',   'Master alerts',          'Alert aggregation'),
 ('backup-db.sh',          '7 INFRA',  'DB backup',              'Nightly DB backup'),
 ('db-cleanup.py',         '7 INFRA',  'DB cleanup',             'Prunes old rows'),
 ('api_spend_tracker.py',  '7 INFRA',  'API spend tracker',      'OpenRouter spend -> Tab 2'),
 ('strategy-screener.py',  '7 INFRA',  'Chad strategy screener', 'Chad strategy ranking'),
 ('flywheel_status.py',    '7 INFRA',  'This tab\'s collector',   'Builds flywheel.json'),
]

def parse_field(f, lo, hi):
    out = set()
    for part in f.split(','):
        step = 1
        if '/' in part: part, s = part.split('/'); step = int(s)
        if part == '*': a, b = lo, hi
        elif '-' in part: a, b = map(int, part.split('-'))
        else:
            a = int(part); b = hi if step > 1 else a
        out.update(range(a, b + 1, step))
    return out

def prev_slot(expr, now):
    m, h, dom, mon, dow = expr.split()
    M, H = parse_field(m, 0, 59), parse_field(h, 0, 23)
    D, MO = parse_field(dom, 1, 31), parse_field(mon, 1, 12)
    W = {x % 7 for x in parse_field(dow, 0, 7)}
    t = now.replace(second=0, microsecond=0)
    for _ in range(60 * 24 * 8):
        if t.minute in M and t.hour in H and t.month in MO and t.day in D and ((t.weekday() + 1) % 7) in W and t <= now:
            return t
        t -= timedelta(minutes=1)
    return None

def interval_min(expr):
    m, h, dom, mon, dow = expr.split()
    if dow != '*': return 7 * 1440
    if h == '*':
        if m.startswith('*/'): return int(m[2:])
        return 60
    return 1440

# ---- crontab
cron = subprocess.run(['crontab', '-l'], capture_output=True, text=True).stdout.splitlines()
jobs = []
for line in cron:
    s = line.strip()
    if not s or s.startswith('#') or '=' in s.split()[0]: continue
    p = s.split(None, 5)
    if len(p) < 6: continue
    expr, cmd = ' '.join(p[:5]), p[5]
    jobs.append((expr, cmd))

# ---- syslog: last CRON start per command
syslog_last = {}
try:
    out = subprocess.run("grep -a 'CRON\\[.*CMD (' /var/log/syslog | tail -n 60000", shell=True, capture_output=True, text=True).stdout
    for ln in out.splitlines():
        m = re.match(r'(\S+)\s+\S+ CRON\[\d+\]: \(root\) CMD \((.*)\)\s*$', ln)
        if m:
            syslog_last[m.group(2).strip()] = m.group(1)
except Exception:
    pass

def logfile_of(cmd):
    m = re.findall(r'>>\s*(\S+)', cmd)
    return m[-1] if m else None

TS_RE = re.compile(r'^\[?(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})')
def line_dt(l):
    m = TS_RE.match(l.strip())
    if not m: return None
    try: return datetime.fromisoformat(m.group(1) + 'T' + m.group(2)).replace(tzinfo=timezone.utc)
    except Exception: return None

def scan_log(path, since_ts):
    if not path or not os.path.exists(path): return None, 'no log'
    try:
        with open(path, 'rb') as f:
            f.seek(0, 2); sz = f.tell(); f.seek(max(0, sz - 6000)); txt = f.read().decode('utf8', 'ignore')
    except Exception as e:
        return None, str(e)
    lines = [l for l in txt.splitlines() if l.strip()]
    bad = None
    cutoff = since_ts - timedelta(minutes=2) if since_ts else None
    stale_from = None
    if cutoff:
        # ignore everything up to the last timestamped line older than this run's start; untimestamped
        # lines are kept only if they come after a timestamped line that is within the latest run
        keep = []; in_run = False; seen_ts = False
        for l in lines[-60:]:
            dt = line_dt(l)
            if dt is not None:
                seen_ts = True; in_run = dt >= cutoff
            if in_run or not seen_ts and False: keep.append(l)
        lines_scan = keep
    else:
        lines_scan = lines[-40:]
    for l in reversed(lines_scan):
        if 'Traceback' in l or re.search(r'\b(Error|ERROR|FATAL|Exception)\b', l):
            bad = ('BROKEN', l.strip()[:140]); break
        if 'http_429' in l or 'AI unavailable' in l:
            bad = ('WARN', l.strip()[:140]); break
    return bad, (lines[-1].strip()[:140] if lines else '')

rows = []
for expr, cmd in jobs:
    meta = next((x for x in META if x[0] in cmd), None)
    stage, label, what = (meta[1], meta[2], meta[3]) if meta else ('7 INFRA', cmd.split('/')[-1].split()[0][:30], '')
    slot = prev_slot(expr, NOW)
    ls = syslog_last.get(cmd.strip())
    last_start = None
    if ls:
        try: last_start = datetime.fromisoformat(ls)
        except Exception: pass
    lf = logfile_of(cmd)
    bad, lastline = scan_log(lf, last_start)
    mtime = datetime.fromtimestamp(os.path.getmtime(lf), timezone.utc) if lf and os.path.exists(lf) else None
    iv = interval_min(expr)
    status, note = 'OK', ''
    if slot and (NOW - slot) > timedelta(minutes=3):
        if last_start is None or last_start < slot - timedelta(seconds=90):
            if last_start is None and (NOW - slot) < timedelta(hours=26):
                note = 'no CRON record in syslog window'
                status = 'UNKNOWN' if mtime and mtime >= slot - timedelta(minutes=2) else 'MISSED'
            else:
                status = 'MISSED'; note = f'expected {slot.strftime("%H:%M")}Z, last start older'
    if status == 'OK' and bad:
        status, note = bad
    if 'daily-pulse.py' in cmd and status in ('OK', 'WARN'):
        try:
            _a = sqlite3.connect('file:/root/p3rp-api/analysis.db?mode=ro', uri=True, timeout=5)
            _r = _a.execute("SELECT confidence, created_at FROM daily_pulse ORDER BY created_at DESC LIMIT 1").fetchone(); _a.close()
            _age = NOW - datetime.fromisoformat(_r[1]).replace(tzinfo=timezone.utc) if _r and datetime.fromisoformat(_r[1]).tzinfo is None else (NOW - datetime.fromisoformat(_r[1]))
            if _r and _r[0] >= 50 and _age < timedelta(hours=30): status, note = 'OK', ''
            elif _r and _r[0] < 50: status, note = 'WARN', f'latest pulse confidence {_r[0]}% (AI fell back to default)'
        except Exception: pass
    # stale data: log not touched for a long time relative to interval, even though ran
    rows.append({
        'stage': stage, 'label': label, 'what': what, 'schedule': expr,
        'every_min': iv, 'status': status, 'note': note,
        'last_start': last_start.isoformat() if last_start else None,
        'log_updated': mtime.isoformat() if mtime else None,
        'last_line': lastline, 'expected_slot': slot.isoformat() if slot else None,
        'log': lf,
    })

SHARED_KEYS = ('backup-db','db-cleanup','health-monitor','deep-health-check','function-monitor','flywheel_status','api_spend_tracker','ground_truth','master-alerts')
def classify(cmd):
    if 'chad-api' in cmd: return 'chad'
    if any(k in cmd for k in SHARED_KEYS): return 'shared'
    return 'p3rp'
for r, (expr, cmd) in zip(rows, jobs):
    r['system'] = classify(cmd)
rows.sort(key=lambda r: (r['stage'], r['label']))

# ---- pipeline numbers
pipe = {}
try:
    a = sqlite3.connect('file:/root/p3rp-api/analysis.db?mode=ro', uri=True, timeout=5)
    pipe['shadow'] = dict(a.execute("SELECT status,COUNT(*) FROM shadow_bots GROUP BY 1").fetchall())
    pipe['proposals'] = dict(a.execute("SELECT status,COUNT(*) FROM ai_strategy_proposals GROUP BY 1").fetchall())
    r = a.execute("SELECT date,market_regime,suggested_bias,confidence,param_tweaks,created_at FROM daily_pulse ORDER BY created_at DESC LIMIT 1").fetchone()
    if r: pipe['pulse'] = {'date': r[0], 'regime': r[1], 'bias': r[2], 'confidence': r[3], 'tweaks': r[4], 'created_at': r[5]}
    r = a.execute("SELECT COUNT(*), SUM(CASE WHEN reasoning LIKE '%unavailable%' THEN 1 ELSE 0 END) FROM ai_signal_scores WHERE created_at > datetime('now','-24 hours')").fetchone()
    pipe['scores_24h'] = {'total': r[0], 'ai_unavailable': r[1] or 0}
    a.close()
except Exception as e:
    pipe['error'] = str(e)[:100]
promoted = 0
try: promoted = len([f for f in os.listdir('/root/p3rp-api/bots/configs') if f.startswith('promoted-')])
except Exception: pass
pipe['promoted_live'] = promoted

# LLM usage today
llm = {}
try:
    today = NOW.strftime('%Y-%m-%d'); calls = {}; fails = {}
    for ln in open('/root/p3rp-api/llm_usage.jsonl'):
        try: d = json.loads(ln)
        except Exception: continue
        if not str(d.get('ts', '')).startswith(today): continue
        k = d.get('model', '?')
        if d.get('status') == 200: calls[k] = calls.get(k, 0) + 1; llm.setdefault('tokens', {}); llm['tokens'][k] = llm['tokens'].get(k, 0) + (d.get('tokens') or 0)
        else: fails[k] = fails.get(k, 0) + 1
    llm['ok'] = calls; llm['fail'] = fails
except Exception as e:
    llm['error'] = str(e)[:80]

# daemons
def count_units(pat, state='active'):
    o = subprocess.run(['systemctl', 'list-units', pat, '--no-legend', f'--state={state}'], capture_output=True, text=True).stdout
    return len([l for l in o.splitlines() if l.strip()])
daemons = {
    'p3rp_farm_active': count_units('p3rp-farm@*'), 'p3rp_farm_failed': count_units('p3rp-farm@*', 'failed'),
    'chad_farm_active': count_units('chad-farm@*'), 'chad_farm_failed': count_units('chad-farm@*', 'failed'),
}
for u in ('chad-api', 'p3rp-api', 'p3rp-watchdog', 'nginx'):
    r = subprocess.run(['systemctl', 'is-active', u], capture_output=True, text=True).stdout.strip()
    daemons[u] = r

# monitor checks
mon = None
try: mon = json.load(open('/tmp/p3rp-function-monitor.json'))
except Exception: pass

# recent flywheel actions
acts = []
try:
    for ln in open('/root/p3rp-api/flywheel_actions.jsonl').read().splitlines()[-12:]:
        try: acts.append(json.loads(ln))
        except Exception: pass
except Exception: pass


# ---- CHAD section
import glob as _glob
CH = '/root/chad-api'
def _env(path):
    d = {}
    for l in open(path):
        l = l.strip()
        if '=' in l and not l.startswith('#'):
            k, v = l.split('=', 1); d[k] = v
    return d
def _age_min(path):
    try: return (time.time() - os.path.getmtime(path)) / 60
    except Exception: return None
chad = {'bots': [], 'stages': [], 'candidates': {}}
try:
    led = sqlite3.connect(f'file:{CH}/chad_ledger.db?mode=ro', uri=True, timeout=5)
    tstats = {r[0]: r[1:] for r in led.execute("SELECT bot_type, COUNT(CASE WHEN exit_reason IS NOT NULL THEN 1 END), COUNT(CASE WHEN exit_reason IS NULL THEN 1 END), ROUND(COALESCE(SUM(CASE WHEN exit_reason IS NOT NULL THEN pnl END),0),0), COUNT(CASE WHEN exit_reason IS NOT NULL AND pnl>0 THEN 1 END) FROM trades GROUP BY bot_type")}
    lastsig = dict(led.execute("SELECT bot_type, MAX(timestamp) FROM signals GROUP BY bot_type").fetchall())
    led.close()
    for cfg in sorted(_glob.glob(f'{CH}/bots/configs/*.env')):
        name = os.path.basename(cfg)[:-4]; e = _env(cfg); bid = e.get('BOT_ID', name)
        unit = f'chad-farm@{name}'
        act = subprocess.run(['systemctl', 'is-active', unit], capture_output=True, text=True).stdout.strip()
        if act != 'active':   # some Chad bots run under a dedicated unit (e.g. chad-market-maker.service)
            for alt in (f'chad-{name}', f'chad-{name}.service'):
                a2 = subprocess.run(['systemctl', 'is-active', alt], capture_output=True, text=True).stdout.strip()
                if a2 == 'active': act = a2; break
        lp = e.get('LOG_PATH') or f'{CH}/bots/logs/{name}.log'
        if not os.path.exists(lp): lp = f'{CH}/bots/logs/{bid}.log'
        la = _age_min(lp); bal = None; cyc = None
        try:
            with open(lp, 'rb') as f:
                f.seek(0, 2); sz = f.tell(); f.seek(max(0, sz - 4000)); tail = f.read().decode('utf8', 'ignore')
            m = re.findall(r'Cycle (\d+) \|.*balance=\$([\d,\.]+)', tail)
            if m: cyc, bal = int(m[-1][0]), float(m[-1][1].replace(',', ''))
        except Exception: pass
        n, op, pnl, wins = tstats.get(bid, (0, 0, 0, 0))
        if act != 'active': st, note = 'STOPPED', 'service not running'
        elif la is None or la > 5: st, note = 'BROKEN', f'log silent {int(la) if la else "?"}m'
        else: st, note = 'OK', ''
        chad['bots'].append({'name': name, 'bot_id': bid, 'strategy': e.get('BOT_TYPE') or e.get('STRATEGY') or name,
            'tf': e.get('TIMEFRAME', '-'), 'coins': e.get('SYMBOL_LIST', ''), 'lev': e.get('LEVERAGE'),
            'ai_threshold': e.get('AI_THRESHOLD') or e.get('AI_CONFIDENCE_THRESHOLD'), 'status': st, 'note': note,
            'balance': bal, 'closed': n, 'open': op, 'pnl': pnl, 'wr': round(100 * wins / n) if n else None,
            'last_signal': lastsig.get(bid), 'log_age_min': round(la, 1) if la is not None else None})
except Exception as ex:
    chad['error'] = str(ex)[:120]
try:
    ca = sqlite3.connect(f'file:{CH}/chad_analysis.db?mode=ro', uri=True, timeout=5)
    chad['candidates'] = dict(ca.execute("SELECT status, COUNT(*) FROM strategy_candidates GROUP BY 1").fetchall()); ca.close()
except Exception: pass
pf = _age_min('/tmp/chad_price_cache.json')
scr = next((r for r in rows if r['system'] == 'chad'), None)
mon_chad = next((c for c in (mon if isinstance(mon, list) else (mon or {}).get("checks", [])) if isinstance(c, dict) and c.get("name") == "chad_signals"), None)
chad['stages'] = [
  {'n': '1 SENSE', 'name': 'Price feed (ws_price_feeder)', 'status': 'OK' if pf is not None and pf < 3 else 'BROKEN', 'note': f'price cache {int(pf)}m old' if pf is not None else 'no cache'},
  {'n': '2 SCORE', 'name': 'AI gate on entries', 'status': 'OK', 'note': 'ai-brain-score.py, thresholds per bot (65-68)'},
  {'n': '3 LEARN', 'name': 'Daily pulse (chad-daily-pulse.py)', 'status': 'OK', 'note': 'Runs 6 AM UTC, sets per-strategy thresholds'},
  {'n': '4 SCREEN', 'name': 'Strategy screener (nightly 02:00 UTC)', 'status': scr['status'] if scr else 'UNKNOWN', 'note': (scr['note'] or scr['last_line']) if scr else ''},
  {'n': '5 JUDGE', 'name': 'Shadow judge (chad_wr_calculator.py)', 'status': 'OK', 'note': f"{chad['candidates'].get('SHADOW', 0)} SHADOW candidates, judge runs 6 PM UTC"},
  {'n': '6 HEAL', 'name': 'Monitor (Chad signal flow check)', 'status': (mon_chad or {}).get('status', 'UNKNOWN'), 'note': (mon_chad or {}).get('message', '')[:120]},
]
bs = chad['bots']
chad['health'] = ('BROKEN' if any(b['status'] == 'BROKEN' for b in bs) or any(x['status'] == 'BROKEN' for x in chad['stages'])
                  else 'WARN' if any(x['status'] in ('WARN', 'FAIL') for x in chad['stages']) else 'OK')
for x in chad['stages']:
    if x['status'] == 'FAIL': x['status'] = 'BROKEN'
chad['totals'] = {'bots': len(bs), 'running': sum(1 for b in bs if b['status'] == 'OK'), 'pnl': sum(b['pnl'] for b in bs),
                  'open': sum(b['open'] for b in bs), 'closed': sum(b['closed'] for b in bs)}

systems = {}
for sysname in ('p3rp', 'chad', 'shared'):
    rs = [r for r in rows if r['system'] == sysname]
    systems[sysname] = {k: sum(1 for r in rs if r['status'] == k) for k in ('OK', 'WARN', 'BROKEN', 'MISSED', 'UNKNOWN')}
    systems[sysname]['total'] = len(rs)
summary = {s: sum(1 for r in rows if r['status'] == s) for s in ('OK', 'WARN', 'BROKEN', 'MISSED', 'UNKNOWN')}
gt = {}
try: gt = json.load(open('/tmp/ground_truth.json'))
except Exception: pass
p3rp_totals = {'pnl': gt.get('p3rp_alltime_pnl'), 'trades': gt.get('p3rp_alltime_trades'), 'wr': gt.get('p3rp_alltime_wr'),
               'open': gt.get('p3rp_open_positions'), 'bots_active': daemons.get('p3rp_farm_active'), 'bots_failed': daemons.get('p3rp_farm_failed')}
doc = {'p3rp_totals': p3rp_totals, 'generated': NOW.isoformat(), 'summary': summary, 'systems': systems, 'chad': chad, 'jobs': rows, 'pipeline': pipe, 'llm_today': llm,
       'daemons': daemons, 'monitor': mon, 'actions': acts[::-1]}
tmp = OUT + '.tmp'; json.dump(doc, open(tmp, 'w'), default=str); os.replace(tmp, OUT); os.chmod(OUT, 0o644)
print('ok', summary)
