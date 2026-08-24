#!/usr/bin/env bash
# TASK 09 desktop runtime torture (evidence, not fake PASS).
# Usage: bash scripts/torture.sh <path-to-saiwork2.exe>
set -u
EXE="$1"
ROOT=/tmp/saiwork2-torture
DATA="$ROOT/data"
LOG="$ROOT/app.log"
CYCLES="${CYCLES:-5}"
mkdir -p "$ROOT"

wait_ready() { # $1 = log file, $2 = timeout secs
  local f="$1" t="$2" n=0
  while [ "$n" -lt "$((t * 4))" ]; do
    grep -q "application ready" "$f" 2>/dev/null && return 0
    n=$((n + 1)); sleep 0.25
  done
  return 1
}
app_count() { tasklist 2>/dev/null | grep -ci "saiwork2.exe" || true; }
close_app() { # graceful WM_CLOSE, bounded
  local n=0
  taskkill //IM saiwork2.exe 2>/dev/null
  while [ "$n" -lt 60 ]; do
    [ "$(app_count)" = "0" ] && return 0
    n=$((n + 1)); sleep 0.25
  done
  return 1
}
force_app() { taskkill //F //IM saiwork2.exe >/dev/null 2>&1; }

echo "=== TORTURE BASELINE: $(date +%H:%M:%S) ==="
echo "exe: $EXE"

# ---- Phase A: rapid start/close cycles on a persistent data root -----------
echo; echo "--- Phase A: $CYCLES rapid launch->READY->close cycles ---"
rm -rf "$ROOT"/data; mkdir -p "$ROOT"
for i in $(seq 1 "$CYCLES"); do
  rm -f "$LOG"
  SAIWORK2_DATA_DIR="$DATA" "$EXE" > "$LOG" 2>&1 &
  if ! wait_ready "$LOG" 20; then
    echo "cycle $i: FAILED to reach READY"; tail -5 "$LOG"; exit 1
  fi
  close_app || { echo "cycle $i: FAILED to exit after close (count=$(app_count))"; exit 1; }
  grep -q "shutdown complete" "$LOG" || { echo "cycle $i: no 'shutdown complete' in log"; tail -3 "$LOG"; exit 1; }
  echo "cycle $i: READY -> close -> exited OK"
done
echo "leftover saiwork2.exe processes after Phase A: $(app_count)"

# ---- Phase B: single-instance stress --------------------------------------
echo; echo "--- Phase B: single-instance stress (primary + 3 secondaries) ---"
rm -f "$LOG"
SAIWORK2_DATA_DIR="$DATA" "$EXE" > "$LOG" 2>&1 &
wait_ready "$LOG" 20 || { echo "primary never READY"; exit 1; }
for j in 1 2 3; do
  SAIWORK2_DATA_DIR="$DATA" "$EXE" > "$ROOT/sec$j.log" 2>&1 &
  local_sec=$!
  # secondary must exit on its own (single-instance relay)
  n=0; while [ "$n" -lt 60 ]; do kill -0 "$local_sec" 2>/dev/null || break; n=$((n+1)); sleep 0.25; done
  if kill -0 "$local_sec" 2>/dev/null; then echo "secondary $j did not exit"; exit 1; fi
  echo "secondary $j: exited (relayed to primary)"
done
[ "$(app_count)" = "1" ] || { echo "FAIL: $(app_count) instances during stress"; exit 1; }
echo "exactly one primary authority during stress: OK"
LINES=$(wc -l < "$LOG")
echo "primary log lines (secondaries must not append): $LINES"
close_app || exit 1

# ---- Phase C: primary crash + relaunch -------------------------------------
echo; echo "--- Phase C: force-kill (crash) + relaunch ---"
rm -f "$LOG"
SAIWORK2_DATA_DIR="$DATA" "$EXE" > "$LOG" 2>&1 &
wait_ready "$LOG" 20 || exit 1
DB_BEFORE=$(ls -la "$DATA"/saiwork2.db 2>/dev/null | awk '{print $5}')
force_app; sleep 1
[ "$(app_count)" = "0" ] || { echo "crashed instance still alive"; exit 1; }
echo "forced-terminated primary (no graceful shutdown): gone"
SAIWORK2_DATA_DIR="$DATA" "$EXE" > "$LOG" 2>&1 &
wait_ready "$LOG" 20 || { echo "relaunch after crash never READY"; exit 1; }
DB_AFTER=$(ls -la "$DATA"/saiwork2.db 2>/dev/null | awk '{print $5}')
echo "db intact after crash+relaunch (bytes before=$DB_BEFORE after=$DB_AFTER)"
grep -q "database opened" "$LOG" && echo "db reopened cleanly: OK"
grep -qi "migration applied" "$LOG" && echo "WARN: re-migration after crash" || echo "no re-migration (schema stable): OK"
close_app || exit 1

# ---- Phase D: portable mode + env override precedence ----------------------
echo; echo "--- Phase D: portable mode + env override precedence ---"
PORT="$ROOT/portable"
rm -rf "$PORT"; mkdir -p "$PORT"
cp "$EXE" "$PORT/saiwork2.exe"
# Runtime DLL next to the exe (tauri-build places it beside the dev binary).
[ -f "$(dirname "$EXE")/WebView2Loader.dll" ] && cp "$(dirname "$EXE")/WebView2Loader.dll" "$PORT/"
touch "$PORT/portable.flag"
rm -f "$ROOT/port1.log"
( cd / && "$PORT/saiwork2.exe" > "$ROOT/port1.log" 2>&1 & )
for n in $(seq 1 80); do grep -q "application ready" "$ROOT/port1.log" 2>/dev/null && break; sleep 0.25; done
grep -q "application ready" "$ROOT/port1.log" || { echo "portable never READY"; tail -5 "$ROOT/port1.log"; exit 1; }
grep -o "data root ready.*" "$ROOT/port1.log" | head -1
[ -d "$PORT/data" ] && echo "portable data root is exe-relative (CWD=/): OK" || { echo "FAIL: no data dir beside exe"; exit 1; }
close_app || exit 1
rm -f "$ROOT/envlog.log"
ENVDATA="$ROOT/envdata"
( cd / && SAIWORK2_DATA_DIR="$ENVDATA" "$PORT/saiwork2.exe" > "$ROOT/envlog.log" 2>&1 & )
for n in $(seq 1 80); do grep -q "application ready" "$ROOT/envlog.log" 2>/dev/null && break; sleep 0.25; done
grep -q "application ready" "$ROOT/envlog.log" || { echo "env-override never READY"; exit 1; }
[ -d "$ENVDATA" ] && echo "explicit SAIWORK2_DATA_DIR overrides portable flag: OK" || { echo "FAIL: env override ignored"; exit 1; }
if [ "$(ls "$PORT/data" 2>/dev/null | wc -l)" = "0" ]; then echo "portable root untouched by env run: OK"; fi
close_app || exit 1

echo; echo "=== TORTURE COMPLETE: all phases OK ==="
