#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob  # ensures empty file patterns don't break the loop

show_help() {
  cat <<EOF
Usage: $0 <output_dir> <event_set_id>

Arguments:
  output_dir     Directory where site-specific subdirectories will be created
				 and generated hazard curve plots will be written in.
                 If relative, it's resolved against the directory where this
                 script was executed. Absolute paths are used as-is.
  event_set_id   Identifier suffix for input event set files (without .csv).
                 Example:
                   - 'larger' → matches *_larger.csv
                   - 'smaller' → matches *_smaller.csv

Example:
  $0 results/hazard_plots larger
  $0 /absolute/path/to/plots smaller
EOF
  exit 1
}

# 📝 Validate inputs
if [[ $# -ne 2 ]]; then
  echo "❌ Error: Missing arguments"
  show_help
fi

# Capture the directory the script was executed from (absolute path)
INITIAL_DIR="$(pwd -P)"

# Resolve OUTPUT_ROOT properly
if [[ "$1" = /* ]]; then
  # absolute path → use directly
  OUTPUT_ROOT="$1"
else
  # relative path → resolve against initial dir
  OUTPUT_ROOT="$INITIAL_DIR/$1"
fi

EVENT_SET_ID=$2

# 🚀 Ensure we're in the correct directory (prefer fork, fallback to main repo)
if [ -d "$HOME/git/opensha-cybershake-fork" ]; then
  cd "$HOME/git/opensha-cybershake-fork"
elif [ -d "$HOME/git/opensha-cybershake" ]; then
  cd "$HOME/git/opensha-cybershake"
else
  # 🔍 Search upwards
  DIR=$(pwd)
  FOUND=0
  while [ "$DIR" != "/" ]; do
    if [ -d "$DIR/opensha-cybershake-fork" ]; then
      cd "$DIR/opensha-cybershake-fork"
      FOUND=1
      break
    elif [ -d "$DIR/opensha-cybershake" ]; then
      cd "$DIR/opensha-cybershake"
      FOUND=1
      break
    fi
    DIR=$(dirname "$DIR")
  done
  if [ $FOUND -eq 0 ]; then
    echo "❌ Could not find opensha-cybershake-fork directory"
    exit 1
  fi
fi

# 📦 Ensure JAR exists
if [ ! -f build/libs/opensha-cybershake-all.jar ]; then
  echo "⚒️ Building JAR with ./gradlew fatJar..."
  ./gradlew fatJar
fi

# 🗺️ Define RUNS mapping
declare -A RUNS=(
  [ALP]=9542
  [USC]=9306
  [SBSM]=9320
  [SVD]=9647
  [PDE]=9663
)

INPUT_DIR="src/test/resources/org/opensha/sha/cybershake/plot/HazardCurvePlotter"
FILES=($INPUT_DIR/*_"$EVENT_SET_ID".csv)

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "❌ No input CSV files found for event set '$EVENT_SET_ID' in $INPUT_DIR"
  exit 1
fi

# 🏃 Process files in parallel
for INPUT_CSV in "${FILES[@]}"; do
  BASENAME=$(basename "$INPUT_CSV")
  SITE="${BASENAME%%_*}"

  if [[ -z "$SITE" ]]; then
    echo "⚠️ Could not extract site from $BASENAME, skipping..."
    continue
  fi

  RUN="${RUNS[$SITE]:-}"
  if [[ -z "$RUN" ]]; then
    echo "⚠️ No RUN ID found for site $SITE, skipping..."
    continue
  fi

  OUTDIR="$OUTPUT_ROOT/$SITE"
  mkdir -p "$OUTDIR"

  (
    echo "▶️ Running for $SITE (RUN=$RUN)..."
    START=$(date +%s)

    java -cp build/libs/opensha-cybershake-all.jar org.opensha.sha.cybershake.plot.HazardCurvePlotter \
      --output-dir "$OUTDIR" \
      --run-id "$RUN" \
      --component RotD50 \
      --period 2,3,5,10 \
      --rv-probs-csv "$INPUT_CSV" \
      --plot-chars-file src/main/resources/org/opensha/sha/cybershake/conf/robPlot.xml \
      --type TXT,PDF

    END=$(date +%s)
    DURATION=$((END - START))

    if [ $DURATION -lt 60 ]; then
      echo "✅ [$SITE] Took $DURATION seconds"
    else
      MINS=$((DURATION / 60))
      SECS=$((DURATION % 60))
      echo "✅ [$SITE] Took $MINS mins and $SECS seconds"
    fi
  ) &
done

wait
echo "🎉 All jobs finished!"
