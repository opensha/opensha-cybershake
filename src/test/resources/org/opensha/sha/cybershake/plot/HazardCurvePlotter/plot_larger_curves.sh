#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob  # ensures empty file patterns don't break the loop

# 🚀 Ensure we're in the correct directory
if [ -d "$HOME/git/opensha-cybershake-fork" ]; then
  cd "$HOME/git/opensha-cybershake-fork"
else
  # 🔍 Search upwards for opensha-cybershake-fork
  DIR=$(pwd)
  FOUND=0
  while [ "$DIR" != "/" ]; do
    if [ -d "$DIR/opensha-cybershake-fork" ]; then
      cd "$DIR/opensha-cybershake-fork"
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
declare -A RUNS

RUNS[ALP]="9542"
RUNS[USC]="9306"
RUNS[SBSM]="9320"
RUNS[SVD]="9647"
RUNS[PDE]="9663"

INPUT_DIR="src/test/resources/org/opensha/sha/cybershake/plot/HazardCurvePlotter"
FILES=($INPUT_DIR/*_larger.csv)

# 🏃 Process files in parallel
for INPUT_CSV in "${FILES[@]}"; do
  BASENAME=$(basename "$INPUT_CSV")
  SITE="${BASENAME%%_*}"

  if [[ -z "$SITE" ]]; then
    echo "⚠️ Could not extract site from $BASENAME, skipping..."
    continue
  fi

  if [[ ${RUNS[$SITE]+_} ]]; then
    RUN=${RUNS[$SITE]}
  else
    echo "⚠️ No RUN ID found for site $SITE, skipping..."
    continue
  fi

  OUTDIR="$INPUT_DIR/modprob/larger/$SITE"
  mkdir -p "$OUTDIR"

  (
    echo "▶️ Running for $SITE (RUN=$RUN)..."
    START=$(date +%s)

    java -cp build/libs/opensha-cybershake-all.jar org.opensha.sha.cybershake.plot.HazardCurvePlotter \
      --output-dir "$OUTDIR" \
      --run-id $RUN \
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

