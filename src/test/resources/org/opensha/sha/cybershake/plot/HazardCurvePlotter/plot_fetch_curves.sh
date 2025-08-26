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

FETCHDB_DIR="src/test/resources/org/opensha/sha/cybershake/plot/HazardCurvePlotter/fetchdb"

# 🏃 Process in parallel
for SITE in "${!RUNS[@]}"; do
  RUN=${RUNS[$SITE]}

  OUTDIR="$FETCHDB_DIR/$SITE"
  mkdir -p "$OUTDIR"

  (
    echo "▶️ Running for $SITE (RUN=$RUN)..."
    START=$(date +%s)

    java -cp build/libs/opensha-cybershake-all.jar org.opensha.sha.cybershake.plot.HazardCurvePlotter \
      --output-dir "$OUTDIR" \
      --run-id $RUN \
	  --component RotD50 \
	  --period 2,3,5,10 \
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

