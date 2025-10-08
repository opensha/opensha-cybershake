#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob  # ensures empty file patterns don't break the loop

show_help() {
  cat <<EOF
Usage: $0 <output_dir> <fetch_dir> <modified_base_dir>

Arguments:
  output_dir          Directory where comparison plots will be written.
                      If relative, it's resolved against the directory where this
                      script was executed. Absolute paths are used as-is.
  fetch_dir           Directory where plot_fetch_curves output TXT files are located.
                      Should contain site subdirectories with TXT files.
  modified_base_dir   Base directory where plot_modified_curves output subdirectories
                      are located (90p, 100p, SA25p, SA100p, Exp1, Exp2).
                      Each subdirectory should contain site subdirectories with TXT files.

Description:
  This script creates comparison plots between fetched and all modified hazard curves.
  For each site and period, it finds the fetched TXT file and all modified variant
  TXT files (90p, 100p, SA25p, SA100p, Exp1, Exp2) and generates a single comparison
  plot using ComparisonCurvePlotter.

  When multiple files exist for the same site/period/variant, the script automatically
  selects the file with the latest date in its filename (format: YYYY_MM_DD).

  Input files can be either CSV or TXT format with X,Y columns (tab or comma separated).
  Lines beginning with '#' are treated as comments and ignored during parsing.

  Expected directory structure:
    fetch_dir/
      <site>/
        <site>_ERF36_Run<runid>_SA_<period>sec_RotD50_<date>.txt
    modified_base_dir/
      90p/
        <site>/
          <site>_ERF36_Run<runid>_SA_<period>sec_RotD50_<date>.txt
      100p/
        <site>/
          <site>_ERF36_Run<runid>_SA_<period>sec_RotD50_<date>.txt
      ...

Example:
  $0 results/comparison_plots results/fetch_plots results/modified_plots
  $0 /absolute/path/to/comparison /path/to/fetch /path/to/modified
EOF
  exit 1
}

# Function to select the latest file from an array based on date in filename
# Expected filename format: *_YYYY_MM_DD.txt
select_latest_file() {
  local -n files=$1  # nameref to the array

  if [[ ${#files[@]} -eq 0 ]]; then
    echo ""
    return
  fi

  if [[ ${#files[@]} -eq 1 ]]; then
    echo "${files[0]}"
    return
  fi

  # Multiple files found - select the one with the latest date
  local latest_file=""
  local latest_date=""

  for file in "${files[@]}"; do
    # Extract date from filename (format: YYYY_MM_DD before .txt)
    # Remove .txt extension and get last component
    local basename=$(basename "$file" .txt)
    # Extract the last part after the last underscore (should be date in format YYYY_MM_DD)
    local date_part="${basename##*_}"

    # Check if this looks like a date (format: YYYY_MM_DD)
    if [[ "$date_part" =~ ^[0-9]{4}_[0-9]{2}_[0-9]{2}$ ]]; then
      # Convert YYYY_MM_DD to YYYYMMDD for numerical comparison
      local date_numeric="${date_part//_/}"

      if [[ -z "$latest_date" ]] || [[ "$date_numeric" > "$latest_date" ]]; then
        latest_date="$date_numeric"
        latest_file="$file"
      fi
    fi
  done

  if [[ -n "$latest_file" ]]; then
    echo "$latest_file"
  else
    # Fallback: if no valid date found, just use the first file
    echo "${files[0]}"
  fi
}

# 📝 Validate inputs
if [[ $# -ne 3 ]]; then
  echo "❌ Error: Missing arguments"
  show_help
fi

# Capture the directory the script was executed from (absolute path)
INITIAL_DIR="$(pwd -P)"

# Resolve OUTPUT_ROOT properly
if [[ "$1" = /* ]]; then
  OUTPUT_ROOT="$1"
else
  OUTPUT_ROOT="$INITIAL_DIR/$1"
fi

# Resolve FETCH_DIR properly
if [[ "$2" = /* ]]; then
  FETCH_DIR="$2"
else
  FETCH_DIR="$INITIAL_DIR/$2"
fi

# Resolve MODIFIED_BASE_DIR properly
if [[ "$3" = /* ]]; then
  MODIFIED_BASE_DIR="$3"
else
  MODIFIED_BASE_DIR="$INITIAL_DIR/$3"
fi

# Validate input directories exist
if [[ ! -d "$FETCH_DIR" ]]; then
  echo "❌ Error: Fetch directory does not exist: $FETCH_DIR"
  exit 1
fi

if [[ ! -d "$MODIFIED_BASE_DIR" ]]; then
  echo "❌ Error: Modified base directory does not exist: $MODIFIED_BASE_DIR"
  exit 1
fi

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

# 🗺️ Define sites and periods
SITES=(ALP USC SBSM SVD PDE s036 s080 s145 s732 s776)
PERIODS=(2 3 5 10)

# Define modified curve variants
VARIANTS=(90p 100p SA25p SA100p Exp1 Exp2)

# Create output directory
mkdir -p "$OUTPUT_ROOT"

echo "🔍 Searching for matching TXT files..."
echo "   Fetch dir: $FETCH_DIR"
echo "   Modified base dir: $MODIFIED_BASE_DIR"
echo "   Output dir: $OUTPUT_ROOT"
echo ""

COMPARISON_COUNT=0
SKIP_COUNT=0

# 🏃 Process each site and period combination
for SITE in "${SITES[@]}"; do
  FETCH_SITE_DIR="$FETCH_DIR/$SITE"
  echo "Site $SITE"

  # Check if fetch site directory exists
  if [[ ! -d "$FETCH_SITE_DIR" ]]; then
    echo "⚠️ Skipping site $SITE: fetch directory not found"
    ((++SKIP_COUNT))
    continue
  fi

  # Process each period
  for PERIOD in "${PERIODS[@]}"; do
    # Find the fetch TXT file matching the pattern
    # Pattern: <SITE>_ERF36_Run*_SA_<PERIOD>sec_RotD50_*.txt
    FETCH_FILES=("$FETCH_SITE_DIR/${SITE}_ERF36_Run"*"_SA_${PERIOD}sec_RotD50_"*.txt)

    # Check if we found any fetch files
    if [[ ${#FETCH_FILES[@]} -eq 0 ]]; then
      echo "⚠️ Skipping $SITE (${PERIOD}sec): no fetch TXT found"
      ((SKIP_COUNT++))
      continue
    fi

    # Select the latest fetch file if multiple exist
    FETCH_TXT=$(select_latest_file FETCH_FILES)

    if [[ -z "$FETCH_TXT" ]]; then
      echo "⚠️ Skipping $SITE (${PERIOD}sec): could not determine fetch TXT file"
      ((SKIP_COUNT++))
      continue
    fi

    if [[ ${#FETCH_FILES[@]} -gt 1 ]]; then
      echo "ℹ️ Multiple fetch files found for $SITE (${PERIOD}sec), using latest: $(basename "$FETCH_TXT")"
    fi

    # Build list of TXT file paths and corresponding curve names for this site/period
    TXT_FILES="$FETCH_TXT"
    CURVE_NAMES="Fetched"

    FOUND_VARIANTS=0

    # Collect all available modified variants
    for VARIANT in "${VARIANTS[@]}"; do
      MODIFIED_SITE_DIR="$MODIFIED_BASE_DIR/$VARIANT/$SITE"

      if [[ ! -d "$MODIFIED_SITE_DIR" ]]; then
        continue
      fi

      # Find matching modified files
      MODIFIED_FILES=("$MODIFIED_SITE_DIR/${SITE}_ERF36_Run"*"_SA_${PERIOD}sec_RotD50_"*.txt)

      if [[ ${#MODIFIED_FILES[@]} -eq 0 ]]; then
        continue
      fi

      # Select the latest modified file
      MODIFIED_TXT=$(select_latest_file MODIFIED_FILES)

      if [[ -n "$MODIFIED_TXT" ]]; then
        if [[ ${#MODIFIED_FILES[@]} -gt 1 ]]; then
          echo "ℹ️ Multiple $VARIANT files found for $SITE (${PERIOD}sec), using latest: $(basename "$MODIFIED_TXT")"
        fi

        TXT_FILES="$TXT_FILES,$MODIFIED_TXT"
        CURVE_NAMES="$CURVE_NAMES,$VARIANT"
        FOUND_VARIANTS=$((FOUND_VARIANTS + 1))
      fi
    done

    # Only create comparison if we have at least one modified variant
    if [[ $FOUND_VARIANTS -eq 0 ]]; then
      echo "⚠️ Skipping $SITE (${PERIOD}sec): no modified variants found"
      ((SKIP_COUNT++))
      continue
    fi

    # Generate comparison plot
    OUTPUT_PREFIX="$OUTPUT_ROOT/comparison_${SITE}_${PERIOD}sec"
    TITLE="${SITE} ${PERIOD}sec Hazard Curve Comparison"

    (
      echo "▶️ Creating comparison plot for $SITE (${PERIOD}sec) with $FOUND_VARIANTS variants..."
      START=$(date +%s)

      java -cp build/libs/opensha-cybershake-all.jar \
        org.opensha.sha.cybershake.plot.ComparisonCurvePlotter \
        --csv-files "$TXT_FILES" \
        --names "$CURVE_NAMES" \
        --title "$TITLE" \
        --x-axis "${PERIOD}sec SA (g)" \
        --y-axis "Annual Probability of Exceedance" \
        --output "$OUTPUT_PREFIX" \
        --type PDF,PNG \
        --y-log \
		--x-range 0,2 \
        --skip-lines 2

      END=$(date +%s)
      DURATION=$((END - START))

      if [ $DURATION -lt 60 ]; then
        echo "✅ [$SITE ${PERIOD}sec] Took $DURATION seconds"
      else
        MINS=$((DURATION / 60))
        SECS=$((DURATION % 60))
        echo "✅ [$SITE ${PERIOD}sec] Took $MINS mins and $SECS seconds"
      fi
    ) &

    ((++COMPARISON_COUNT))
  done
done

wait
echo ""
echo "🎉 All comparison jobs finished!"
echo "   Created: $COMPARISON_COUNT comparison plots"
echo "   Skipped: $SKIP_COUNT missing file combinations"
