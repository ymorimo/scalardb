#!/bin/bash

set -e

# === FUNCTIONS ===

info()    { echo -e "\033[1;34m▶ $1\033[0m"; }
success() { echo -e "\033[1;32m✅ $1\033[0m"; }
warn()    { echo -e "\033[1;33m⚠️ $1\033[0m"; }
error()   { echo -e "\033[1;31m❌ $1\033[0m"; }

cleanup() {
  info "Cleaning up PostgreSQL container..."
  docker kill "$POSTGRES_CONTAINER" &>/dev/null || true
  docker rm -v "$POSTGRES_CONTAINER" &>/dev/null || true
  success "Removed PostgreSQL container"
}
trap cleanup EXIT

run_container() {
  local mem=$1
  local cpu=$2
  local container_name="import-mem${mem}-cpu${cpu}"

  info "Running Docker container: $container_name with --memory=$mem --cpus=$cpu"

  docker run --rm \
    --name "$container_name" \
    --network my-network \
    --memory="$mem" \
    --cpus="$cpu" \
    -v "$PWD/$PROPERTIES_PATH":"$CONTAINER_PROPERTIES_PATH" \
    -v "$PWD/$OUTPUT_DIR_HOST":"$CONTAINER_OUTPUT_DIR" \
    "$IMAGE_NAME" \
    export --config "$CONTAINER_PROPERTIES_PATH" \
           --namespace test \
           --table all_columns \
           --format csv \
           --output-dir "$CONTAINER_OUTPUT_DIR_PATH" \
           --max-threads 8 \
           --data-chunk-size 10

  success "Finished: $container_name"
  echo "----------------------------------------"
}

# === PREREQUISITE CHECK ===

command -v docker >/dev/null 2>&1 || { error "Docker is not installed or not in PATH"; exit 1; }

# === CONFIGURATION VARIABLES ===

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_TAG="4.0.0-SNAPSHOT"
IMAGE_NAME="ghcr.io/scalar-labs/scalardb-data-loader:${IMAGE_TAG}"
DATABASE_ROOT_PATH="$SCRIPT_DIR/database"
POSTGRES_SETUP_SCRIPT="$DATABASE_ROOT_PATH/db_setup.sh"
POSTGRES_CONTAINER="postgres-db"
SCHEMA_PATH="$DATABASE_ROOT_PATH/schema.json"
PROPERTIES_PATH="/database/scalardb.properties"
DATA_POPULATOR_PATH="$SCRIPT_DIR/datapopulator"
POPULATE_DATA_COUNT=5000
CONTAINER_PROPERTIES_PATH="/app/scalardb.properties"
OUTPUT_DIR_HOST="output-dir"
CONTAINER_OUTPUT_DIR="/app/output-dir"
CONTAINER_OUTPUT_DIR_PATH="/app/output-dir/"
MEMORY_CONFIGS=("2g" "2g")
CPU_CONFIGS=("1" "2")

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --memory=*)
      MEMORY_CONFIGS=(${1#*=})
      shift
      ;;
    --cpu=*)
      CPU_CONFIGS=(${1#*=})
      shift
      ;;
    --data-count=*)
      POPULATE_DATA_COUNT=${1#*=}
      shift
      ;;
    --image-tag=*)
      IMAGE_TAG=${1#*=}
      IMAGE_NAME="ghcr.io/scalar-labs/scalardb-data-loader:${IMAGE_TAG}"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--memory=mem1,mem2,...] [--cpu=cpu1,cpu2,...] [--data-count=count] [--image-tag=tag]"
      echo "Example: $0 --memory=1g,2g,4g --cpu=1,2,4 --data-count=10000 --image-tag=4.0.0-SNAPSHOT"
      exit 1
      ;;
  esac
done

# === SETUP OUTPUT DIR ===
mkdir -p "$OUTPUT_DIR_HOST"
chmod 777 "$OUTPUT_DIR_HOST"

# === START POSTGRES AND LOAD SCHEMA ===
info "Starting PostgreSQL Docker container..."
pushd "$DATABASE_ROOT_PATH" >/dev/null
bash "$POSTGRES_SETUP_SCRIPT"
popd >/dev/null
success "PostgreSQL container is running"

# === POPULATE DATABASE ===
info "Populating database with $POPULATE_DATA_COUNT records..."
pushd "$DATA_POPULATOR_PATH" >/dev/null
./gradlew runBulkInsert --args="scalardb.properties $POPULATE_DATA_COUNT"
popd >/dev/null
success "Data population completed"

# === RUN EXPORTS ===
for mem in "${MEMORY_CONFIGS[@]}"; do
  for cpu in "${CPU_CONFIGS[@]}"; do
    run_container "$mem" "$cpu"
  done
done

# === CLEANUP OUTPUT DIR ===
info "Cleaning up output directory..."
rm -rf "$OUTPUT_DIR_HOST"
success "All export runs completed successfully 🎉"
