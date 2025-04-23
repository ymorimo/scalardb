#!/bin/bash

set -euo pipefail

# --- CONFIGURATION VARIABLES ---

IMAGE_TAG="4.0.0-SNAPSHOT"
IMAGE_NAME="ghcr.io/scalar-labs/scalardb-data-loader:${IMAGE_TAG}"
DATABASE_ROOT_PATH="database"
POSTGRES_SETUP_SCRIPT="./db_setup.sh"
POSTGRES_CONTAINER="postgres-db"
PYTHON_SCRIPT_PATH="scripts/import-data-generator.py"
DATA_SIZE="1MB"
PYTHON_ARGUMENTS="-s $DATA_SIZE -o /app/generated-imports.csv /app/database/schema.json test.all_columns"
PROPERTIES_PATH="./database/scalardb.properties"
INPUT_SOURCE_FILE="./generated-imports.csv"
CONTAINER_INPUT_FILE="/app/generated-imports.csv"
CONTAINER_PROPERTIES_PATH="/app/scalardb.properties"
LOG_DIR_HOST="./performance-logs"
CONTAINER_LOG_DIR="/app/logs"
CONTAINER_LOG_DIR_PATH="/app/logs/"
MEMORY_CONFIGS=("1g" "2g")
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
    --data-size=*)
      DATA_SIZE=${1#*=}
      shift
      ;;
    --image-tag=*)
      IMAGE_TAG=${1#*=}
      IMAGE_NAME="ghcr.io/scalar-labs/scalardb-data-loader:${IMAGE_TAG}"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--memory=mem1,mem2,...] [--cpu=cpu1,cpu2,...] [--data-size=size] [--image-tag=tag]"
      echo "Example: $0 --memory=1g,2g,4g --cpu=1,2,4 --data-size=2MB --image-tag=4.0.0-SNAPSHOT"
      exit 1
      ;;
  esac
done

# --- FUNCTIONS ---

log_step() {
  echo -e "\n▶ \033[1;34m$1\033[0m"
}

cleanup_postgres() {
  docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
}

cleanup_files() {
  rm -rf "$LOG_DIR_HOST" "$INPUT_SOURCE_FILE"
}

ensure_docker_network() {
  if ! docker network inspect my-network >/dev/null 2>&1; then
    log_step "Creating Docker network 'my-network'"
    docker network create my-network
  fi
}

trap cleanup_postgres EXIT

# --- PREPARATION ---

mkdir -p "$LOG_DIR_HOST"
chmod 777 "$LOG_DIR_HOST"

if [[ ! -f "$PYTHON_SCRIPT_PATH" ]]; then
  echo "❌ Python script not found at $PYTHON_SCRIPT_PATH"
  exit 1
fi

log_step "Running Python script to generate input data using Docker..."
docker run --rm -it \
  -v "$PWD":/app \
  python:alpine \
  python3 /app/"$PYTHON_SCRIPT_PATH" $PYTHON_ARGUMENTS
echo "✅ Input file generated."

ensure_docker_network

# --- MAIN EXECUTION LOOP ---

for mem in "${MEMORY_CONFIGS[@]}"; do
  for cpu in "${CPU_CONFIGS[@]}"; do

    log_step "Starting PostgreSQL Docker container..."
    (cd "$DATABASE_ROOT_PATH" && bash "$POSTGRES_SETUP_SCRIPT")
    echo "✅ PostgreSQL container is running."

    CONTAINER_NAME="import-mem${mem}-cpu${cpu}"
    log_step "Running Docker container: $CONTAINER_NAME with --memory=$mem --cpus=$cpu"

    docker run --rm \
      --name "$CONTAINER_NAME" \
      --network my-network \
      --memory="$mem" \
      --cpus="$cpu" \
      -v "$PWD/$INPUT_SOURCE_FILE":"$CONTAINER_INPUT_FILE" \
      -v "$PWD/$PROPERTIES_PATH":"$CONTAINER_PROPERTIES_PATH" \
      -v "$LOG_DIR_HOST":"$CONTAINER_LOG_DIR" \
      "$IMAGE_NAME" \
      import --config "$CONTAINER_PROPERTIES_PATH" \
             --file "$CONTAINER_INPUT_FILE" \
             --namespace test \
             --table all_columns \
             --format csv \
             --import-mode insert \
             --mode transaction \
             --transaction-size 10 \
             --data-chunk-size 50 \
             --max-threads 16 \
             --log-dir "$CONTAINER_LOG_DIR_PATH"

    echo "✅ Finished: $CONTAINER_NAME"
    echo "----------------------------------------"

    log_step "Cleaning up PostgreSQL container..."
    cleanup_postgres
  done
done

# --- CLEANUP ---

log_step "Cleaning up generated files..."
cleanup_files

echo -e "\n🎉 \033[1;32mAll import runs completed successfully.\033[0m"
