#!/bin/bash

set -euo pipefail

# --- CONFIGURATION VARIABLES ---

IMAGE_NAME="ghcr.io/scalar-labs/scalardb-data-loader:4.0.0-SNAPSHOT"
DATABASE_ROOT_PATH="database"
POSTGRES_SETUP_SCRIPT="./db_setup.sh"
POSTGRES_CONTAINER="postgres-db"
PYTHON_SCRIPT_PATH="scripts/import-data-generator.py"
PYTHON_ARGUMENTS="-s 1MB -o output.csv database/schema.json test.all_columns"
PROPERTIES_PATH="./database/scalardb.properties"
INPUT_SOURCE_FILE="./output.csv"
CONTAINER_INPUT_FILE="/app/output.csv"
CONTAINER_PROPERTIES_PATH="/app/scalardb.properties"
LOG_DIR_HOST="./performance-logs"
CONTAINER_LOG_DIR="/app/logs"
CONTAINER_LOG_DIR_PATH="/app/logs/"
MEMORY_CONFIGS=("1g" "2g")
CPU_CONFIGS=("1" "2")

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

log_step "Running Python script to generate input data..."
python3 "$PYTHON_SCRIPT_PATH" $PYTHON_ARGUMENTS
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
             --data-chunk-queue-size 100 \
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
