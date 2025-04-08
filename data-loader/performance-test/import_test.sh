#!/bin/bash

set -e

# --- CONFIGURATION VARIABLES ---

IMAGE_NAME="ghcr.io/scalar-labs/scalardb-data-loader:4.0.0-SNAPSHOT"
CONTAINER_BASE_NAME="data-loader-cli"
ROOT_PERFORMANCE_TEST_FOLDER="data-loader/performance-test"
DATABASE_ROOT_PATH="database"
POSTGRES_SETUP_SCRIPT="./db_setup.sh"
POSTGRES_CONTAINER="postgres-db"
PYTHON_SCRIPT_PATH="scripts/import-data-generator.py"
PYTHON_ARGUMENTS="-s 100MB -o output.csv database/schema.json test.all_columns"
SCHEMA_PATH="./database/schema.json"
PROPERTIES_PATH="./database/scalardb.properties"
INPUT_SOURCE_FILE="./output.csv"
CONTAINER_INPUT_FILE="/app/output.csv"
CONTAINER_PROPERTIES_PATH="/app/scalardb.properties"
LOG_DIR_HOST="./performance-logs"
CONTAINER_LOG_DIR="/app/logs"
CONTAINER_LOG_DIR_PATH="/app/logs/"
MEMORY_CONFIGS=("1g" "2g")
CPU_CONFIGS=("1" "2")

# --- STEP 1: Build Docker image for data loader CLI ---

./gradlew data-loader:cli:docker

# --- STEP 2: Switch to performance test folder ---

cd "$ROOT_PERFORMANCE_TEST_FOLDER"
mkdir -p performance-logs
chmod 777 performance-logs

# --- STEP 2: Generate input data using Python ---

echo "▶ Running Python script to generate input data..."
python3 "$PYTHON_SCRIPT_PATH" $PYTHON_ARGUMENTS
echo "✅ Input file generated."



for mem in "${MEMORY_CONFIGS[@]}"; do
  for cpu in "${CPU_CONFIGS[@]}"; do

    # --- STEP 3: start DB and load schema
    # Start PostgreSQL container
    echo "▶ Starting PostgreSQL Docker container..."
    (cd "$DATABASE_ROOT_PATH" && bash "$POSTGRES_SETUP_SCRIPT")
    echo "✅ PostgreSQL container is running."

    # --- STEP 4: Run the docker image
    CONTAINER_NAME="import-mem${mem}-cpu${cpu}"
    echo "▶ Running Docker container: $CONTAINER_NAME with --memory=$mem --cpus=$cpu"

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

    # Kill and remove the PostgreSQL container
    echo "🧹 Cleaning up PostgreSQL container..."
    docker kill "$POSTGRES_CONTAINER" || true
    docker rm -v "$POSTGRES_CONTAINER" || true
    echo "🗑️ Removed postgres container"
  done
done

# Removes generated input and output files (comment if files are required for final output check)
rm -rf performance-logs
rm output.csv

echo "🎉 All import runs completed."
