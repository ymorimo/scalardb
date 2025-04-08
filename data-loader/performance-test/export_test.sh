#!/bin/bash

set -e

# --- CONFIGURATION VARIABLES ---

IMAGE_NAME="ghcr.io/scalar-labs/scalardb-data-loader:4.0.0-SNAPSHOT"
CONTAINER_BASE_NAME="data-loader-cli"
ROOT_PERFORMANCE_TEST_FOLDER="data-loader/performance-test"
DATABASE_ROOT_PATH="database"
POSTGRES_SETUP_SCRIPT="./db_setup.sh"
POSTGRES_CONTAINER="postgres-db"
SCHEMA_PATH="./database/schema.json"
PROPERTIES_PATH="./database/scalardb.properties"
DATA_POPULATOR_PATH="datapopulator"
POPULATE_DATA_COUNT=100000
CONTAINER_PROPERTIES_PATH="/app/scalardb.properties"
OUTPUT_DIR_HOST="./output-dir"
CONTAINER_OUTPUT_DIR="/app/output-dir"
CONTAINER_OUTPUT_DIR_PATH="/app/output-dir/"

MEMORY_CONFIGS=("1g" "2g")
CPU_CONFIGS=("1" "2")

# --- STEP 1: Build Docker image for data loader CLI ---
./gradlew data-loader:cli:docker

# --- STEP 2: Switch to performance test folder ---
cd "$ROOT_PERFORMANCE_TEST_FOLDER"
mkdir -p output-dir
chmod 777 output-dir

# --- STEP 3: start DB and load schema
echo "▶ Starting PostgreSQL Docker container..."
(cd "$DATABASE_ROOT_PATH" && bash "$POSTGRES_SETUP_SCRIPT")
echo "✅ PostgreSQL container is running."

# --- STEP 4: populate database
cd "$DATA_POPULATOR_PATH"
./gradlew run --args="scalardb.properties $POPULATE_DATA_COUNT"
cd ..

echo "✅ Data population completed"

for mem in "${MEMORY_CONFIGS[@]}"; do
  for cpu in "${CPU_CONFIGS[@]}"; do

    CONTAINER_NAME="import-mem${mem}-cpu${cpu}"
    echo "▶ Running Docker container: $CONTAINER_NAME with --memory=$mem --cpus=$cpu"

    docker run --rm \
      --name "$CONTAINER_NAME" \
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
             --max-threads 8

    echo "✅ Finished: $CONTAINER_NAME"
    echo "----------------------------------------"
  done
done

echo "🧹 Cleaning up PostgreSQL container..."
docker kill "$POSTGRES_CONTAINER" || true
docker rm -v "$POSTGRES_CONTAINER" || true
echo "🗑️ Removed postgres container"

# Optional: Keep or remove output directory
rm -rf output-dir

echo "🎉 All runs completed successfully."
