#!/bin/bash
# run_benchmark.sh
# Runs all scaling experiments for the Reddit wordcount project.
# Outputs a CSV with runtimes + per-run Spark logs.
#
# Before you run this:
#   - Set SPARK_HOME and SPARK_MASTER below
#   - Make sure analysis_job.py works on its own first
#   - Upload data subsets to HDFS (see paths below)
#   - chmod +x run_benchmark.sh

# notes:
#   1. Fill in the CONFIG section below with your cluster details
#   2. Make sure your PySpark job (analysis_job.py) works manually first
#   3. Ensure HDFS data subsets are prepared (see DATA SETUP section)
#   4. Make this script executable: chmod +x run_benchmark.sh


# -- Config --

SPARK_HOME="/path/to/spark"
SPARK_MASTER="spark://MASTER_HOSTNAME:7077"
SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
PYSPARK_JOB="src/analysis_job.py"

# These must match the Host entries in ~/.ssh/config
ALL_WORKERS=("group42-worker1" "group42-worker2" "group42-worker3")
NUM_TOTAL_WORKERS=${#ALL_WORKERS[@]}

# Full parquet path — we use --fraction to control how much data gets processed
PARQUET_PATH="hdfs:///data/reddit/parquet/reddit_cleaned"

# Fractions of the full dataset (adjust if your parquet size differs from ~10GB)
FRAC_2GB=0.2
FRAC_4GB=0.4
FRAC_5GB=0.5
FRAC_6GB=0.6

OUTPUT_BASE="hdfs:///user/ubuntu/benchmark_output"
NUM_RUNS=3  # number of times to repeat each config

# -- Output setup --

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="benchmark_results_${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"

LOG_FILE="${RESULTS_DIR}/benchmark.log"
CSV_FILE="${RESULTS_DIR}/results.csv"
echo "experiment,test,run,workers,dataset_size,executor_cores,executor_memory,runtime_seconds" > "$CSV_FILE"

# -- Functions --

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

start_workers() {
    local num_workers=$1
    if [ "$num_workers" -gt "$NUM_TOTAL_WORKERS" ]; then
        log "ERROR: asked for ${num_workers} workers but we only have ${NUM_TOTAL_WORKERS}"
        exit 1
    fi
    log "Starting ${num_workers} worker(s)..."
    for ((i=0; i<num_workers; i++)); do
        ssh "${ALL_WORKERS[$i]}" \
            "${SPARK_HOME}/sbin/start-worker.sh ${SPARK_MASTER}" >> "$LOG_FILE" 2>&1
        log "  -> ${ALL_WORKERS[$i]} up"
    done
    sleep 10  # let them register with master
}

stop_workers() {
    log "Stopping all workers..."
    for worker in "${ALL_WORKERS[@]}"; do
        ssh "${worker}" \
            "${SPARK_HOME}/sbin/stop-worker.sh" >> "$LOG_FILE" 2>&1 || true
    done
    sleep 5
}

# Runs spark-submit once and logs the wall-clock time
run_spark_job() {
    local experiment=$1
    local test_label=$2
    local run_num=$3
    local num_workers=$4
    local data_path=$5
    local cores=$6
    local memory=$7

    local output_path="${OUTPUT_BASE}/${experiment}_${test_label}_run${run_num}"
    local run_log="${RESULTS_DIR}/${experiment}_${test_label}_run${run_num}.log"

    log "--- ${experiment} | Test ${test_label} | Run ${run_num}/${NUM_RUNS} ---"
    log "    workers=${num_workers} cores=${cores} mem=${memory} data=${data_path}"

    # remove old output if we're re-running
    hdfs dfs -rm -r -f "$output_path" >> "$LOG_FILE" 2>&1 || true

    local start_time=$(date +%s.%N)

    $SPARK_SUBMIT \
        --master "$SPARK_MASTER" \
        --executor-cores "$cores" \
        --executor-memory "$memory" \
        --conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir="${RESULTS_DIR}/spark_logs" \
        "$PYSPARK_JOB" \
        --input "$PARQUET_PATH" \
        --output "$output_path" \
        --fraction "$data_path" \
        >> "$run_log" 2>&1

    local exit_code=$?
    local end_time=$(date +%s.%N)
    local runtime=$(echo "$end_time - $start_time" | bc)

    if [ $exit_code -eq 0 ]; then
        log "    done in ${runtime}s"
    else
        log "    FAILED (exit ${exit_code}) — see ${run_log}"
        runtime="FAILED"
    fi

    # figure out size label from the path name
    local size_label
    case "$data_path" in
        $FRAC_2GB) size_label="2GB" ;;
        $FRAC_4GB) size_label="4GB" ;;
        $FRAC_5GB) size_label="5GB" ;;
        $FRAC_6GB) size_label="6GB" ;;
        *)         size_label="unknown" ;;
    esac

    echo "${experiment},${test_label},${run_num},${num_workers},${size_label},${cores},${memory},${runtime}" >> "$CSV_FILE"
}

# Stops everything, starts the right number of workers, runs NUM_RUNS times
run_config() {
    local experiment=$1
    local test_label=$2
    local num_workers=$3
    local data_path=$4
    local cores=$5
    local memory=$6

    stop_workers
    start_workers "$num_workers"

    for ((run=1; run<=NUM_RUNS; run++)); do
        run_spark_job "$experiment" "$test_label" "$run" "$num_workers" "$data_path" "$cores" "$memory"
    done
}

# -- Run experiments --

log "=========================================="
log "Starting benchmark"
log "=========================================="
log "Output: ${RESULTS_DIR}"
log "Runs per config: ${NUM_RUNS}"

mkdir -p "${RESULTS_DIR}/spark_logs"

# 1) Horizontal strong. fixed 5GB, add workers
log ""
log "=== Horizontal strong scaling ==="
run_config "horizontal_strong" "A" 1 "$FRAC_5GB" 2 "2g"
run_config "horizontal_strong" "B" 2 "$FRAC_5GB" 2 "2g"
run_config "horizontal_strong" "C" 3 "$FRAC_5GB" 2 "2g"

# 2) Horizontal weak. 2GB/worker, scale data with workers
log ""
log "=== Horizontal weak scaling ==="
run_config "horizontal_weak" "A" 1 "$FRAC_2GB" 2 "2g"
run_config "horizontal_weak" "B" 2 "$FRAC_4GB" 2 "2g"
run_config "horizontal_weak" "C" 3 "$FRAC_6GB" 2 "2g"

# 3) Vertical strong. fixed 5GB + 3 workers, increase cores
log ""
log "=== Vertical strong scaling ==="
run_config "vertical_strong" "A" 3 "$FRAC_5GB" 1 "2g"
run_config "vertical_strong" "B" 3 "$FRAC_5GB" 2 "2g"

# 4) Vertical weak. 1 worker, scale cores and data together
log ""
log "=== Vertical weak scaling ==="
run_config "vertical_weak" "A" 1 "$FRAC_2GB" 1 "2g"
run_config "vertical_weak" "B" 1 "$FRAC_4GB" 2 "2g"

# -- Done --

log ""
log "=========================================="
log "Benchmark finished"
log "=========================================="
log "CSV:    ${CSV_FILE}"
log "Log:    ${LOG_FILE}"
log "Spark:  ${RESULTS_DIR}/spark_logs/"
log ""
log "Results:"
cat "$CSV_FILE" | tee -a "$LOG_FILE"

stop_workers

log ""
log "Pass ${CSV_FILE} to the reporter for plotting."