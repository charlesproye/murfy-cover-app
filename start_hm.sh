#!/bin/bash

set -Eeuo pipefail

trap cleanup SIGINT SIGTERM ERR EXIT

parse_params() {
  # default values of variables set from params
  dotenv=0
  migrate=0
  init=0
  param=''

  while :; do
    case "${1-}" in
    # -h | --help) usage ;;
    -v | --verbose) set -x ;;
    -e | --dotenv) dotenv=1 ;;
    # example named parameter
    # -p | --param)
    #   param="${2-}"
    #   shift
    #   ;;
    -?*) die "Unknown option: $1" ;;
    *) break ;;
    esac
    shift
  done

  args=("$@")
}

main() {
    if  [[ $dotenv > 0 ]]; then
      set -a; source .env; set +a;
    fi

    # Ajoutez le répertoire `src` au PYTHONPATH
    export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"

    # Set memory-related environment variables if not already set
    export PYTHONMALLOC=${PYTHONMALLOC:-debug}
    export PYTHONFAULTHANDLER=${PYTHONFAULTHANDLER:-1}
    export PYTHONASYNCIODEBUG=${PYTHONASYNCIODEBUG:-0}
    export PYTHONOPTIMIZE=${PYTHONOPTIMIZE:-2}

    LOG_LEVEL=${LOG_LEVEL:-INFO}
    MAX_WORKERS=${MAX_WORKERS:-4}  # Default to fewer workers
    REFRESH_INTERVAL=${REFRESH_INTERVAL:-}
    COMPRESS_INTERVAL=${COMPRESS_INTERVAL:-}
    COMPRESS_THREADED=${COMPRESS_THREADED:-1}  # Default to threaded mode
    BATCH_SIZE=${BATCH_SIZE:-25}  # Default to smaller batch size

    msg "Starting High Mobility ingestion with memory optimization"
    msg "Memory optimization settings: PYTHONMALLOC=$PYTHONMALLOC, PYTHONOPTIMIZE=$PYTHONOPTIMIZE"
    msg "Max workers: $MAX_WORKERS, Batch size: $BATCH_SIZE"

    # Configure Python command with memory-optimized settings
    chunks=("python3" "-B")  # -B prevents writing bytecode

    # Set command arguments
    chunks+=("./src/ingestion/high_mobility")
    chunks+=("--max_workers" "$MAX_WORKERS")
    chunks+=("--batch_size" "$BATCH_SIZE")

    [[ ! -z "$REFRESH_INTERVAL" ]] && chunks+=("--refresh_interval" "$REFRESH_INTERVAL")
    [[ ! -z "$COMPRESS_INTERVAL" ]] && chunks+=("--compress_interval" "$COMPRESS_INTERVAL")
    [[ "$COMPRESS_THREADED" == "1" ]] && chunks+=("--compress_threaded")

    for e in "${chunks[@]}"
    do
        cmd=${cmd:+$cmd }$e
    done

    msg "Exec: $cmd"
    
    # Display memory usage before starting
    free -m
    msg "Starting High Mobility ingestion process"
    
    # Execute the command
    exec $cmd
}

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  # script cleanup here
}

msg() {
  echo >&2 -e "${1-}"
}

die() {
  local msg=$1
  local code=${2-1} # default exit status 1
  msg "$msg"
  exit "$code"
}


parse_params "$@"
main
