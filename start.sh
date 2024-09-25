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
    # To run the test before having a docker container for this part 
    # python3 src/main.py

    # Ajoutez le répertoire `src` au PYTHONPATH
    export PYTHONPATH="${PYTHONPATH:-}:$(pwd)"

    LOG_LEVEL=${LOG_LEVEL:-INFO}
    MAX_WORKERS=${MAX_WORKERS:-}
    REFRESH_INTERVAL=${REFRESH_INTERVAL:-}
    COMPRESS_INTERVAL=${COMPRESS_INTERVAL:-}
    COMPRESS_THREADED=${COMPRESS_THREADED:-}
    # ACCESSLOG=${ACCESSLOG:-true}
    # ERRORLOG=${ERRORLOG:-true}

    chunks=("python3" "-m" "src.ingestion.high_mobility")

    # [[ "$ACCESSLOG" == "true" ]] && chunks+=("--access-logfile" "-")
    # [[ "$ERRORLOG" == "true" ]] && chunks+=("--error-logfile" "-")
    [[ ! -z "$MAX_WORKERS" ]] && chunks+=("--max_workers" "$MAX_WORKERS")
    [[ ! -z "$REFRESH_INTERVAL" ]] && chunks+=("--refresh_interval" "$REFRESH_INTERVAL")
    [[ ! -z "$COMPRESS_INTERVAL" ]] && chunks+=("--compress_interval" "$COMPRESS_INTERVAL")
    [[ ! -z "$COMPRESS_THREADED" ]] && chunks+=("--compress_threaded")

    for e in "${chunks[@]}"
    do
        cmd=${cmd:+$cmd }$e
    done

    msg "Exec: $cmd"
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

