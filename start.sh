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

    LOG_LEVEL=${LOG_LEVEL:-INFO}
    MAX_WORKERS=${MAX_WORKERS:-}
    REFRESH_INTERVAL=${REFRESH_INTERVAL:-}
    COMPRESS_TIME=${COMPRESS_TIME:-}
    # ACCESSLOG=${ACCESSLOG:-true}
    # ERRORLOG=${ERRORLOG:-true}

    chunks=("python3" "./src/ingestion/ingest.py")

    # [[ "$ACCESSLOG" == "true" ]] && chunks+=("--access-logfile" "-")
    # [[ "$ERRORLOG" == "true" ]] && chunks+=("--error-logfile" "-")
    [[ ! -z "$MAX_WORKERS" ]] && chunks+=("--max_workers" "$MAX_WORKERS")
    [[ ! -z "$REFRESH_INTERVAL" ]] && chunks+=("--refresh_interval" "$REFRESH_INTERVAL")
    [[ ! -z "$COMPRESS_TIME" ]] && chunks+=("--compress_time" "$COMPRESS_TIME")

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

