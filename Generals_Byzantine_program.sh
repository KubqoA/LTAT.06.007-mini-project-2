#!/usr/bin/env sh

if [ -z "$1" ]; then
  echo "Usage: $0 [number_of_processes]" >&2
  exit 1
fi

python3 main.py "$1"
