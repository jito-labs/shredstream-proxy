#!/usr/bin/env bash

REMOTE_DIR="~/"

# Function to display the script's usage
usage() {
  echo "The script syncs the script directory with a remote directory"
  echo "Usage: [--remote_dir </path/to/remote/dir>]"
  echo "If no remote directory is specified, we sync with the default remote directory: $REMOTE_DIR"
  echo "Usage: need to run as ./s [--remote_dir </path/to/remote/dir>] ..."
  echo "  --remote_dir       : Remote directory to sync the current directory to. Example --remote_dir /path/to/remote/dir"
}

parse_args() {
  # Loop through the remaining arguments
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --remote_dir)
        if [[ $# -gt 1 ]]; then
          REMOTE_DIR="$2" # Assign the value to IS_DEBUG
          shift         # Consume the value
        else
          echo "Error: Missing value for $1. Example usage: --remote_dir /path/to/remote/dir"
          exit 1
        fi
        ;;
      --help)
        # Display usage information
        usage
        exit 0
        ;;
      *)
        # Handle unknown arguments here if needed
        echo "Unknown argument: $1"
        usage
        exit 1
        ;;
    esac
    shift
  done
}

# Parse command-line arguments
parse_args "$@"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

if [ -f .env ]; then
  # Load Environment Variables
  export "$(cat .env | grep -v '#' | awk '/=/ {print $1}')"
else
  echo "Missing .env file"
  exit 0
fi

echo "Syncing $SCRIPT_DIR to host: $BUILD_SERVER:$REMOTE_DIR"
rsync -avh --delete --exclude target "$SCRIPT_DIR" "$BUILD_SERVER":"$REMOTE_DIR"
