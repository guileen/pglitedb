#!/bin/bash

# Keep only the latest KEEP_COUNT markdown files in spec/ directory
# Sort by modification time (newest first) and remove older files

# Number of files to keep (modifiable)
KEEP_COUNT=40

echo "Cleaning up spec/ directory..."
cd /Users/gl/agentwork/pglitedb

# Count total markdown files in spec directory
total_files=$(ls -1 spec/*.md 2>/dev/null | wc -l | tr -d ' ')

if [ "$total_files" -le "$KEEP_COUNT" ]; then
    echo "No files to remove. spec/ directory has $total_files files ($KEEP_COUNT or fewer)."
    exit 0
fi

# Get list of files to remove (older than the latest KEEP_COUNT)
files_to_remove=$(ls -1t spec/*.md | tail -n +$((KEEP_COUNT + 1)))

if [ -n "$files_to_remove" ]; then
    echo "Removing $(echo "$files_to_remove" | wc -l | tr -d ' ') old files:"
    echo "$files_to_remove"
    rm $files_to_remove
    echo "Cleanup completed."
else
    echo "No files to remove."
fi