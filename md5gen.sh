#!/bin/bash

# Check if a directory is provided as an argument
if [ $# -ne 1 ]; then
    echo "Usage: $0 <directory>"
    exit 1
fi

directory="$1"

# Function to handle files and symbolic links
process_file() {
    local file="$1"

    if [ -L "$file" ]; then
        # Symbolic link: resolve the target
        target=$(readlink -f "$file")

        if [ -f "$target" ]; then
            # Target is a regular file
            size=$(stat --format="%s" "$target")
            md5=$(md5sum "$target" | awk '{print $1}')
            echo "$target,$size,$md5,$file"
        elif [ -d "$target" ]; then
            # Target is a directory: process its files
            find "$target" -maxdepth 1 -type f -exec bash -c 'process_file "$0"' {} \;
        else
            echo "Target of symlink $file does not exist."
        fi
    elif [ -f "$file" ]; then
        # Regular file: print size and md5sum
        size=$(stat --format="%s" "$file")
        md5=$(md5sum "$file" | awk '{print $1}')
        echo "$file,$size,$md5,-"
    fi
}

# Export the function to be used with find
export -f process_file

# Start processing the directory
#echo "Path,Size,MD5,symbolic_link"  # CSV Header
find "$directory" -maxdepth 1 -type f -exec bash -c 'process_file "$0"' {} \;
find "$directory" -maxdepth 1 -type l -exec bash -c 'process_file "$0"' {} \;
