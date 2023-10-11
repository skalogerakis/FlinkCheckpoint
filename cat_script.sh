#!/bin/bash

# Check for the correct number of arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <input_files> <output_file>"
    exit 1
fi

input_files="$1"    # Comma-separated list of input files
output_file="$2"    # Output file

# Split the comma-separated input files into an array
IFS=',' read -ra files <<< "$input_files"

# Use cat to concatenate the contents of input files into the output file
cat "${files[@]}" > "$output_file"

# Check if the cat command was successful
if [ "$?" -eq 0 ]; then
    echo "Concatenation complete. Output written to $output_file"
else
    echo "Concatenation failed."
fi
