import difflib
import sys
import time
import os
import glob
from datetime import datetime

def list_files(pattern):
    """List files in the current directory matching the given pattern."""
    return sorted(glob.glob(pattern))

def display_and_select_files(files, file_type):
    """Display a numbered list of files and prompt for selection."""
    if not files:
        print(f"No {file_type} files found in the current directory.")
        sys.exit(1)
    print(f"\nAvailable ({file_type}) files:")
    for i, file in enumerate(files, 1):
        print(f"{i}. {file}")
    
    while True:
        try:
            choice = int(input(f"\nSelect a ({file_type}) file (enter number 1-{len(files)}): "))
            if 1 <= choice <= len(files):
                return files[choice - 1]
            else:
                print(f"Please enter a number between 1 and {len(files)}.")
        except ValueError:
            print("Invalid input. Please enter a valid number.")

def read_file(file_path):
    """Read the content of a file and return it as a list of lines."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.readlines()
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file '{file_path}': {e}")
        sys.exit(1)

def compare_files(pre_validation_file, post_validation_file, output_file):
    """Compare two files and write the differences to an output file."""
    # Read the contents of both files
    pre_validation_lines = read_file(pre_validation_file)
    post_validation_lines = read_file(post_validation_file)
    
    # Generate the differences using difflib
    differ = difflib.Differ()
    diff = list(differ.compare(pre_validation_lines, post_validation_lines))
    
    # Write differences to the output file
    try:
        with open(output_file, 'w', encoding='utf-8') as file:
            file.write("=== Differences between Pre-Validation and Post-Validation Files ===\n\n")
            file.write(f"Pre-Validation File: {pre_validation_file}\n")
            file.write(f"Post-Validation File: {post_validation_file}\n\n")
            file.write("Legend:\n")
            file.write("  Lines prefixed with '- ' are in Pre-Validation File but not in Post-Validation File\n")
            file.write("  Lines prefixed with '+ ' are in Post-Validation File but not in Pre-Validation File\n")
            file.write("  Lines prefixed with '? ' indicate differences in lines\n")
            file.write("  Lines with no prefix are common to both files\n\n")
            
            # Write the actual differences
            for line in diff:
                file.write(line)
        
        print("Generating Report ....")
        time.sleep(3)
        print(f"\nDifferences written to {output_file}")
    except Exception as e:
        print(f"Error writing to output file: {e}")
        sys.exit(1)

def main():
    # Define file patterns
    pre_validation_pattern = "Nifi_Pre_Validation_Detailed_Report_*"
    post_validation_pattern = "Nifi_Post_Validation_Detailed_Report_*"
    
    # List files matching the patterns
    pre_validation_files = list_files(pre_validation_pattern)
    post_validation_files = list_files(post_validation_pattern)
    
    # Display and select Pre-Validation file
    pre_validation_file = display_and_select_files(pre_validation_files, "Pre-Validation")
    
    # Display and select Post-Validation file
    post_validation_file = display_and_select_files(post_validation_files, "Post-Validation")
    
    # Generate output file name based on current date (DDMMYYYY)
    current_date = datetime.now().strftime("%d%m%Y")
    output_file = f"Nifi_Sql_Execute_Validation_Report_{current_date}.txt"
    
    # Compare the files and generate output
    compare_files(pre_validation_file, post_validation_file, output_file)

if __name__ == "__main__":
    main()
