import subprocess
import os
import multiprocessing
import queue
import csv
import argparse
import pandas as pd

def run_command(cmd):
    """Run a shell command and return its output."""
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            print(f"Error running command {' '.join(cmd)}: {result.stderr}")
            return None
    except Exception as e:
        print(f"Exception running command {' '.join(cmd)}: {str(e)}")
        return None

def process_files(dir_queue, csv_path):
    """Processes directories from the queue and writes results to CSV in real-time."""
    while True:
        try:
            # Get directory from the queue with a timeout
            directory = dir_queue.get(timeout=1)
            if directory:
                output = run_command(['bash', 'md5gen.sh', directory])
                if output:
                    # Split and format the output
                    results = [line.split(',') for line in output.splitlines() if line]

                    # Write results to CSV
                    with open(csv_path, mode='a', newline='') as file:
                        csv_writer = csv.writer(file)
                        csv_writer.writerows(results)

                dir_queue.task_done()  # Mark the task as done
        except queue.Empty:
            break

def initialize_csv(csv_path):
    """Initializes the CSV file with the header."""
    with open(csv_path, mode='w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(['Path', 'Size', 'MD5', 'symbolic_link'])  # Write header

def get_file_info_parallel(path, csv_path, num_processes=None):
    if num_processes is None:
        num_processes = 4

    dir_queue = multiprocessing.JoinableQueue()

    # Initialize the CSV file
    initialize_csv(csv_path)

    # Create and start worker processes
    processes = []
    for _ in range(num_processes):
        p = multiprocessing.Process(target=process_files, args=(dir_queue, csv_path))
        p.start()
        processes.append(p)

    # Put directories in the queue
    for root, dirs, files in os.walk(path):
        dir_queue.put(root)

    # Wait for all tasks to be done
    dir_queue.join()

    # Terminate the processes
    for _ in processes:
        dir_queue.put(None)  # Sending None to terminate processes

    # Wait for all processes to finish
    for p in processes:
        p.join()

    # Sort CSV after all results are written
    read_and_sort_csv(csv_path, csv_path)

def read_and_sort_csv(input_file, output_file):
    """Reads a CSV file, sorts it by the Path column, and writes it back."""
    df = pd.read_csv(input_file)
    sorted_df = df.sort_values(by='Path')
    sorted_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    # Command-line argument parsing
    parser = argparse.ArgumentParser(description="Process files in a directory and calculate MD5 checksums.")
    parser.add_argument("-d", "--directory", type=str, help="Top-level directory to start processing", dest='top_dir')
    parser.add_argument("-p", "--process", type=int, default=4, help="Number of threads for parallel processing", dest='process')
    parser.add_argument("-csv", "--csvpath", type=str, default='dump.csv', help="CSV File to dump the data", dest='csv_dump')
    args = parser.parse_args()

    top_directory = args.top_dir
    num_threads = args.process
    csv_path = args.csv_dump

    get_file_info_parallel(top_directory, csv_path, num_threads)
