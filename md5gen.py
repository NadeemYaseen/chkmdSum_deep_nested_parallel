import subprocess
import os
import multiprocessing
import queue
import pandas as pd
import csv


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


def process_files(dir_queue, result_queue):
    """Processes dirs from the queue and puts results in the result queue."""
    while True:
        try:
            # Get directory from the queue with a timeout
            dir = dir_queue.get(timeout=1)
            # Run the command
            if dir:
                output = run_command(['bash', 'md5gen.sh', dir])
                result_queue.put(output)  # Place result in the result queue
                dir_queue.task_done()  # Mark the task as done
        except queue.Empty:
            break


def write_results_to_csv(results, output_file):
    """Writes results to a CSV file."""
    with open(output_file, mode='w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(['Path', 'Size', 'MD5', 'symbolic_link'])  # Write header

        # Prepare data to be written
        data_to_write = []
        for result in results:
            if result:  # Only process if result is not None
                # Split the result by newlines and extend the data_to_write list
                data_to_write.extend(line.split(',') for line in result.splitlines() if line)

        # Write all the data at once using writerows
        csv_writer.writerows(data_to_write)


def read_and_sort_csv(input_file, output_file):
    """Reads a CSV file into a DataFrame, sorts it by the Path column, and writes it back."""
    df = pd.read_csv(input_file)
    sorted_df = df.sort_values(by='Path')  # Sort by Path column
    sorted_df.to_csv(output_file, index=False) 

def get_file_info_parallel(path, num_processes=None):
    if num_processes is None:
        num_processes = 4

    dir_queue = multiprocessing.JoinableQueue()
    result_queue = multiprocessing.Queue()

    # Create and start worker processes
    processes = []
    for _ in range(num_processes):
        p = multiprocessing.Process(target=process_files, args=(dir_queue, result_queue))
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

    # Collect results
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    # Wait for all processes to finish
    for p in processes:
        p.join()

    write_results_to_csv(results,'/home/users/nadeem.yaseen/osfpga/svg/bypythondump.csv')
    read_and_sort_csv('/home/users/nadeem.yaseen/osfpga/svg/bypythondump.csv','/home/users/nadeem.yaseen/osfpga/svg/bypythondump_sorted.csv')
    #return results


if __name__ == "__main__":
    path = "/home/users/nadeem.yaseen/osfpga/svg"
    get_file_info_parallel(path)
    #print(results)
