import boto3
import os
import requests
import subprocess
import time
import random
import multiprocessing
from botocore.exceptions import ClientError

# --- Configuration ---
S3_BUCKET_NAME = 'sptfy-dataset'  # 👈 The name of your S3 bucket
S3_TODO_PREFIX = 'tasks/download_todo/'
S3_IN_PROGRESS_PREFIX = 'tasks/download_in_progress/'
S3_COMPLETED_PREFIX = 'tasks/download_completed/'
S3_FAILED_PREFIX = 'tasks/download_failed/'
S3_OUTPUT_PREFIX = 'raw-audio/'

LOCAL_TEMP_DIR = 'temp_processing'
HEADERS = {'User-Agent': 'PodcastDatasetCrawler-AudioResearch/1.0'}

# Set the number of concurrent worker PROCESSES.
# A 1.5x ratio to vCPUs is a strong starting point for mixed workloads.
# For a c5.4xlarge (16 vCPUs), 24 is a good value.
NUM_WORKERS = 8

# If the fleet encounters this many consecutive critical failures, all workers on this instance will shut down.
MAX_CONSECUTIVE_FAILURES = 20
# How many times a worker should fail to find a task before shutting down.
IDLE_CHECK_THRESHOLD = 2

# Boto3 clients are not safe to share across processes, so each worker will create its own.

def claim_task():
    """
    Lists tasks in the 'todo' folder and attempts to atomically move one
    to the 'in_progress' folder. Returns the new key or None on failure.
    """
    # Each process needs its own client
    s3_client = boto3.client('s3')
    
    # List more tasks than we strictly need in case of contention
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_TODO_PREFIX, MaxKeys=50)
    if 'Contents' not in response:
        return None

    tasks = response['Contents']
    random.shuffle(tasks) # Randomize to reduce contention between workers

    for task in tasks:
        source_key = task['Key']
        try:
            episode_id = os.path.basename(source_key).replace('.task', '')
            in_progress_key = f"{S3_IN_PROGRESS_PREFIX}{episode_id}.task"

            # Atomic Move: Copy the object, then delete the original.
            s3_client.copy_object(
                Bucket=S3_BUCKET_NAME,
                CopySource={'Bucket': S3_BUCKET_NAME, 'Key': source_key},
                Key=in_progress_key
            )
            s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=source_key)
            
            return in_progress_key
        except ClientError as e:
            # This is an expected race condition if another worker claimed the file.
            if e.response['Error']['Code'] == 'NoSuchKey':
                continue
            else:
                raise # Re-raise other unexpected S3 errors
    return None


def process_task(task_key):
    """
    The core work function for a single task.
    Downloads, converts, and uploads a single podcast.
    Returns True on success, False on failure.
    """
    s3_client = boto3.client('s3')
    episode_id = os.path.basename(task_key).replace('.task', '')
    final_local_path = os.path.join(LOCAL_TEMP_DIR, f"{episode_id}.flac")
    temp_original_path = os.path.join(LOCAL_TEMP_DIR, f"{episode_id}_original.tmp")

    try:
        task_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=task_key)
        url = task_obj['Body'].read().decode('utf-8')
        
        with requests.get(url, headers=HEADERS, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(temp_original_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        command = ['ffmpeg', '-i', temp_original_path, '-ar', '24000', '-ac', '1', '-y', final_local_path]
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        final_s3_key = f"{S3_OUTPUT_PREFIX}{episode_id}.flac"
        s3_client.upload_file(final_local_path, S3_BUCKET_NAME, final_s3_key)
        
        # Defensive Finalization (Success)
        try:
            completed_key = f"{S3_COMPLETED_PREFIX}{episode_id}.task"
            s3_client.copy_object(Bucket=S3_BUCKET_NAME, CopySource={'Bucket': S3_BUCKET_NAME, 'Key': task_key}, Key=completed_key)
            s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=task_key)
            print(f"🎉 Completed: {episode_id}")
            return True # Indicate success
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"⚠️ Note: Task {episode_id} was already completed by another worker.")
                return True # Treat as success
            else:
                raise

    except Exception as e:
        print(f"❌ FAILED: {episode_id}. Error: {e}")
        # Defensive Finalization (Failure)
        try:
            failed_key = f"{S3_FAILED_PREFIX}{episode_id}.task"
            s3_client.copy_object(Bucket=S3_BUCKET_NAME, CopySource={'Bucket': S3_BUCKET_NAME, 'Key': task_key}, Key=failed_key)
            s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=task_key)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                 print(f"⚠️ Note: Failed task {episode_id} was already moved by another worker.")
            else:
                print(f"❌ Critical error: Could not move failed task {episode_id}. Error: {e}")
        return False # Indicate failure
    finally:
        # Cleanup local files
        if os.path.exists(temp_original_path): os.remove(temp_original_path)
        if os.path.exists(final_local_path): os.remove(final_local_path)


def worker_job(rank, failure_counter, lock):
    """
    The main loop for a single worker PROCESS. It continuously tries to claim and
    process tasks until it can't find any or the circuit breaker trips.
    """
    print(f"--- Worker-{rank} started (PID: {os.getpid()}) ---")
    idle_checks = 0
    while idle_checks < IDLE_CHECK_THRESHOLD:
        if failure_counter.value >= MAX_CONSECUTIVE_FAILURES:
            print(f"--- Worker-{rank}: Circuit breaker tripped! Shutting down. ---")
            break

        task_key = claim_task()
        if task_key:
            idle_checks = 0 # Reset idle counter on finding work
            success = process_task(task_key)
            if success:
                # On success, reset the shared failure counter
                failure_counter.value = 0
            else:
                # On failure, use the shared lock to safely increment the counter
                with lock:
                    failure_counter.value += 1
        else:
            # If no tasks are found, increment the idle counter and wait
            idle_checks += 1
            print(f"   Worker-{rank} idle (check {idle_checks}/{IDLE_CHECK_THRESHOLD})... sleeping.")
            time.sleep(30 + random.uniform(0, 30)) # Sleep with jitter
    
    print(f"--- Worker-{rank} shutting down after {IDLE_CHECK_THRESHOLD} idle checks. ---")


def main():
    """
    Initializes and runs the multiprocessing pool.
    """
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    
    # Use a Manager to create shared objects that can be passed between processes
    with multiprocessing.Manager() as manager:
        failure_counter = manager.Value('i', 0)
        lock = manager.Lock() # Create a shared lock
        
        print(f"--- Main process started. Launching {NUM_WORKERS} worker processes. ---")
        
        with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
            # Create a list of arguments for each worker, including the shared lock
            worker_args = [(i, failure_counter, lock) for i in range(NUM_WORKERS)]
            # Map each worker process to the worker_job function with its arguments
            pool.starmap(worker_job, worker_args)

    print("🏁 All worker processes have completed. Main process exiting. Goodbye!")

if __name__ == "__main__":
    # For Linux/macOS, 'fork' is efficient. For Windows/macOS, 'spawn' is safer.
    # We force 'fork' here assuming a Linux EC2 environment.
    multiprocessing.set_start_method("fork", force=True)
    main()
