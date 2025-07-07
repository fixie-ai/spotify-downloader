import boto3
import os
import requests
import subprocess
import time
import random
import concurrent.futures

# --- Configuration ---
S3_BUCKET_NAME = 'sptfy-dataset'  # 👈 The name of your S3 bucket
S3_TODO_PREFIX = 'tasks/todo/'
S3_IN_PROGRESS_PREFIX = 'tasks/in_progress/'
S3_COMPLETED_PREFIX = 'tasks/completed/'
S3_FAILED_PREFIX = 'tasks/failed/'
S3_OUTPUT_PREFIX = 'converted-flac-audio/'

# The EC2 instance has temporary local storage we'll use for processing
LOCAL_TEMP_DIR = 'temp_processing'
HEADERS = {'User-Agent': 'PodcastDatasetCrawler-AudioResearch/1.0'}

# Set the number of internal worker threads to match the vCPUs of a c5.4xlarge
MAX_WORKERS = 2

# Boto3 S3 client will be created in the main block
s3_client = boto3.client('s3')

def claim_and_move_task(source_key):
    """Atomically moves a single task file. Returns the new key or None on failure."""
    try:
        episode_id = os.path.basename(source_key).replace('.task', '')
        in_progress_key = f"{S3_IN_PROGRESS_PREFIX}{episode_id}.task"

        s3_client.copy_object(
            Bucket=S3_BUCKET_NAME,
            CopySource={'Bucket': S3_BUCKET_NAME, 'Key': source_key},
            Key=in_progress_key
        )
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=source_key)
        
        return in_progress_key
    except Exception:
        # This can happen if another worker claims the same file. It's safe to ignore.
        return None

def process_task(task_key):
    """
    The core work function for a single thread.
    Downloads, converts, and uploads a single podcast based on a task file.
    """
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
        
        completed_key = f"{S3_COMPLETED_PREFIX}{episode_id}.task"
        s3_client.copy_object(Bucket=S3_BUCKET_NAME, CopySource={'Bucket': S3_BUCKET_NAME, 'Key': task_key}, Key=completed_key)
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=task_key)
        print(f"✅ Completed: {episode_id}")

    except Exception as e:
        print(f"❌ FAILED: {episode_id}. Error: {e}")
        failed_key = f"{S3_FAILED_PREFIX}{episode_id}.task"
        s3_client.copy_object(Bucket=S3_BUCKET_NAME, CopySource={'Bucket': S3_BUCKET_NAME, 'Key': task_key}, Key=failed_key)
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=task_key)
    finally:
        if os.path.exists(temp_original_path): os.remove(temp_original_path)
        if os.path.exists(final_local_path): os.remove(final_local_path)


def main():
    """
    Main worker loop that processes tasks in batches using a thread pool.
    """
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    print(f"--- Worker started. Concurrency level: {MAX_WORKERS} threads. ---")
    
    while True:
        # --- 1. Claim a Batch of Tasks ---
        print("🔍 Searching for a batch of tasks...")
        tasks_to_claim = []
        # List more tasks than we need in case some are claimed by other workers
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_TODO_PREFIX, MaxKeys=MAX_WORKERS * 2)
        
        if 'Contents' in response:
            tasks_to_claim = [task['Key'] for task in response['Contents']]
        
        if not tasks_to_claim:
            print("No tasks found. Sleeping for 60 seconds...")
            time.sleep(60)
            continue

        claimed_tasks = []
        for task_key in tasks_to_claim:
            if len(claimed_tasks) >= MAX_WORKERS:
                break
            moved_key = claim_and_move_task(task_key)
            if moved_key:
                claimed_tasks.append(moved_key)
        
        if not claimed_tasks:
            # All listed tasks were claimed by others before we could get them
            continue

        # --- 2. Process the Batch in Parallel ---
        print(f"⚙️ Processing a batch of {len(claimed_tasks)} tasks...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # The map function will run process_task for each item in claimed_tasks
            # It automatically waits for all threads to complete before moving on.
            list(executor.map(process_task, claimed_tasks))
        
        print("--- Batch complete. Looking for more tasks. ---")


if __name__ == "__main__":
    main()