import boto3
import os
import requests
import subprocess
import time
import random
import concurrent.futures

# --- Configuration ---
S3_BUCKET_NAME = 'sptfy-dataset'  # 👈 The name of your S3 bucket
S3_TODO_PREFIX = 'tasks/download_todo/'
S3_IN_PROGRESS_PREFIX = 'tasks/download_in_progress/'
S3_COMPLETED_PREFIX = 'tasks/download_completed/'
S3_FAILED_PREFIX = 'tasks/download_failed/'
S3_OUTPUT_PREFIX = 'raw-audio/'

LOCAL_TEMP_DIR = 'temp_processing'
HEADERS = {'User-Agent': 'PodcastDatasetCrawler-AudioResearch/1.0'}

# Set the number of internal worker threads to match the vCPUs of your instance
MAX_WORKERS = 16 

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
        
        # --- Defensive Finalization (Success) ---
        try:
            completed_key = f"{S3_COMPLETED_PREFIX}{episode_id}.task"
            s3_client.copy_object(Bucket=S3_BUCKET_NAME, CopySource={'Bucket': S3_BUCKET_NAME, 'Key': task_key}, Key=completed_key)
            s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=task_key)
            print(f"✅ Completed: {episode_id}")
        except s3_client.exceptions.NoSuchKey:
            # This is safe. It means another worker finished this task while we were working.
            print(f"⚠️ Note: Task {episode_id} was already completed by another worker.")

    except Exception as e:
        print(f"❌ FAILED: {episode_id}. Error: {e}")
        # --- Defensive Finalization (Failure) ---
        try:
            failed_key = f"{S3_FAILED_PREFIX}{episode_id}.task"
            s3_client.copy_object(Bucket=S3_BUCKET_NAME, CopySource={'Bucket': S3_BUCKET_NAME, 'Key': task_key}, Key=failed_key)
            s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=task_key)
        except s3_client.exceptions.NoSuchKey:
            # Another worker might have also failed on this task and moved it already.
             print(f"⚠️ Note: Failed task {episode_id} was already moved by another worker.")
        except Exception as move_err:
            print(f"❌ Critical error: Could not move failed task {episode_id}. Error: {move_err}")

    finally:
        # Cleanup local files regardless of success or failure
        if os.path.exists(temp_original_path): os.remove(temp_original_path)
        if os.path.exists(final_local_path): os.remove(final_local_path)


def main():
    """
    Main worker loop that processes tasks in batches using a thread pool.
    """
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    print(f"--- Worker started. Concurrency level: {MAX_WORKERS} threads. ---")
    
    while True:
        print("🔍 Searching for a batch of tasks...")
        tasks_to_claim = []
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
            continue

        print(f"⚙️ Processing a batch of {len(claimed_tasks)} tasks...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            list(executor.map(process_task, claimed_tasks))
        
        print("--- Batch complete. Looking for more tasks. ---")


if __name__ == "__main__":
    main()
