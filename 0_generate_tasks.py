import json
import boto3
import os

# --- Configuration ---
S3_BUCKET_NAME = 'sptfy-dataset'  # 👈 The name of your S3 bucket
S3_KEY_FOR_JSONL = 'source-data/spotify_podcast_data.jsonl'  # 👈 The "path" to the jsonl file
S3_TODO_PREFIX = 'tasks/todo/'  # The "folder" where new task files will be created

# Initialize Boto3 S3 client
s3_client = boto3.client('s3')

def main():
    """
    Reads the source dataset from S3 and creates a task file for each episode.
    """
    print(f"Starting task generation from s3://{S3_BUCKET_NAME}/{S3_KEY_FOR_JSONL}")
    
    # Stream the source .jsonl file from S3
    source_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY_FOR_JSONL)
    
    tasks_created = 0
    lines_read = 0
    
    for line in source_obj['Body'].iter_lines():
        try:
            lines_read += 1
            episode = json.loads(line.decode('utf-8'))
            episode_id = episode.get('id')
            url = episode.get('enclosure_url')

            if not episode_id or not url:
                print(f"⚠️ Skipping line {lines_read} due to missing data.")
                continue

            task_key = f"{S3_TODO_PREFIX}{episode_id}.task"
            
            # Simple check to see if the task already exists to allow for resumability
            try:
                s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=task_key)
                # If head_object succeeds, the file exists, so we skip it.
                continue
            except s3_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] != '404':
                    raise # Re-raise unexpected errors

            # Upload the task file to S3. The content of the file is the download URL.
            s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=task_key, Body=url)
            tasks_created += 1

            if tasks_created % 1000 == 0:
                print(f"   Created {tasks_created:,} tasks...")

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"❌ Error decoding JSON on line {lines_read}: {e}")
            continue

    print("\n🎉 Task generation complete!")
    print(f"   Total lines read: {lines_read:,}")
    print(f"   New tasks created: {tasks_created:,}")


if __name__ == "__main__":
    main()