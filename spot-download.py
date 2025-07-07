import json
import os
import requests
import time
import concurrent.futures

# --- Configuration ---
JSONL_FILE_PATH = 'spotify_podcast_data.jsonl'  # 👈 Update this path
DOWNLOAD_DIRECTORY = 'spotify_podcasts_log'
LOG_FILE = os.path.join(DOWNLOAD_DIRECTORY, 'simulation_log.txt')
MAX_WORKERS = 20  # 👈 1. Number of parallel threads. Start small (e.g., 10) and increase carefully.
headers = {
    'User-Agent': 'PodcastDatasetCrawler-AudioResearch/1.0'
}


# --- Initialization ---
os.makedirs(DOWNLOAD_DIRECTORY, exist_ok=True)

# 2. This function processes a SINGLE line. It will be run on a worker thread.
def process_line(line):
    """Takes one line of the jsonl file, fetches headers, and returns the result."""
    try:
        episode = json.loads(line)
        episode_id = episode.get('id')
        url = episode.get('enclosure_url')

        if not episode_id or not url:
            return None # Skip invalid lines

        response = requests.head(url, timeout=20, allow_redirects=True, headers=headers)
        
        file_size = 0
        if response.status_code == 200:
            file_size = int(response.headers.get('content-length', 0))
        
        # Return a tuple with all the necessary info
        return (episode_id, response.status_code, file_size)

    except (json.JSONDecodeError, requests.exceptions.RequestException):
        # If anything goes wrong, return None so it can be skipped.
        return None

# --- Main Execution Block ---
if __name__ == "__main__":
    # Load previously processed IDs to avoid re-doing work
    processed_ids = set()
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            processed_ids = set(line.strip().split(',')[0] for line in f)
    print(f"✅ Found {len(processed_ids)} previously processed IDs. Resuming...")

    # Read all lines from the source file that need processing
    with open(JSONL_FILE_PATH, 'r') as f:
        lines_to_process = [line for line in f if json.loads(line).get('id') not in processed_ids]

    print(f"📰 Found {len(lines_to_process):,} new episodes to process.")

    # Initialize counters
    total_size_bytes = 0
    podcasts_processed = 0
    valid_podcasts = 0
    
    # 3. Use ThreadPoolExecutor to process lines in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Use executor.map to apply the function to each line. This starts all threads.
        future_to_line = executor.map(process_line, lines_to_process)

        # 4. Process results as they are completed
        with open(LOG_FILE, 'a') as log:
            for result in future_to_line:
                if result is None:
                    continue # Skip lines that failed to parse or connect

                podcasts_processed += 1
                episode_id, status_code, file_size = result
                
                if status_code == 200:
                    valid_podcasts += 1
                    total_size_bytes += file_size
                
                # Write to log immediately
                log.write(f"{episode_id},{file_size}\n")
                log.flush()

                # Print progress update
                if podcasts_processed % 100 == 0:
                    print(f"Processed {podcasts_processed}/{len(lines_to_process)} episodes...")

    # --- Display Final Totals ---
    print("\n🎉 Simulation complete!")
    total_gb = total_size_bytes / 1024 / 1024 / 1024
    total_tb = total_gb / 1024
    success_rate = (valid_podcasts / podcasts_processed * 100) if podcasts_processed > 0 else 0

    print("\n--- Summary for this run ---")
    print(f"Podcasts Processed: {podcasts_processed:,}")
    print(f"Valid (200 OK) Podcasts: {valid_podcasts:,}")
    print(f"Success Rate: {success_rate:.2f}%")
    print("---")
    print(f"Total Scanned Size: {total_gb:,.2f} GB ({total_tb:,.2f} TB)")