import json
import os
import requests
import time
import concurrent.futures
import subprocess

# --- Configuration ---
JSONL_FILE_PATH = 'spotify_podcast_data.jsonl'  # 👈 Update this path
# Directory where the final, converted audio files will be saved
DOWNLOAD_DIRECTORY = 'downloads'
# Log file to track successfully downloaded and converted episodes
LOG_FILE = os.path.join(DOWNLOAD_DIRECTORY, 'download_log.txt')

# Number of parallel download/conversion threads.
# Start with a low number (2-5) as this is very resource-intensive.
MAX_WORKERS = 4

# Custom User-Agent for requests
HEADERS = {
    'User-Agent': 'PodcastDatasetCrawler-AudioResearch/1.0'
}

def download_and_convert(line_data):
    """
    Takes one line of the jsonl file, downloads the audio, converts it to a
    standard 24000 Hz sample rate MP3, and returns the result.
    """
    try:
        episode = json.loads(line_data)
        episode_id = episode.get('id')
        url = episode.get('enclosure_url')

        if not episode_id or not url:
            return None

        # Define paths for the final output and a temporary original file
        final_path = os.path.join(DOWNLOAD_DIRECTORY, f"{episode_id}.mp3")
        temp_original_path = os.path.join(DOWNLOAD_DIRECTORY, f"{episode_id}_original.tmp")

        # --- 1. Download the Original File ---
        with requests.get(url, headers=HEADERS, stream=True, timeout=60) as r:
            r.raise_for_status() # Will raise an error for bad status codes
            with open(temp_original_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        # --- 2. Re-encode with FFmpeg ---
        # This command converts the downloaded file to MP3 with a 24000 Hz sample rate
        command = [
            'ffmpeg',
            '-i', temp_original_path, # Input file
            '-ar', '24000',           # Set audio sample rate to 24000 Hz
            '-ac', '1',               # Set audio channels to 1 (mono) for consistency
            '-y',                     # Overwrite output file if it exists
            final_path
        ]

        # Execute the FFmpeg command, hiding its output unless there's an error
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # --- 3. Cleanup and Return Result ---
        final_size = os.path.getsize(final_path)
        os.remove(temp_original_path) # Delete the larger original file

        return (episode_id, final_size)

    except (json.JSONDecodeError, requests.exceptions.RequestException, subprocess.CalledProcessError, FileNotFoundError) as e:
        # If anything fails, clean up the temp file if it exists and return None
        if 'temp_original_path' in locals() and os.path.exists(temp_original_path):
            os.remove(temp_original_path)
        # Using locals() to safely check if the variable was assigned
        episode_id_for_error = json.loads(line_data).get('id', 'unknown')
        print(f"❌ Error processing {episode_id_for_error}: {e}")
        return None

# --- Main Execution Block ---
if __name__ == "__main__":
    os.makedirs(DOWNLOAD_DIRECTORY, exist_ok=True)

    # --- Load previously downloaded IDs to allow resuming ---
    processed_ids = set()
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            processed_ids = set(line.strip().split(',')[0] for line in f)
    print(f"✅ Found {len(processed_ids):,} previously downloaded episodes. Resuming...")

    # --- Read all lines from the source file that need processing ---
    lines_to_process = []
    with open(JSONL_FILE_PATH, 'r') as f:
        for line in f:
            try:
                # A quick check to avoid re-parsing JSON for every line
                peek_id = line[8:30]
                if peek_id not in processed_ids:
                    lines_to_process.append(line)
            except Exception:
                continue

    if not lines_to_process:
        print("🎉 No new episodes to process. All done!")
        exit()
        
    print(f"📰 Found {len(lines_to_process):,} new episodes to download and convert with {MAX_WORKERS} workers.")

    # --- Initialize counters for this run ---
    total_size_bytes_converted = 0
    podcasts_processed_this_run = 0
    
    # --- Use ThreadPoolExecutor to process files in parallel ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_line = executor.map(download_and_convert, lines_to_process)

        # --- Process results as they are completed ---
        with open(LOG_FILE, 'a') as log:
            for result in future_to_line:
                podcasts_processed_this_run += 1
                
                if result:
                    episode_id, final_size = result
                    total_size_bytes_converted += final_size
                    log.write(f"{episode_id},{final_size}\n")
                    log.flush()

                # Print a progress update every 10 episodes
                if podcasts_processed_this_run % 10 == 0:
                    print(f"   Processed {podcasts_processed_this_run:,}/{len(lines_to_process):,} episodes...")

    # --- Display Final Totals ---
    print("\n🎉 Download and conversion complete!")
    total_gb = total_size_bytes_converted / 1024 / 1024 / 1024

    print("\n--- Summary for This Run ---")
    print(f"Episodes Processed: {podcasts_processed_this_run:,}")
    print(f"Successful Downloads & Conversions: {len(os.listdir(DOWNLOAD_DIRECTORY)) - 1}") # -1 for the log file
    print("---")
    print(f"Total Size of Converted Audio: {total_gb:,.2f} GB")