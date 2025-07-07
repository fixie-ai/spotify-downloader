import os
import subprocess
import requests # For the download part

def download_and_standardize(url, output_path):
    """Downloads a file and converts it to a standard format using FFmpeg."""
    
    # --- 1. Download the Original File ---
    # Create a temporary path for the original download
    original_path = output_path.replace('.mp3', '_original.tmp')
    
    try:
        print(f"⬇️ Downloading original file to {original_path}...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(original_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to download {url}: {e}")
        return

    # --- 2. Re-encode with FFmpeg ---
    print(f"⚙️ Converting {original_path} to {output_path} at 24000...")
    
    # Construct the FFmpeg command
    # -i: input file
    # -b:a: set audio bitrate
    # -y: overwrite output file if it exists
    # Construct the FFmpeg command for changing the sample rate
    # -ar: set audio sample rate
    command = [
        'ffmpeg',
        '-i', original_path,
        '-ar', '24000',  # Sets the sample rate to 24000 Hz
        '-y',
        output_path
    ]

    try:
        # Execute the command
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"✅ Successfully created {output_path}")
    except subprocess.CalledProcessError as e:
        # If FFmpeg fails, print its error output
        print(f"❌ FFmpeg failed for {original_path}:")
        print(e.stderr.decode())
    except FileNotFoundError:
        print("❌ FFmpeg not found. Is it installed and in your system's PATH?")
    finally:
        # --- 3. Cleanup ---
        # Delete the original, larger file to save space
        print(f"🗑️ Deleting temporary file {original_path}...")
        os.remove(original_path)

# --- Example Usage ---
if __name__ == "__main__":
    podcast_url = "https://anchor.fm/s/11b84b68/podcast/play/9079164/https%3A%2F%2Fd3ctxlq1ktw2nl.cloudfront.net%2Fproduction%2F2019-11-18%2F39310279-44100-2-39e843297150c.m4a"
    standardized_output_file = '000A9sRBYdVh66csG2qEdj.mp3'
    
    download_and_standardize(podcast_url, standardized_output_file)