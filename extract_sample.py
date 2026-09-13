"""
Fast CSV Sampler for Large Datasets (5GB+)
Extracts header + first N records in milliseconds without loading the full file into memory.
"""
import os
import sys
import time

def extract_sample(input_file, output_file="data/events_sample.csv", num_records=50000):
    if not os.path.exists(input_file):
        print(f"[!] Error: Input file '{input_file}' not found.")
        return False

    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)

    print(f"[*] Source file  : {input_file} ({os.path.getsize(input_file) / (1024**3):.2f} GB)" if os.path.getsize(input_file) >= 1024**3 else f"[*] Source file  : {input_file} ({os.path.getsize(input_file) / (1024**2):.2f} MB)")
    print(f"[*] Target count : {num_records:,} records (+ header)")
    print(f"[*] Output path  : {output_file}")

    start_time = time.time()
    count = 0

    # Stream line-by-line with 1MB buffer for maximum I/O speed
    with open(input_file, "r", encoding="utf-8", errors="replace", buffering=1024 * 1024) as infile, \
         open(output_file, "w", encoding="utf-8", buffering=1024 * 1024) as outfile:

        # 1. Copy header (prefixed with '#' so Flink's csv.allow-comments skips it cleanly)
        header = infile.readline()
        if not header:
            print("[!] File is empty.")
            return False
        if not header.startswith("#"):
            outfile.write("#" + header)
        else:
            outfile.write(header)

        # 2. Copy first N records and stop immediately
        for line in infile:
            outfile.write(line)
            count += 1
            if count >= num_records:
                break

    elapsed = time.time() - start_time
    output_size_mb = os.path.getsize(output_file) / (1024 * 1024)

    print(f"[+] Success! Extracted {count:,} records in {elapsed:.3f}s")
    print(f"[+] Output size: {output_size_mb:.2f} MB saved to '{output_file}'.")
    return True

if __name__ == "__main__":
    if len(sys.argv) > 1:
        src = sys.argv[1]
        n_records = int(sys.argv[2]) if len(sys.argv) > 2 else 50000
        out = sys.argv[3] if len(sys.argv) > 3 else "data/events_sample.csv"
        extract_sample(src, out, n_records)
    else:
        # Prompt user if no arguments are passed
        print("=" * 60)
        print("  Fast Large CSV Extractor (Kaggle eCommerce Events)")
        print("=" * 60)
        src = input("Enter path to large CSV file (e.g., C:/Users/syd/Downloads/2019-Dec.csv): ").strip().strip('"')
        if not src:
            print("[!] No file path entered. Exiting.")
            sys.exit(1)
        records_input = input("Number of records to extract [default 50000]: ").strip()
        n_records = int(records_input) if records_input.isdigit() else 50000
        out_input = input("Output file path [default data/events_sample.csv]: ").strip().strip('"')
        out = out_input if out_input else "data/events_sample.csv"
        extract_sample(src, out, n_records)
