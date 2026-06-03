import json
import os
import glob
import time
import pandas as pd
from kafka import KafkaProducer
from tqdm import tqdm


class JsonProducer:
    """A Kafka producer that reads from multiple JSON files and supports checkpointing."""
    
    CHECKPOINT_FILE = "producer_checkpoint.json"

    def __init__(self, bootstrap_servers: str, topic_name: str, file_pattern: str, records_per_second: int = 100):
        self.topic_name = topic_name
        self.file_pattern = file_pattern
        self.records_per_second = records_per_second
        self.delay_per_record = 1.0 / records_per_second if records_per_second > 0 else 0
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=50,
            batch_size=131072,
            compression_type='gzip',
            buffer_memory=67108864,
            api_version=(3, 6, 0)
        )
        self.last_file = None
        self.last_index = -1
        self._load_checkpoint()

    def _load_checkpoint(self):
        """Loads the last processed file and index from the checkpoint file."""
        if os.path.exists(self.CHECKPOINT_FILE):
            try:
                with open(self.CHECKPOINT_FILE, "r") as f:
                    content = f.read().strip()
                    if not content:
                        return
                    checkpoint = json.loads(content)
                    self.last_file = checkpoint.get("last_file")
                    self.last_index = checkpoint.get("last_index", -1)
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Warning: Could not load checkpoint file: {e}. Starting from scratch.")

    def _save_checkpoint(self, current_file: str, current_index: int):
        """Saves current progress to the checkpoint file."""
        with open(self.CHECKPOINT_FILE, "w") as f:
            json.dump({
                "last_file": current_file,
                "last_index": current_index
            }, f)

    def _get_sorted_files(self):
        """Finds all files matching the pattern and sorts them numerically."""
        files = glob.glob(self.file_pattern)
        
        def extract_num(f):
            basename = os.path.basename(f)
            try:
                # Extract number from 'sub_raw_<num>.json'
                return int(basename.replace("sub_raw_", "").replace(".json", ""))
            except ValueError:
                return 0
                
        return sorted(files, key=extract_num)

    def _determine_start_index(self, files: list) -> int:
        """Determines the file index to start processing from based on checkpoint."""
        if self.last_file in files:
            return files.index(self.last_file)
        return 0

    def _process_file(self, file_path: str):
        """Reads a JSON file and sends its records to Kafka."""
        try:
            df = pd.read_json(file_path)
            records = df.to_dict(orient="records")
        except ValueError as e:
            print(f"Error reading {file_path}: {e}")
            return

        start_row_idx = 0
        if file_path == self.last_file and self.last_index >= 0:
            start_row_idx = self.last_index + 1
            if start_row_idx >= len(records):
                print(f"File {file_path} was already fully processed.")
                return
            
        print(f"Total records: {len(records)}, Starting from offset: {start_row_idx}")
        
        current_idx = start_row_idx
        try:
            for current_idx in tqdm(range(start_row_idx, len(records)), desc=f"Sending {os.path.basename(file_path)}"):
                row = records[current_idx]
                self.producer.send(self.topic_name, row)
                
                # Rate limiting: only sleep if records_per_second is set to a reasonable positive value
                if self.records_per_second > 0 and self.delay_per_record > 0:
                    time.sleep(self.delay_per_record)
                
                # Checkpoint every 10000 records WITHOUT flushing (much faster)
                if (current_idx - start_row_idx + 1) % 10000 == 0:
                    self._save_checkpoint(file_path, current_idx)
            
            # Flush after fully processing a file
            self.producer.flush()
            self._save_checkpoint(file_path, len(records) - 1)
            
            # Update checkpoint state for the next file
            self.last_file = file_path
            self.last_index = len(records) - 1
            
        except KeyboardInterrupt:
            print("\nProcess interrupted. Flushing Kafka and saving checkpoint...")
            self.producer.flush()
            self._save_checkpoint(file_path, current_idx)
            print(f"Saved Checkpoint: File={file_path}, Index={current_idx}")
            raise  # Re-raise to gracefully exit the main loop

    def run(self):
        """Main execution loop to find, sort, and process files."""
        files = self._get_sorted_files()
        if not files:
            print(f"No files found matching pattern: {self.file_pattern}")
            return

        print(f"Loaded checkpoint: File={self.last_file}, Index={self.last_index}")
        
        start_file_idx = self._determine_start_index(files)
        
        try:
            for i in range(start_file_idx, len(files)):
                current_file = files[i]
                print(f"\nProcessing file {i+1}/{len(files)}: {current_file}")
                self._process_file(current_file)
                
        except KeyboardInterrupt:
            print("\nExiting...")
            return
            
        print("\nFinished sending all data to Kafka!")


def main():
    producer = JsonProducer(
        bootstrap_servers="127.0.0.1:9092",
        topic_name="buswaypoint_json",
        file_pattern="data/HPCLAB/part1/part1/sub_raw_*.json",
        records_per_second=100  # Set to 100 for around 100 msg/s
    )
    producer.run()


if __name__ == "__main__":
    main()