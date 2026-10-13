import csv
import json
import time
from kafka import KafkaProducer

class LibraryOccupancyStreamer:

    def __init__(self, bootstrap_servers, topic, room_files):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.room_files = room_files
        self.producer = self._initialize_producer()
        self.handles = []

    def _initialize_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def start_streaming(self):
        try:
            print(f"Streaming data to Kafka topic '{self.topic}'...")

            self.handles = [open(f, mode='r') for f in self.room_files]
            readers = [csv.DictReader(h) for h in self.handles]

            while True:
                active_rows = 0
                for i, reader in enumerate(readers):
                    try:
                        row = next(reader)
                        # Metadata injection
                        row['Room_ID'] = f"Room_{i+1}"
                        
                        self.producer.send(self.topic, row)
                        active_rows += 1
                    except StopIteration:
                        continue 

                if active_rows == 0:
                    print("All room files have been processed.")
                    break

                # Simulate real-time delay
                time.sleep(1)

        except KeyboardInterrupt:
            print("\nStreaming stopped by user.")
        finally:
            self.cleanup()

    def cleanup(self):
        print("Cleaning up resources...")
        for h in self.handles:
            h.close()
        self.producer.flush()
        self.producer.close()

if __name__ == "__main__":
    SERVERS = ['localhost:9092']
    TARGET_TOPIC = 'library_occupancy'
    FILES = [f'../raw_data/combined_Room{i}.csv' for i in range(1, 6)]

    streamer = LibraryOccupancyStreamer(SERVERS, TARGET_TOPIC, FILES)
    streamer.start_streaming()
