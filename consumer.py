# %%
from confluent_kafka import Consumer
import re

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'foo',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

# %%
topic='book_gutenberg'
consumer.subscribe([topic])

output_file = 'cleaned_book.txt'

# %%
# Configuration
MAX_EMPTY_POLLS = 10  # Ends after ~10 seconds of silence
MAX_ERRORS = 5        # Ends after 5 consecutive errors
empty_polls = 0
error_count = 0


with open(output_file, 'w', encoding='utf-8') as file:
    try:
        while True:
            msg = consumer.poll(1.0)

            # 1. Handle "No Message" (Timeout)
            if msg is None:
                empty_polls += 1
                if empty_polls >= MAX_EMPTY_POLLS:
                    print("Closing: No new messages received.")
                    break
                continue

            # 2. Handle Errors
            if msg.error():
                error_count += 1
                print(f"Consumer error: {msg.error()}")
                if error_count >= MAX_ERRORS:
                    print("Closing: Too many consecutive errors.")
                    break
                continue

            # 3. Handle Success
            # Reset counters when we actually get data
            empty_polls = 0
            error_count = 0

            text = msg.value().decode('utf-8')

            text = text.lower()
            text = re.sub(r'[^a-z0-9\s]', ' ', text)
            text = re.sub(r'\s+', ' ', text).strip()

            if text:
                print(text)
                file.write(text + '\n')

    finally:
        consumer.close()

print(f"Cleaned text saved to {output_file}")