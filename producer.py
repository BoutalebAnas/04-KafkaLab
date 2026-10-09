# %%
import socket
from confluent_kafka import Producer
import zipfile
from xml.etree import ElementTree

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic='book_gutenberg'
book_file='pg79744-images-3.epub'
# %% Streaming Query

with zipfile.ZipFile(book_file, 'r') as book:
    for filename in book.namelist():
        if filename.endswith(('.xhtml', '.html', '.htm')):
            with book.open(filename) as file:
                tree = ElementTree.parse(file)
                content = ' '.join(tree.getroot().itertext())

                for line in content.splitlines():
                    line = line.strip()
                    if line:
                        producer.produce(topic=topic, value=line)
                        print(line)
                        producer.poll(0)

producer.flush()
print("Envoi du livre terminé")
