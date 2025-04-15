from operator import itemgetter
import sys
sys.path.insert(0, 'cassandra_driver.libs.zip')
sys.path.insert(0, 'cassandra.zip')
from cassandra.cluster import Cluster



cluster = Cluster(['cassandra-server'])
session = cluster.connect()
session.set_keyspace('my_keyspace')
query_tf = "INSERT INTO tf (word, doc_id, tf) VALUES (%s, %s, %s)"
query_doc = "INSERT INTO docs (doc_id, title,  len) VALUES (%s, %s, %s) IF NOT EXISTS"

current_word = None
current_doc = None
current_count = 0
current_document_count = 0
current_length = 0
current_id = 0
current_title = None
word = None

for doc in sys.stdin:
    line = doc.strip()
    token, values = line.split('\t', 1)
    word = token
    id, title, count, length = values.split('<split>')
    count = int(count)
    if current_word == token:
        current_count += count
        current_id = id
        current_length = int(length)
        current_title = title
    else:
        if current_word:
            w, i = current_word.split('|')
            session.execute(query_tf, (w, int(i), current_count))
            session.execute(query_doc, (int(i), current_title, current_length))
        current_word = token
        current_count = count
        current_title = title
        current_id = id
        current_length = int(length)
if current_word == word:
    w, i = current_word.split('|')
    session.execute(query_tf, (w, int(i), current_count))
    session.execute(query_doc, (int(i), current_title, current_length))