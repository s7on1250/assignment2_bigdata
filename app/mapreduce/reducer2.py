from operator import itemgetter
import sys
sys.path.insert(0, 'cassandra_driver.libs.zip')
sys.path.insert(0, 'cassandra.zip')
from cassandra.cluster import Cluster



cluster = Cluster(['cassandra-server'])
session = cluster.connect()
session.set_keyspace('my_keyspace')
query_df = "INSERT INTO df (word, df) VALUES (%s, %s)"
current_word = None
current_doc = None
current_count = 0
current_document_count = 0
current_length = 0
current_id = 0
word = None
for doc in sys.stdin:
    line = doc.strip()
    token, values = line.split('\t', 1)
    word = token
    id, count = values.split(',')
    count = int(count)
    if current_word == token and id != current_id:
        current_count += count
        current_id = id
    elif current_word != token:
        if current_word:
            session.execute(query_df, (current_id, current_count))
            #print(f'{current_word}\t{current_count}')
        current_word = token
        current_id = id
if current_word == word:
    session.execute(query_df, (current_id, current_count))
    #print(f'{current_word}\t{current_count}')