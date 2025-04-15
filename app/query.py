import sys
import math
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

K1 = 1.5
B  = 0.75

conf = SparkConf() \
    .setAppName("BM25Search") \
    .set("spark.cassandra.connection.host", "cassandra-server")

sc = SparkContext(conf=conf)
spark = SparkSession(sc)

KEYSPACE = "my_keyspace"

if len(sys.argv) > 1:
    query = sys.argv[1]
else:
    query = sys.stdin.read().strip()

query_terms = list(set(query.lower().split()))
docs_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="docs", keyspace=KEYSPACE) \
    .load()

docs_rdd = docs_df.rdd.map(lambda row: (
    row['doc_id'],
    (row['len'], row['title'])
)).cache()

N = docs_rdd.count()
total_dl = docs_rdd.map(lambda t: t[1][0]).sum()
avgdl = total_dl / N if N > 0 else 0

df_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="df", keyspace=KEYSPACE) \
    .load()
df_rdd = df_df.rdd.map(lambda row: (row['word'], row['df']))
df_dict = dict(df_rdd.filter(lambda x: x[0] in query_terms).collect())

def compute_idf(word):
    df_val = df_dict.get(word, 0)
    if df_val == 0:
        return 0
    return math.log((N - df_val + 0.5) / (df_val + 0.5))

idf_dict = {word: compute_idf(word) for word in query_terms}
broadcast_idf = sc.broadcast(idf_dict)

tf_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="tf", keyspace=KEYSPACE) \
    .load()
tf_rdd = tf_df.rdd.map(lambda row: (row['doc_id'], (row['word'], row['tf']))) \
         .filter(lambda x: x[1][0] in query_terms)
tf_docs_joined = tf_rdd.join(docs_rdd)


def compute_bm25(record):
    doc_id, ((word, tf_val), (doc_len, title)) = record
    idf_val = broadcast_idf.value.get(word, 0)
    if idf_val <= 0 or tf_val <= 0:
        return (doc_id, (0, title))

    denominator = tf_val + K1 * (1 - B + B * (doc_len / avgdl))
    score = idf_val * (tf_val * (K1 + 1)) / denominator
    return (doc_id, (max(score, 0), title))

doc_term_scores = tf_docs_joined.map(compute_bm25)

doc_scores = doc_term_scores.reduceByKey(lambda a, b: (a[0] + b[0], a[1]))

top_docs = doc_scores.takeOrdered(10, key=lambda x: -x[1][0])

for doc_id, (score, title) in top_docs:
    print(f"Doc ID: {doc_id}, Title: {title}, Score: {score:.4f}")

sc.stop()
