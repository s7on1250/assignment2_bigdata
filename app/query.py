import sys
import math
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

K1 = 1.5
B = 0.75


def main():
    conf = SparkConf() \
        .setAppName("BM25Search") \
        .set("spark.cassandra.connection.host", "cassandra-server")

    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    KEYSPACE = "my_keyspace"

    try:

        query = sys.argv[1] if len(sys.argv) > 1 else sys.stdin.read().strip()
        if not query:
            raise ValueError("Empty query provided")

        query_terms = [t.lower().strip() for t in query.split()]
        query_terms = list(set([t for t in query_terms if t]))
        if not query_terms:
            raise ValueError("No valid query terms")

        docs_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="docs", keyspace=KEYSPACE) \
            .load()

        docs_rdd = docs_df.rdd.map(lambda row: (
            row['doc_id'],
            (row['len'], row['title'])
        )).cache()

        N = docs_rdd.count()
        if N == 0:
            raise ValueError("No documents found in index")

        avgdl = docs_rdd.map(lambda x: x[1][0]).mean()

        df_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="df", keyspace=KEYSPACE) \
            .load()

        df_dict = dict(df_df.rdd
                       .filter(lambda x: x['word'] in query_terms)
                       .map(lambda row: (row['word'], row['df']))
                       .collect())

        idf_dict = {}
        for term in query_terms:
            df = df_dict.get(term, 0)
            idf = math.log((N - df + 0.5) / (df + 0.5) + 1e-9)
            idf_dict[term] = idf

        broadcast_idf = sc.broadcast(idf_dict)

        tf_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="tf", keyspace=KEYSPACE) \
            .load()

        tf_rdd = tf_df.rdd \
            .filter(lambda row: row['word'] in query_terms) \
            .map(lambda row: (row['doc_id'], (row['word'], row['tf'])))

        tf_docs_joined = tf_rdd.join(docs_rdd)

        def compute_bm25(record):
            doc_id, ((word, tf), (doc_len, title)) = record
            idf = broadcast_idf.value.get(word, 0)
            numerator = idf * tf * (K1 + 1)
            denominator = tf + K1 * (1 - B + B * (doc_len / avgdl))
            return (doc_id, (numerator / denominator if denominator else 0, title))

        doc_scores = tf_docs_joined.map(compute_bm25) \
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1])) \
            .mapValues(lambda x: (x[0], x[1]))

        top_docs = doc_scores.takeOrdered(10, key=lambda x: -x[1][0])

        print("Top documents:")
        for doc_id, (score, title) in top_docs:
            print(f"{doc_id}\t{title}")

    finally:
        sc.stop()


if __name__ == "__main__":
    main()