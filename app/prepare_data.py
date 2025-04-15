from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession, functions


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()


df = spark.read.parquet("/a.parquet")
n = 1500
n_final = 1100
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)

def create_doc(row):
    title = row['title'].replace('\n', ' ')
    processed_text = row['text'].replace('\n', ' ')
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + title).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(processed_text)

df = df.withColumn(
    "title",

    functions.regexp_replace(
        functions.regexp_replace(functions.col("title"), "\n", " "),
        "\t", " "
    )
)

filtered_df = df.select(['id', 'title', 'text']).filter(
    (functions.col("text").isNotNull()) &
    (functions.length(functions.trim(functions.col("text"))) > 0) &
    (~functions.trim(functions.col("text")).rlike("^<.*>$")) &
    (~functions.trim(functions.col("text")).rlike("^\\{.*\\}$"))
)
filtered_df = filtered_df.select(['id', 'title', 'text']).sample(fraction=n_final / df.count(), seed=0).limit(n_final)
filtered_df.foreach(create_doc)

filtered_df.write.mode("overwrite").csv("/index/data", sep = "\t")
