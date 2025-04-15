from cassandra.cluster import Cluster
import zipfile
import cassandra
import os


def zip_package(package, zip_filename):
    package_dir = os.path.dirname(package.__file__)
    package_name = package.__name__

    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(package_dir):
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, os.path.dirname(package_dir))
                zf.write(file_path, rel_path)

zip_package(cassandra, 'cassandra.zip')

print("hello app")

# Connects to the cassandra server
cluster = Cluster(['cassandra-server'])

session = cluster.connect()
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS my_keyspace
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")
session.set_keyspace('my_keyspace')
# session.execute("""
# DROP TABLE IF EXISTS df;
# """)
# session.execute("""
# DROP TABLE IF EXISTS tf;
# """)
# session.execute("""
# DROP TABLE IF EXISTS docs;
# """)
session.execute("""
    CREATE TABLE IF NOT EXISTS df (
        word text PRIMARY KEY,
        df int
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS tf (
        word text,
        doc_id int,
        tf int,
        PRIMARY KEY (word, doc_id)
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS docs (
        doc_id int PRIMARY KEY,
        title text,
        len int
    )
""")

# Print each row\

# # displays the available keyspaces
# rows = session.execute('DESC keyspaces')
# for row in rows:
#     print(row)