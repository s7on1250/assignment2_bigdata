#!/usr/bin/python
if [ -n "$1" ]; then
    INPUT_PATH="$1"
    if [ -f "$INPUT_PATH" ] || [ -d "$INPUT_PATH" ]; then
        echo "Local file or directory provided. Uploading to temporary HDFS directory..."
        TEMP_HDFS_INPUT="/temp/local_input"
        hdfs dfs -rm -r "$TEMP_HDFS_INPUT" 2>/dev/null
        hdfs dfs -mkdir -p "$TEMP_HDFS_INPUT"

        if [ -d "$INPUT_PATH" ]; then
            hdfs dfs -put "$INPUT_PATH"/* "$TEMP_HDFS_INPUT"/
        else
            hdfs dfs -put "$INPUT_PATH" "$TEMP_HDFS_INPUT"/
        fi
        INPUT_HDFS="$TEMP_HDFS_INPUT"
    else
        INPUT_HDFS="$INPUT_PATH"
    fi
else
    INPUT_HDFS="/index/data"
fi
OUTPUT_DIR1="/tmp/index/pipeline1"
OUTPUT_DIR2="/tmp/index/pipeline2"
hdfs dfs -mkdir -p '/tmp/index'

hdfs dfs -rm -r $OUTPUT_DIR1

hdfs dfs -rm -r $OUTPUT_DIR2
cd mapreduce
mapred streaming \
-files "mapper1.py,reducer1.py,../cassandra.zip" \
-mapper 'python3 mapper1.py' \
-reducer 'python3 reducer1.py' \
-input $INPUT_HDFS -output $OUTPUT_DIR1"/out1.output"

mapred streaming \
-files "mapper2.py,reducer2.py,../cassandra.zip" \
-mapper 'python3 mapper2.py' \
-reducer 'python3 reducer2.py' \
-input $INPUT_HDFS -output $OUTPUT_DIR2"/out2.output"
