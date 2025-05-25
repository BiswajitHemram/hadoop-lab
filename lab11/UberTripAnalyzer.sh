#!/bin/bash

# ANSI escape codes for green prompt and reset color
GREEN="\e[32m"
RESET="\e[0m"

# Dynamic prompt with user@hostname:~$ in green color
PROMPT="${GREEN}${USER}@$(hostname -s):~\$${RESET}"

run_cmd() {
  # Print command with green prompt
  echo -e "${PROMPT} $*"
  eval "$@"
}

BASE_DIR="$HOME/lab11"
INPUT_DIR="$BASE_DIR/input"
CODE_DIR="$BASE_DIR/code"
JAVA_FILE="UberTripAnalyzer.java"
JAR_FILE="UberTripAnalyzer.jar"
MAIN_CLASS="UberTripAnalyzer"
HDFS_INPUT_DIR="/user/prg11/input"
HDFS_OUTPUT_DIR="/user/prg11/output"

# Cleanup previous lab11 directory if it exists
if [ -d "$BASE_DIR" ]; then
  run_cmd "rm -rf $BASE_DIR"
fi

# Create input and code directories
run_cmd "mkdir -p $INPUT_DIR $CODE_DIR"

# Ask user for input file (local path or URL)
while true; do
  echo -n "$PROMPT read -rp \"Input file full path or URL: \" INPUT_PATH"
  read -rp "Input file full path or URL: " INPUT_PATH

  if [[ "$INPUT_PATH" =~ ^https?:// ]]; then
    # Handle URL input
    INPUT_FILE=$(basename "$INPUT_PATH")
    run_cmd "wget -O \"$INPUT_DIR/$INPUT_FILE\" \"$INPUT_PATH\""
    if [ -f "$INPUT_DIR/$INPUT_FILE" ]; then
      break
    else
      echo "Download failed. Please try again."
    fi
  elif [ -f "$INPUT_PATH" ]; then
    # Handle local file input
    INPUT_FILE=$(basename "$INPUT_PATH")
    run_cmd "cp \"$INPUT_PATH\" \"$INPUT_DIR/$INPUT_FILE\""
    break
  else
    echo "File not found or invalid URL. Please try again."
  fi
done

# Create Java source file if not exists
if [ ! -f "$CODE_DIR/$JAVA_FILE" ]; then
  echo "$PROMPT cat > $CODE_DIR/$JAVA_FILE << 'EOF'"
  cat > "$CODE_DIR/$JAVA_FILE" << 'EOF'
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UberTripAnalyzer {

    // Mapper
    public static class TripMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Skip header
            if (line.startsWith("dispatching_base_number")) return;

            String[] fields = line.split(",");
            if (fields.length >= 4) {
                String basement = fields[0].trim();
                String date = fields[1].trim();
                String trips = fields[3].trim();
                context.write(new Text(date), new Text(basement + "," + trips));
            }
        }
    }

    // Reducer
    public static class TripReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxTrips = Integer.MIN_VALUE;
            String maxBasement = "";

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                String basement = parts[0];
                int trips = Integer.parseInt(parts[1]);

                if (trips > maxTrips) {
                    maxTrips = trips;
                    maxBasement = basement;
                }
            }
            context.write(key, new Text(maxBasement + "\t" + maxTrips));
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "uber trip analyzer");

        job.setJarByClass(UberTripAnalyzer.class);
        job.setMapperClass(TripMapper.class);
        job.setReducerClass(TripReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
EOF
else
  echo "$PROMPT echo \"$JAVA_FILE already exists, skipping creation.\""
fi

# Compile Java code
run_cmd "cd $CODE_DIR"
run_cmd "export HADOOP_CLASSPATH=\$(hadoop classpath)"
run_cmd "javac -classpath \$HADOOP_CLASSPATH -d . $JAVA_FILE"

# Create JAR file
run_cmd "cd $BASE_DIR"
run_cmd "jar -cvf $JAR_FILE -C code/ ."

# Prepare HDFS input directory
run_cmd "hdfs dfs -test -e $HDFS_INPUT_DIR"
if [ $? -eq 0 ]; then
  run_cmd "hdfs dfs -rm -r -f $HDFS_INPUT_DIR"
fi
run_cmd "hdfs dfs -mkdir -p $HDFS_INPUT_DIR"
run_cmd "hdfs dfs -put -f $INPUT_DIR/$INPUT_FILE $HDFS_INPUT_DIR"

# Prepare HDFS output directory
run_cmd "hdfs dfs -test -e $HDFS_OUTPUT_DIR"
if [ $? -eq 0 ]; then
  run_cmd "hdfs dfs -rm -r -f $HDFS_OUTPUT_DIR"
fi

# Run Hadoop job
run_cmd "hadoop jar $JAR_FILE $MAIN_CLASS $HDFS_INPUT_DIR/$INPUT_FILE $HDFS_OUTPUT_DIR"

# Display output
run_cmd "hdfs dfs -cat $HDFS_OUTPUT_DIR/part-r-00000"
