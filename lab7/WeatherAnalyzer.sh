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

BASE_DIR="$HOME/lab7"
INPUT_DIR="$BASE_DIR/input"
CODE_DIR="$BASE_DIR/code"
JAVA_FILE="WeatherAnalyzer.java"
JAR_FILE="WeatherAnalyzer.jar"
MAIN_CLASS="WeatherAnalyzer"
HDFS_INPUT_DIR="/user/prg7/input"
HDFS_OUTPUT_DIR="/user/prg7/output"

# Cleanup previous lab7 directory if it exists
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAnalyzer {

    // Mapper Class
    public static class WeatherMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] parts = line.split("\\s+");

            // Skip header row or invalid lines
            if (parts.length < 2 || parts[0].equalsIgnoreCase("Date")) {
                return;
            }

            try {
                String date = parts[0];              // Extract date
                int maxTemp = Integer.parseInt(parts[1]); // Extract max temp
                String weatherType = (maxTemp >= 30) ? "Shiny" : "Cool"; // Classification
                context.write(new Text(date), new Text(weatherType));
            } catch (NumberFormatException e) {
                // Skip malformed temperature values
            }
        }
    }

    // Reducer Class
    public static class WeatherReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    // Driver Method
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Analysis");

        job.setJarByClass(WeatherAnalyzer.class);
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
