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

BASE_DIR="$HOME/lab3"
INPUT_DIR="$BASE_DIR/input"
CODE_DIR="$BASE_DIR/code"
JAVA_FILE="HighestTemperature.java"
JAR_FILE="HighestTemperature.jar"
MAIN_CLASS="HighestTemperature"
HDFS_INPUT_DIR="/user/prg3/input"
HDFS_OUTPUT_DIR="/user/prg3/output"
DEFAULT_INPUT="https://raw.githubusercontent.com/BiswajitHemram/hadoop-lab/refs/heads/main/lab3/Temperature.txt"


# Cleanup previous lab4 directory if it exists
if [ -d "$BASE_DIR" ]; then
  run_cmd "rm -rf $BASE_DIR"
fi

# Create input and code directories
run_cmd "mkdir -p $INPUT_DIR $CODE_DIR"

# Ask user for input file (local path or URL)
while true; do
  echo -ne "$PROMPT read -rp \"Input file full path or URL: \" INPUT_PATH\n"
  read -rp "Input file full path or URL: " INPUT_PATH
  INPUT_PATH="${INPUT_PATH:-$DEFAULT_INPUT}"  # Use default if empty

  if [[ "$INPUT_PATH" =~ ^https?:// ]]; then
    INPUT_FILE=$(basename "$INPUT_PATH")
    run_cmd "wget -O \"$INPUT_DIR/$INPUT_FILE\" \"$INPUT_PATH\""
    if [ -f "$INPUT_DIR/$INPUT_FILE" ]; then
      break
    else
      echo "Download failed. Please try again."
    fi
  elif [ -f "$INPUT_PATH" ]; then
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HighestTemperature {

    // Mapper class
    public static class HighestMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] parts = line.split("\\s+");  // Assuming space-separated input

            if (parts.length == 2) {
                String year = parts[0];
                try {
                    int temperature = Integer.parseInt(parts[1]);
                    context.write(new Text(year), new IntWritable(temperature));
                } catch (NumberFormatException e) {
                    System.err.println("Skipping invalid temperature value: " + parts[1]);
                }
            } else {
                System.err.println("Skipping malformed line: " + line);
            }
        }
    }

    // Reducer class
    public static class HighestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int maxTemp = Integer.MIN_VALUE;

            for (IntWritable val : values) {
                maxTemp = Math.max(maxTemp, val.get());
            }

            context.write(key, new IntWritable(maxTemp));
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Highest Temperature Per Year");

        job.setJarByClass(HighestTemperature.class);
        job.setMapperClass(HighestMapper.class);
        job.setReducerClass(HighestReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
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
