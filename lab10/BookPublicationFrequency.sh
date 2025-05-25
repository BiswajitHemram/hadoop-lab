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

BASE_DIR="$HOME/lab10"
INPUT_DIR="$BASE_DIR/input"
CODE_DIR="$BASE_DIR/code"
JAVA_FILE="BookPublicationFrequency.java"
JAR_FILE="BookPublicationFrequency.jar"
MAIN_CLASS="BookPublicationFrequency"
HDFS_INPUT_DIR="/user/prg10/input"
HDFS_OUTPUT_DIR="/user/prg10/output"
DEFAULT_INPUT="https://raw.githubusercontent.com/BiswajitHemram/hadoop-lab/refs/heads/main/lab10/Books-mini.csv"

# Cleanup previous lab10 directory if it exists
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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BookPublicationFrequency {
    // Mapper Class
    public static class PublicationYearMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final int YEAR_INDEX = 3; // Index of the publication year field

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String line = value.toString();
            if (line.startsWith("ISBN")) return;


            String[] fields = line.split(",");
            if (fields.length > YEAR_INDEX) {
                    String year = fields[YEAR_INDEX].trim();
                    context.write(new Text(year), new IntWritable(1));
            }
        }
    }

    // Reducer Class
    public static class PublicationFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    // Driver Method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "book publication frequency");
        job.setJarByClass(BookPublicationFrequency.class);
        job.setMapperClass(PublicationYearMapper.class);
        job.setReducerClass(PublicationFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path
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
