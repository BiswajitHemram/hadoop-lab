#!/bin/bash

PROMPT="hadoop@LabExam:~$"

run_cmd() {
  echo "$PROMPT $*"
  eval "$@"
}

BASE_DIR="$HOME/lab5"
INPUT_DIR="$BASE_DIR/input"
CODE_DIR="$BASE_DIR/code"
JAVA_FILE="MatrixMultiplication.java"
JAR_FILE="MatrixMultiplication.jar"
MAIN_CLASS="MatrixMultiplication"
HDFS_INPUT_DIR="/user/prg5/input"
HDFS_OUTPUT_DIR="/user/prg5/output"

# Cleanup previous lab5 directory if it exists
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

    // Mapper class
    public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length < 4) return;  // Ignore malformed lines

            String matrixName = tokens[0];
            int row = Integer.parseInt(tokens[1]);
            int col = Integer.parseInt(tokens[2]);
            int val = Integer.parseInt(tokens[3]);
            
            int matrixK = 2;
            
            if (matrixName.equals("A")) {
                // Emit row-wise elements of A
                for (int k = 0; k < matrixK; k++) {
                    context.write(new Text(row + "," + k), new Text("A," + col + "," + val));
                }
            } else if (matrixName.equals("B")) {
                // Emit column-wise elements of B
                for (int i = 0; i < matrixK; i++) {
                    context.write(new Text(i + "," + col), new Text("B," + row + "," + val));
                }
            }
        }
    }

    // Reducer class
    public static class MultiplicationReducer extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int matrixK = 2;
            int[] vectorA = new int[matrixK];
            int[] vectorB = new int[matrixK];

            // Populate the vectors
            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                String matrixName = tokens[0];
                int index = Integer.parseInt(tokens[1]);
                int val = Integer.parseInt(tokens[2]);

                if (matrixName.equals("A")) {
                    vectorA[index] = val;
                } else if (matrixName.equals("B")) {
                    vectorB[index] = val;
                }
            }

            // Compute dot product
            int result = 0;
            for (int i = 0; i < matrixK; i++) {
                result += vectorA[i] * vectorB[i];
            }

            // Output result (row, col) -> value
            context.write(key, new IntWritable(result));
        }
    }

    // Driver method
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "matrix multiplication");
        job.setJarByClass(MatrixMultiplication.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output

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
