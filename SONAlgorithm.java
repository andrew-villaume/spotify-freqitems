

/*
$HADOOP_HOME/bin/hadoop fs -rm -r /Capstone/output
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main SONAlgorithm.java
jar cf SONAlgorithm.jar SONAlgorithm*.class
$HADOOP_HOME/bin/hadoop jar ./SONAlgorithm.jar SONAlgorithm /Capstone/input/preprocessed_spotify_data.txt /Capstone/output

$HADOOP_HOME/bin/hadoop fs -get /Capstone/output
 */

 import java.io.IOException;
 import java.util.*;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.*;
 import org.apache.hadoop.mapreduce.*;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 import org.apache.hadoop.util.GenericOptionsParser;
 
 public class SONAlgorithm {
    
   // First Pass: Mapper
   public static class FirstPassMapper extends Mapper<Object, Text, Text, IntWritable> {
     private final static IntWritable one = new IntWritable(1);
     private Text itemset = new Text();

     private int counter = 0;
    private int maxPlaylistsToProcess = 1000;
 
 
     public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (counter < maxPlaylistsToProcess){
            String[] playlistData = value.toString().split("\t", 2); // Split playlist data by the first tab
            String[] songs = playlistData[1].split(";>:"); // Split songs by ';>:'
        
            // Apply the Apriori 
            // Generate candidate itemsets (pairs, triples, etc.)
        
            for (int i = 0; i < songs.length; i++) {
                for (int j = i + 1; j < songs.length; j++) {
                itemset.set(songs[i] + ";>:" + songs[j]);
                context.write(itemset, one);
                }
            }
        }
        counter++;
     }
   }
 
   // First Pass: Reducer
   public static class FirstPassReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
     private IntWritable result = new IntWritable();
 
     public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       int sum = 0;
       for (IntWritable val : values) {
         sum += val.get();
       }
 
       // Set a local support threshold for the first pass
       int localSupportThreshold = 50; // Adjust this value as needed
 
       if (sum >= localSupportThreshold) {
         result.set(sum);
         context.write(key, result);
       }
     }
   }
 
   // Second Pass: Mapper
   public static class SecondPassMapper extends Mapper<Object, Text, Text, IntWritable> {
     private final static IntWritable one = new IntWritable(1);
 
     public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       String[] parts = value.toString().split("\t");
       String itemset = parts[0];
       context.write(new Text(itemset), one);
     }
   }
 
   // Second Pass: Reducer
   public static class SecondPassReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
     private IntWritable result = new IntWritable();
 
     public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       int sum = 0;
       for (IntWritable val : values) {
         sum += val.get();
       }
 
       // Set a global support threshold for the second pass
       int globalSupportThreshold = 10; // Adjust this value as needed
 
       if (sum >= globalSupportThreshold) {
         result.set(sum);
         context.write(key, result);
       }
     }
   }
 
   public static void main(String[] args) throws Exception {
     Configuration conf = new Configuration();
     String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

     // First pass job configuration
    Job firstPassJob = Job.getInstance(conf, "SONAlgorithm - First Pass");
    firstPassJob.setJarByClass(SONAlgorithm.class);
    firstPassJob.setMapperClass(FirstPassMapper.class);
    firstPassJob.setReducerClass(FirstPassReducer.class);
    firstPassJob.setOutputKeyClass(Text.class);
    firstPassJob.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(firstPassJob, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(firstPassJob, new Path("/Capstone/intermediate_output"));
    firstPassJob.waitForCompletion(true);
    // Second pass job configuration
    Job secondPassJob = Job.getInstance(conf, "SONAlgorithm - Second Pass");
    secondPassJob.setJarByClass(SONAlgorithm.class);
    secondPassJob.setMapperClass(SecondPassMapper.class);
    secondPassJob.setReducerClass(SecondPassReducer.class);
    secondPassJob.setOutputKeyClass(Text.class);
    secondPassJob.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(secondPassJob, new Path("/Capstone/intermediate_output"));
    FileOutputFormat.setOutputPath(secondPassJob, new Path(otherArgs[1]));
    System.exit(secondPassJob.waitForCompletion(true) ? 0 : 1);
    }
}
