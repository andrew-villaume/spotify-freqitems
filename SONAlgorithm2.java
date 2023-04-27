

/*
$HADOOP_HOME/bin/hadoop fs -rm -r /Capstone/output
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main SONAlgorithm2.java
jar cf SONAlgorithm2.jar SONAlgorithm2*.class
$HADOOP_HOME/bin/hadoop jar ./SONAlgorithm2.jar SONAlgorithm2 /Capstone/input/preprocessed_spotify_data.txt /Capstone/output

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
 import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

 public class SONAlgorithm2 {
    
   // First Pass: Mapper
   public static class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text itemset = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // value contains a subset of playlists with N lines
        List<String> playlists = Arrays.asList(value.toString().split("\\n"));

        // Implement the Apriori algorithm or any other randomized algorithm
        // to find the frequent itemsets in the subset
        Set<String> frequentItemsets = findFrequentItemsets(playlists);

        // Output key-value pairs (F, 1), where F is a frequent itemset from the subset
        for (String frequentItemset : frequentItemsets) {
            itemset.set(frequentItemset);
            context.write(itemset, new IntWritable(1));
        }
    }

    // Implement this method to find frequent itemsets in the subset using Apriori or other randomized algorithms
    private Set<String> findFrequentItemsets(List<String> playlists) {
      Map<String, Integer> songPairCounts = new HashMap<>();
  
      for (String playlistLine : playlists) {
        String[] playlistData = playlistLine.toString().split("\t", 2); 
        String[] songs = playlistData[1].split(";>:"); 
        for (int i = 0; i < songs.length; i++) {
            for (int j = i + 1; j < songs.length; j++) {
                String[] songData1 = songs[i].split(" - ", 2);
                String[] songData2 = songs[j].split(" - ", 2);
                String artist1 = songData1[0].trim();
                String artist2 = songData2[0].trim();
    
                // Compare artist names, and proceed only if they are not the same
                if (!artist1.equals(artist2) && !songs[i].equals(songs[j])) {
                    String songPair = songs[i] + ";>:" + songs[j];
                    songPairCounts.put(songPair, songPairCounts.getOrDefault(songPair, 0) + 1);
                }
            }
        }
    }
  
      int localSupportCount = 10;
      Set<String> frequentItemsets = new HashSet<>();
      for (Map.Entry<String, Integer> entry : songPairCounts.entrySet()) {
          if (entry.getValue() >= localSupportCount) {
              frequentItemsets.add(entry.getKey());
          }
      }
  
      return frequentItemsets;
  }
}
 
   // First Pass: Reducer
   public static class FirstReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private NullWritable nullValue = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Key: frequent itemset (song pair) from the first map function
        context.write(key, nullValue);
    }
}

// public class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

//   private Set<String> candidateItemsets;
//   private Text songPair = new Text();
//   private IntWritable count = new IntWritable();

//   @Override
//   protected void setup(Context context) throws IOException, InterruptedException {
//       super.setup(context);
//       // Load candidate itemsets from the distributed cache (output of the first phase)
//       candidateItemsets = loadCandidateItemsets(context.getConfiguration());
//   }

//   @Override
//   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//       // value contains a single playlist
//       String[] songs = value.toString().split(" "); // Assuming songs in a playlist are space-separated

//       Map<String, Integer> songPairCounts = new HashMap<>();
//       for (int i = 0; i < songs.length; i++) {
//           for (int j = i + 1; j < songs.length; j++) {
//               String songPair = songs[i] + "," + songs[j];
//               if (candidateItemsets.contains(songPair)) {
//                   songPairCounts.put(songPair, songPairCounts.getOrDefault(songPair, 0) + 1);
//               }
//           }
//       }

//       // Output key-value pairs (C, v)
//       for (Map.Entry<String, Integer> entry : songPairCounts.entrySet()) {
//           songPair.set(entry.getKey());
//           count.set(entry.getValue());
//           context.write(songPair, count);
//       }
//   }

//   private Set<String> loadCandidateItemsets(Configuration conf) throws IOException {
//       // ...
//   }
// }
 
   public static void main(String[] args) throws Exception {
      if (args.length != 2) {
          System.err.println("Usage: SONAlgorithmPhase1 <input path> <output path>");
          System.exit(-1);
      }

      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "SON Algorithm Phase 1");
      job.setJarByClass(SONAlgorithm2.class);

      // Set input format class and number of lines per split
      job.setInputFormatClass(NLineInputFormat.class);
      NLineInputFormat.setNumLinesPerSplit(job, 1000); // Set N as the number of lines per split.

      // Set mapper and reducer classes
      job.setMapperClass(FirstMapper.class);
      job.setReducerClass(FirstReducer.class);

      // Set output key and value classes
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      // Set input and output paths
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      // Exit after completing the job
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
