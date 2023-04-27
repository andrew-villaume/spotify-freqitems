
/*
$HADOOP_HOME/bin/hadoop fs -rm -r /Capstone/output

$HADOOP_HOME/bin/hadoop fs -rm -r /Capstone/output/phase2
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main SONAlgorithm2.java
jar cf SONAlgorithm2.jar SONAlgorithm2*.class
$HADOOP_HOME/bin/hadoop jar ./SONAlgorithm2.jar SONAlgorithm2 -D mapreduce.framework.name=yarn /Capstone/input/preprocessed_spotify_data.txt /Capstone/output

$HADOOP_HOME/bin/hadoop fs -get /Capstone/output

$HADOOP_HOME/bin/hadoop fs -rm -r /Capstone/output/phase2
$HADOOP_HOME/bin/hadoop fs -get /Capstone/output/phase2
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;


public class SONAlgorithm2 extends Configured implements Tool {

    // First Pass: Mapper
    public static class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text itemset = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value contains a subset of playlists with 1000 lines
            List<String> playlists = Arrays.asList(value.toString().split("\\n"));

            Set<String> frequentItemsets = findFrequentItemsets(playlists);

            // Output key-value pairs (F, 1), where F is a frequent itemset from the subset
            for (String frequentItemset : frequentItemsets) {
                itemset.set(frequentItemset);
                context.write(itemset, new IntWritable(1));
            }
        }

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

            int localSupportCount = 20;
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
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Key: frequent itemset (song pair) from the first map function
            context.write(key, nullValue);
        }
    }

    public static class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Set<String> candidateItemsets;
        private Text songPair = new Text();
        private IntWritable count = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // Load candidate itemsets from the distributed cache (output of the first phase)
            candidateItemsets = loadCandidateItemsets(context.getConfiguration());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value contains a single playlist
            String[] songs = value.toString().split(";>:");

            Map<String, Integer> songPairCounts = new HashMap<>();
            for (int i = 0; i < songs.length; i++) {
                for (int j = i + 1; j < songs.length; j++) {
                    String songPair = songs[i] + ";>:" + songs[j];
                    if (candidateItemsets.contains(songPair)) {
                        songPairCounts.put(songPair, songPairCounts.getOrDefault(songPair, 0) + 1);
                    }
                }
            }

            for (Map.Entry<String, Integer> entry : songPairCounts.entrySet()) {
                songPair.set(entry.getKey());
                count.set(entry.getValue());
                context.write(songPair, count);
            }
        }

        private Set<String> loadCandidateItemsets(Configuration conf) throws IOException {
            Set<String> candidateItemsets = new HashSet<>();

            // Load the candidate itemsets file from the distributed cache
            URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    FileSystem fs = FileSystem.get(cacheFile, conf);
                    Path path = new Path(cacheFile.toString());
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String itemset = line.trim();
                            if (!itemset.isEmpty()) {
                                candidateItemsets.add(itemset);
                            }
                        }
                    }
                }
            }

            return candidateItemsets;
        }
    }

    public static class SecondReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            
            int supportThreshold = 60;
            if (sum >= supportThreshold) {
                context.write(key, new IntWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // ToolRunner allows for command line configuration parameters - suitable for
        // shifting between local job and yarn
        // example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value
        // <input_path> <output_path>
        // We use -D mapreduce.framework.name=<value> where <value>=local means the job
        // is run locally and <value>=yarn means using YARN
        int res = ToolRunner.run(new Configuration(), new SONAlgorithm2(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();

        // //JOB 1 (comment this out on second run)
        // Job job = Job.getInstance(conf, "SON Algorithm Phase 1");
        // job.setJarByClass(SONAlgorithm2.class);

        // // Set input format class and number of lines per split
        // job.setInputFormatClass(NLineInputFormat.class);
        // NLineInputFormat.setNumLinesPerSplit(job, 2000); // num lines per split

        // // Set mapper and reducer classes
        // job.setMapperClass(FirstMapper.class);
        // job.setReducerClass(FirstReducer.class);

        // // Set output key and value classes
        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(IntWritable.class);

        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // return job.waitForCompletion(true) ? 0 : 1;

        //JOB 2 (comment this out on first run)
        DistributedCache.addCacheFile(new URI("/Capstone/output/part-r-00000"), conf);
    

        Job job2 = new Job(conf, "SONAlgorithm Phase 2");
        job2.setJarByClass(SONAlgorithm2.class);
        job2.setMapperClass(SecondMapper.class);
        job2.setReducerClass(SecondReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/phase2"));

        //System.exit(job2.waitForCompletion(true) ? 0 : 1);
        return job2.waitForCompletion(true) ? 0 : 1;
    }
}
