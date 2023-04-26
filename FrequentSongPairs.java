import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
$HADOOP_HOME/bin/hadoop fs -rm -r /Capstone/output
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main FrequentSongPairs.java
jar cf FrequentSongPairs.jar FrequentSongPairs*.class
$HADOOP_HOME/bin/hadoop jar ./FrequentSongPairs.jar FrequentSongPairs /Capstone/input/preprocessed_spotify_data.txt /Capstone/output

$HADOOP_HOME/bin/hadoop fs -get /Capstone/output
 */

public class FrequentSongPairs {

    public static class PairMapper extends Mapper<Object, Text, Text, Text> {
        private Text pair = new Text();
        private Text playlistId = new Text();

        // for testing purposes, limit the number of playlists processed.
        private int counter = 0;
        private int maxPlaylistsToProcess = 1000;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (counter < maxPlaylistsToProcess){
                String[] splitLine = value.toString().split("\t", 2);
                if (splitLine.length != 2) {
                    return;
                }

                playlistId.set(splitLine[0]);
                List<String> songs = Arrays.asList(splitLine[1].split(";>:"));

                // Generate all possible song pairs within the playlist and emit them as key-value pairs
                for (int i = 0; i < songs.size(); i++) {
                    for (int j = i + 1; j < songs.size(); j++) {
                        String songPair = songs.get(i) + ";>:" + songs.get(j);
                        pair.set(songPair);
                        context.write(pair, playlistId);
                    }
                }
            }
            counter++;
        }
    }

    public static class PairReducer extends Reducer<Text, Text, Text, Text> {
        //private org.w3c.dom.Text playlistIds = new Text();
    

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> uniquePlaylists = new HashSet<>();
            int numPlaylists = 0;

            // Aggregate unique playlists for each song pair
            for (Text playlistId : values) {
                numPlaylists++;
            }
            
            int supportThreshold = 15;

            // If the song pair appears in enough playlists, emit it as a frequent pair
            if (numPlaylists >= supportThreshold) {
                context.write(key, new Text(Integer.toString(numPlaylists)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "frequent song pairs");
        job.setJarByClass(FrequentSongPairs.class);
        job.setMapperClass(PairMapper.class);
        job.setReducerClass(PairReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}