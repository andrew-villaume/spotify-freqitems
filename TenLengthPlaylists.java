import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
$HADOOP_HOME/bin/hadoop fs -rm -r /Capstone/output
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main TenLengthPlaylists.java
jar cf TenLengthPlaylists.jar TenLengthPlaylists*.class
$HADOOP_HOME/bin/hadoop jar ./TenLengthPlaylists.jar TenLengthPlaylists /Capstone/input/preprocessed_spotify_data.txt /Capstone/output

$HADOOP_HOME/bin/hadoop fs -get /Capstone/output
 */

public class TenLengthPlaylists {

    public static class PlaylistMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitLine = value.toString().split("\t", 2);
            if (splitLine.length != 2) {
                return;
            }
            
            String playlistID = splitLine[0];
            String[] songs = splitLine[1].split(";>:");
            int itemCount = songs.length;

            if (itemCount == 10) {
                context.write(value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ten length playlists");
        job.setJarByClass(TenLengthPlaylists.class);
        job.setMapperClass(PlaylistMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0); // No reducer needed in this case
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}