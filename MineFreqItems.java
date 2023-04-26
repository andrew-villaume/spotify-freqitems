
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


//comment: make sure you go through this code and understand it
public class MineFreqItems extends Configured implements Tool {
	public static class TokenizerMapperA extends Mapper<Object, Text, Text, IntWritable> {
		private Text node1;
		private IntWritable zero = new IntWritable(0);

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//Write movie ID and int writeable 1
			node1 = new Text(value.toString());
			context.write(node1, zero);
		}
	}
	public static class TokenizerMapperB extends Mapper<Object, Text, Text, IntWritable> {
		private Text node1;
		private IntWritable one = new IntWritable(1);

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			node1 = new Text(value.toString());
			context.write(node1, one);
		}
	}

	public static class CountReducer extends Reducer<Text, IntWritable, Text, NullWritable>{
		private Text node1;
		List<Integer> array;

		@Override
		protected void setup(Context context) {
			array = new ArrayList<Integer>();
		 }

		 @Override
		 protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
			//Add all action movie IDs to unique set distinctNodes
			String keyString = key.toString().strip();
			double fiveStarCounter = 0;

			//Boolean to tell if the movie has been logged
			boolean canWrite = false;

				for (IntWritable text : values) {
					int value = text.get();

					if (value == 0) {
						canWrite = true;
					}
					else if (value == 1) {
						fiveStarCounter++;
					 }
				}


				
				if (fiveStarCounter >= 500 && canWrite) {
					array.add(Integer.parseInt(keyString));

				}

		 }

		 @Override
		 protected void cleanup(Context context) throws IOException, InterruptedException {
			Collections.sort(array);

			for (Integer s : array) {
				node1 = new Text(Integer.toString(s));
				context.write(node1, NullWritable.get());
			}
		 }
	}

	public static void main(String[] args) throws Exception { 

		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN 
		int res = ToolRunner.run(new Configuration(), new MineFreqItems(), args);
        System.exit(res);
    }

    @Override
   	public int run(String[] args) throws Exception {
   		//Configuration conf = new Configuration();
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "500+ 5 star reviewed movies"); 
		job.setJarByClass(MineFreqItems.class); 
		job.setMapperClass(MineFreqItems.TokenizerMapperA.class); 
		job.setReducerClass(MineFreqItems.CountReducer.class); 
		job.setNumReduceTasks(1); 
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(IntWritable.class); 
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(NullWritable.class); 

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,  TokenizerMapperA.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,  TokenizerMapperB.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class,  TokenizerMapperB.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[3])); 
		return job.waitForCompletion(true) ? 0 : 1;
   	}
}