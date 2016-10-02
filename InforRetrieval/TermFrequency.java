


package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TermFrequency extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TermFrequency.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TermFrequency(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " termfrequency ");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class); 			// Class for Mapper Output Key
		job.setMapOutputValueClass(IntWritable.class); 	// Class for Mapper Output Value											
		job.setOutputKeyClass(Text.class); 				// Class for Reducer Output Key
		job.setOutputValueClass(DoubleWritable.class); 	// Class for Reducer Output Value
														

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/*
	 *  Map: Forms tokens from the input data and outputs key/value pairs as Token#Filename and '1'	
	 */	
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();
			Text currentWord = new Text();

			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				/*
				 * Finding input file name and adding to Map output key
				 */
				FileSplit fSplit = (FileSplit) context.getInputSplit();
				String fname = fSplit.getPath().getName();
				word = word + "#####" + fname;
				
				currentWord = new Text(word);
				context.write(currentWord, one);
			}
		}
	}

	/*
	 * Reducer: Collects the tokens from mapper in sorted order and calculates the logarithmic term frequency.
	 * 			Outputs key/value pairs as Token#Filename and logarithmic term frequency.
	 */	
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			/*
			 * Calculation for Logarithmic Term Frequency
			 */
			double logarithmicSum = 1.0 + Math.log10(sum);
			context.write(word, new DoubleWritable(logarithmicSum));
		}
	}
}
