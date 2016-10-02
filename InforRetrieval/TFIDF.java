


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
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import java.util.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;

public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new TFIDF(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " tfidf ");
		job.setJarByClass(this.getClass());

		FileSystem fs = FileSystem.get(getConf());
		Path pt = new Path(args[0]);
		ContentSummary cs = fs.getContentSummary(pt);
		long fileCount = cs.getFileCount();

		getConf().set("docNumber", "" + fileCount);

		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class); 			// Class for Mapper Output Key
		job.setMapOutputValueClass(IntWritable.class); 	// Class for Mapper Output Value 
		job.setOutputKeyClass(Text.class); 				// Class for Reducer Output Key
		job.setOutputValueClass(DoubleWritable.class);	// Class for Reducer Output Value
														

		int status = job.waitForCompletion(true) ? 0 : 1;
		if (status == 1)
			return 1;
		else {
			/*
			 * Second Map Reduce job for 
			 */
			Job job1 = Job.getInstance(getConf(), " termfrequency ");
			job1.setJarByClass(this.getClass());
			job1.setInputFormatClass(NLineInputFormat.class);
			NLineInputFormat.addInputPaths(job1, args[1]);

			FileOutputFormat.setOutputPath(job1, new Path(args[2]));
			job1.setMapperClass(Map1.class);
			job1.setReducerClass(Reduce1.class);
			job1.setMapOutputKeyClass(Text.class); 			// Class for Mapper Output  Key
			job1.setMapOutputValueClass(Text.class); 		// Class for Mapper Output Value
			job1.setOutputKeyClass(Text.class); 			// Class for Reducer Output Key
			job1.setOutputValueClass(DoubleWritable.class);	// Class for Reducer Output Value

			return job1.waitForCompletion(true) ? 0 : 1;

		}
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
			
			double logarithmicSum = 1.0 + Math.log10(sum);
			context.write(word, new DoubleWritable(logarithmicSum));
			
		}
	}

	/*
	 *  Map: Takes term frequencies as input and outputs key/value pairs as Token and 'Filename=TermFrequency'	
	 */
	
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {


		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();
			Text currentWord = new Text();

			String key1 = line.split("#####")[0];
			String value1 = line.split("#####")[1].split("\t")[0];
			String value2 = line.split("#####")[1].split("\t")[1];
			currentWord = new Text(key1);
			Text valueWord = new Text(value1 + "=" + value2);

			context.write(currentWord, valueWord);

		}
	}

	/*
	 * Reducer: Collects the TermFrequencies from mapper in sorted order and calculates IDF and TFIDF.
	 * 			Outputs key/value pairs as Token#Filename and TFIDF score.
	 */		
	public static class Reduce1 extends Reducer<Text, Text, Text, DoubleWritable> {

		@Override
		public void reduce(Text word, Iterable<Text> postings, Context context)
				throws IOException, InterruptedException {
			long fileCount = context.getConfiguration().getLong("docNumber", 1);	// Number of input files
			String fname, tfscore;
			int i = 0;
			ArrayList<Text> cache = new ArrayList<Text>();
			for (Text term : postings) {
				i++;
				Text cache_obj = new Text(term);
				cache.add(cache_obj);
			}

			double logarithmicSum = Math.log10(1 + (fileCount / i));	// IDF Calculation

			for (Text term : cache) {
				fname = term.toString().split("=")[0];
				tfscore = term.toString().split("=")[1];
				double tf_double = Double.parseDouble(tfscore);
				double tf_idf = tf_double * logarithmicSum;				//TF-IDF Calculation		
				Text newKey = new Text(word.toString() + "#####" + fname);

				context.write(newKey, new DoubleWritable(tf_idf));
			}

		}
	}

}
