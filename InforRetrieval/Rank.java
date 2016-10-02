


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
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import java.io.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Rank extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Rank.class);

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Rank(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " search ");
		job.setJarByClass(this.getClass());

		String str = "";
		for (int i = 3; i < args.length; i++) {
			if (i == 3)
				str = args[i];
			else
				str = str + "&" + args[i];
		}

		job.getConfiguration().setStrings("cmd_arguments", str.split("&"));

		

		NLineInputFormat.addInputPaths(job, args[0]);
		job.setInputFormatClass(NLineInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		
		int status = job.waitForCompletion(true) ? 0 : 1;
		if (status == 1)
			return 1;
		else {
		
		Job job1 = Job.getInstance(getConf(), " rank ");
		job1.setJarByClass(this.getClass());
		
		NLineInputFormat.addInputPaths(job1, args[1]);
		job1.setInputFormatClass(NLineInputFormat.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setMapOutputKeyClass(DoubleWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		job1.setSortComparatorClass(DescendingSort.class);	//Custom class used for descending sort

		return job1.waitForCompletion(true) ? 0 : 1;
		}
	}

	/*
	 *  Map: Take words and TFIDF scores from the input data, match the user query with the words
	 *  and outputs matching key/value pairs as Filename and TFIDF scores
	 */	
	
	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();
			Text currentWord = new Text();

			String key1 = line.split("&#&#&")[0];
			String fname = line.split("&#&#&")[1].split("\t")[0];
			String score = line.split("&#&#&")[1].split("\t")[1];
			currentWord = new Text(fname);
			DoubleWritable tfidfScore = new DoubleWritable(Double.parseDouble(score.trim()));

			String[] cmd_args = context.getConfiguration().getStrings("cmd_arguments");
			

			for (int i = 0; i < cmd_args.length; i++) {		//Matching search query keywords
				if (cmd_args[i].equals(key1)) {
					context.write(currentWord, tfidfScore);
					break;
				}

			}

		}
	}

/*
 * Reducer : Accumulates TFIDF scores for all matched keywords in a file, sum them up and output
 * 			key/value pairs as filename and 
 */
	
	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable count : counts) {
				sum += count.get();
			}
			context.write(word, new DoubleWritable(sum));
		}
	}


/*
 * Mapper: Takes input as Filename and Weight scores, outputs key/value pairs as Score and Filename	
 */

	public static class Map1 extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();

			String fname = line.split("\t")[0];
			String score = line.split("\t")[1];

			DoubleWritable key1 = new DoubleWritable(Double.parseDouble(score));
			Text currentWord = new Text(fname);

			context.write(key1, currentWord);

		}
	}
	
/*
 * Reducer: Gets keys as Scores in descending sorted and outputs key/value pairs as filename and weight scores	
 */

	public static class Reduce1 extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		@Override
		public void reduce(DoubleWritable score, Iterable<Text> files, Context context)
				throws IOException, InterruptedException {
			for (Text file1 : files) {
				double val = score.get();
				context.write(file1, new DoubleWritable(val));
			}

		}
	}	
}


/*
 * Class for sorting the keys in Descending order
 */
class DescendingSort extends WritableComparator {

	protected DescendingSort() {
		super(FloatWritable.class, true);

	}

	@SuppressWarnings("rawtypes")

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FloatWritable k1 = (FloatWritable) w1;
		FloatWritable k2 = (FloatWritable) w2;

		return -1 * k1.compareTo(k2);

	}

}

