/**
 * @author Gaurav Dhamdhere
 * Student ID: 800934991
 * 
 * Search_Rank
 * 
 * Searches documents relevant to the search query and ranks them in descending order of
 * their TFIDF scores
 * 
 * 2 Jobs - 
 * 1. Search relevant documents
 * 2. Rank them based on their TFIDF scores
 * 
 */

package search;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class Search_Rank extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " search ");
		job.setJarByClass(this.getClass());

		String str = "";
		for (int i = 2; i < args.length; i++) {	//Get keywords from search query and form its array
			if (i == 2)
				str = args[i];
			else
				str = str + "&" + args[i];
		}

		job.getConfiguration().setStrings("cmd_arguments", str.split("&")); // Set Querywords to the job configuration

		FileInputFormat.addInputPaths(job, args[0] + "_tfidf"); // Path to TFIDF scores

		FileOutputFormat.setOutputPath(job, new Path(args[0] + "_search")); // Path to store relevant documents and their TFIDF scores
		job.setMapperClass(SearchMap.class);
		job.setReducerClass(SearchReduce.class);
		job.setMapOutputKeyClass(Text.class); // Class for Mapper Output Key
		job.setMapOutputValueClass(DoubleWritable.class); // Class for Mapper Output Value
		job.setOutputKeyClass(Text.class);		// Class for Reducer Output Key
		job.setOutputValueClass(DoubleWritable.class); // Class for Reducer Output Value
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set("mapred.textoutputformat.separator", "#&#&SEP#&#&");// Customized Key-Value seperator

		int status = job.waitForCompletion(true) ? 0 : 1;
		if (status == 1)
			return 1;
		else {
			FileSystem fs = FileSystem.get(getConf()); // Create FileSystem Object to delete intermediate folders

			Job job1 = Job.getInstance(getConf(), " rank ");
			job1.setJarByClass(this.getClass());

			NLineInputFormat.addInputPaths(job1, args[0] + "_search"); // Path to relevant documents
			job1.setInputFormatClass(NLineInputFormat.class);
			FileOutputFormat.setOutputPath(job1, new Path(args[1])); // Path for ranked documents
			job1.setMapperClass(RankMap.class);
			job1.setReducerClass(RankReduce.class);
			job1.setMapOutputKeyClass(DoubleWritable.class); // Class for Mapper Output Key
			job1.setMapOutputValueClass(Text.class); // Class for Mapper Output Value
			job1.setOutputKeyClass(Text.class);  // Class for Reducer Output Key
			job1.setOutputValueClass(DoubleWritable.class); // Class for Reducer Output Value
			job1.setSortComparatorClass(DescendingSort.class); // Custom class used for descending sort
			job1.setNumReduceTasks(1);  //Enforcing single reducer
			int st = job1.waitForCompletion(true) ? 0 : 1;

			Path delPath = new Path(args[0] + "_search"); // Deleting relevant files folder
			if (fs.exists(delPath))
				fs.delete(delPath, true);

			delPath = new Path(args[0] + "_tf"); // Deleting term frequencies folder
			if (fs.exists(delPath))
				fs.delete(delPath, true);

			delPath = new Path(args[0] + "_tfidf"); // Deleting TFIDF scores
			if (fs.exists(delPath))
				fs.delete(delPath, true);

			return st;

		}
	}

	/*
	 * Mapper to relevant documents for the query
	 * Output: Key - Filename, Value - TFIDF score for the word in that document
	 */

	public static class SearchMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();

			String key1 = line.split("##&&RSEP&&##")[0]; // get word
			String fname = line.split("##&&RSEP&&##")[1].split("\t")[0]; // get filename
			String score = line.split("##&&RSEP&&##")[1].split("\t")[1]; // get tfidf score
			Text currentWord = new Text(fname);
			DoubleWritable tfidfScore = new DoubleWritable(Double.parseDouble(score.trim()));

			String[] cmd_args = context.getConfiguration().getStrings("cmd_arguments");

			for (int i = 0; i < cmd_args.length; i++) { // Matching search query keywords with word in the file
				if (key1.contains(cmd_args[i].trim())) {
					//Write (Key, Value) as (Filename, TFIDF score for matched word)
					context.write(currentWord, tfidfScore);
					break;
				}

			}

		}
	}

	/*
	 * Reducer : Accumulates TFIDF scores for all matched keywords for a file and
	 * sum them up.
	 * Output: Key - Filename, Value - TFIDF score
	 */

	public static class SearchReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable count : counts) {  //Sum all TFIDF scores for a document
				sum += count.get();
			}
			//Write (Key, value) as (Filename, Total TFIDF score)
			context.write(word, new DoubleWritable(sum));
		}
	}

	/*
	 * Mapper to sort the matched files based on their TFIDF scores
	 * Output: Key - Filename, Value - TFIDF score
	 * Custom sorter is used and single reducer is enforced
	 */

	public static class RankMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();

			String fname = line.split("#&#&SEP#&#&")[0]; // get filename
			String score = line.split("#&#&SEP#&#&")[1]; // get tfidf score

			DoubleWritable key1 = new DoubleWritable(Double.parseDouble(score));
			Text currentWord = new Text(fname);
			
			//Write (Key, Value) as (TFIDF score, Filename)
			//Here we are switching key <-> value so that custom sorter will sort based on tfidf keys
			context.write(key1, currentWord);

		}
	}

	/*
	 * Reducer: Gets keys as Scores in descending sorted order
	 * Output : Key - Filename, Value - TFIDF score
	 * 
	 */

	public static class RankReduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		@Override
		public void reduce(DoubleWritable score, Iterable<Text> files, Context context)
				throws IOException, InterruptedException {
			for (Text file1 : files) {
				double val = score.get();
				//Write (Key, Values) as (Filename, TFIDF scores)
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
