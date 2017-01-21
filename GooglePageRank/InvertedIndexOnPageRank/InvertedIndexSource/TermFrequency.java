/**
 * @author Gaurav Dhamdhere
 * Student ID: 800934991
 * 
 * TermFrequency
 * 
 * Calculates the logarithmic term frequency for words found inside Text tag of documents
 */
package tfidf;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TermFrequency extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " termfrequency ");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, args[0]); // Iput path for the HTML file
		FileOutputFormat.setOutputPath(job, new Path(args[0]+"_tf")); // Output path to store logarithmic term frequencies
		job.setMapperClass(TFMap.class);
		job.setReducerClass(TFReduce.class);
		job.setMapOutputKeyClass(Text.class); // Class for Mapper Output Key
		job.setMapOutputValueClass(IntWritable.class); // Class for Mapper Output Value
		job.setOutputKeyClass(Text.class); // Class for Reducer Output Key
		job.setOutputValueClass(DoubleWritable.class); // Class for Reducer Output Value

		int status = job.waitForCompletion(true) ? 0 : 1;
		long nodeCount = job.getCounters().findCounter("Val", "cntVal").getValue();
		System.out.println("Number of Files found--------" + nodeCount); // get total file count
		String[] parameters = {args[0],""+nodeCount};
		
		status = ToolRunner.run(new TFIDF(), parameters); //Call TFIDF MR job by sending file count to as parameter
		return status;
		
			
	}

	/*
	 * Mapper to calculate logarithmic term frequency for query terms
	 * Input: Wiki File
	 * Output: Key - Word and Filename, Value - 1
	 * 
	 */

	public static class TFMap extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();
			Pattern p1 = Pattern.compile("<title>(.*?)</title>"); // Pattern to search for Filename
			Pattern p2 = Pattern.compile("<text(.*?)</text>"); // Pattern to search for Words

			Matcher m1 = p1.matcher(line.trim()); // Filename pattern set to a matcher
			Matcher m2 = p2.matcher(line.trim());// Words pattern set to matcher

			String titlePage = null;
			if (m1.find()) {
				titlePage = m1.group().trim();
				titlePage = titlePage.substring(7, titlePage.length() - 8); // Find Filename between <title> tags
				context.getCounter("Val", "cntVal").increment(1); // Increment Counter. This is to count the number of Files
			} else {
				return; // If does not find file page, return
			}
			String text = "";
			int found = 0;
			if (m2.find()) {
				found = 1;
				text = m2.group().trim();
				text = text.substring(text.indexOf('>') + 1, text.length() - 7); // Find words between <text> tags
				text.replace("[[", "").replace("]]", ""); // Remove unwanted square brackets
				text = StringEscapeUtils.unescapeHtml(text); // Convert HTML characters like &quot, &lt, &gt etc. into Tags
				text = text.replaceAll("\\<.*?>",""); // Strip of HTML tags to get pure words
			}
			
			// If a word is found, write map output as (Word and Filename, 1).
			if (found == 1) {
				for (String word : text.split(" ")) { 
					context.write(new Text(word + "##&&RSEP&&##" + titlePage), new IntWritable(1));
				}
			}

		}
	}

	/*
	 * Reducer: Collects the tokens from mapper and calculates logarithmic term frequency
	 * Formula used : 1 + log10(TermFrequency in the file)
	 * Output: Key - Word and Filename , Value - Logarithmic term Frequency
	 * 
	 */

	public static class TFReduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			// Get total count of each word's occurence in a particular file
			if (!word.toString().trim().isEmpty() || !word.toString().trim().equals(" ")) {
				for (IntWritable count : counts) {
					sum += count.get();
				}

				/*
				 * Calculation for Logarithmic Term Frequency
				 */
				double logarithmicSum = 1.0 + Math.log10(sum);
				//Write (Key, Value) as (Word and File, Logarithmic Term Frequency)
				context.write(word, new DoubleWritable(logarithmicSum)); 
			}
		}
	}
}
