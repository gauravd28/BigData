/**
 * @author Gaurav Dhamdhere
 * Student ID: 800934991
 * 
 * CalculateRank
 * 
 * This class calculates the Page Rank of all pages based on the contributions of page ranks they get from their in-links
 * 
 * Page Rank formula used is-
 * 
 * PR(A) = (1-d) + d*(PR(B)/|B| + PR(C)/|C| + ...) where B, C .... are in-links for A and d is damping factor = 0.85
 * 
 */
package core;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class CalculateRank extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " calc_rank ");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, args[0]); // Path of previous ranks
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Path for storing new ranks
		job.setMapperClass(CalculateRankMap.class);
		job.setReducerClass(CalculateRankReduce.class);
		job.setMapOutputKeyClass(Text.class); // Class for Mapper Output Key
		job.setMapOutputValueClass(Text.class); // Class for Mapper
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set("mapred.textoutputformat.separator", "#&#&SEP#&#&"); // Customized Key-Value seperator
		job.setOutputKeyClass(Text.class); // Class for Reducer Output Key
		job.setOutputValueClass(Text.class); // Class for Reducer Output Value
		
		//Following line is to stop generating _SUCCESS file
		job.getConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		
		return job.waitForCompletion(true) ? 0 : 1;

	}
	
	/*
	 * Mapper to calculate the page rank. Takes link graph and previous page rank as input.
	 * Outputs- 1. Key - Target URL, Value - Contribution of Rank received to it from Source URL
	 * 			2. Key - Source URL, Value - List of target URLs
	 */

	public static class CalculateRankMap extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			double rank =0.0; // Variable in which previous rank is read
			
			String urlList = ""; // Variable in which list of URLs is read
			
			
			String[] line = lineText.toString().split("#&#&SEP#&#&");
			String currentNode = line[0];				// Source URL
			line = line[1].split("#&#&RSEP#&#&");		
			rank = Double.parseDouble(line[0]);			// Previous Rank of source URL
			
			
			
			if (line.length>1) {		// To check if Target URL list is present or its empty

				urlList = line[1];		
				String[] urls = urlList.split("###&&&&&&###");  // Target URLs array
				double newRank = rank / urls.length;		 	// Rank contribution for each target link

				for (String url : urls) {
					context.write(new Text(url), new Text("" + newRank));	// Output Target URL and rank contribution given to it
				}
			}

			
				context.write(new Text(currentNode), new Text(urlList));	// Output Source URL and Target URL list

		}
	}

	
	/*
	 * Reducer for calculating Page Rank.
	 * Output: Key - Source URL , Value - Initial rank and list of target URLs
	 */
	public static class CalculateRankReduce extends Reducer<Text, Text, Text, Text> {

	
		@Override
		public void reduce(Text word, Iterable<Text> links, Context context) throws IOException, InterruptedException {

			double newR = 0.0;
			String pageLinks = "";
			

			for (Text link : links) {
				
				String str = link.toString();	

				
				if (str.contains("###&&&&&&###")) { // Check if the input (Key, Value) is (Source URL, List of Target URLs)
					pageLinks	 = str;
				} else if(!str.equals("")){			// Else if Empty target link
					newR = newR + Double.parseDouble(str);;	// Adding all rank contribution

				}

			}

			double finalRank = 0.15 + 0.85*newR; // Embedding the Damping Factor of 0.85 in the equation
			
			// Output source URL with new page rank and list of target URLs
			context.write(word, new Text("" + finalRank + "#&#&RSEP#&#&" + pageLinks)); 

			
			
		}
	}

}
