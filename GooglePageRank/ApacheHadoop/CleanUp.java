/**
 * @author Gaurav Dhamdhere
 * Student ID: 800934991
 * 
 * CleanUp
 * 
 * This is a clean up map reduce pass
 * Removes the Target URL lists from the final page rank output
 * and writes top 100 URLs with highest page ranks.
 * Also, cleans up intermediate directories
 * 
 */
package postprocessing;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class CleanUp extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), " clean_up ");
		job.setJarByClass(this.getClass());
		
		FileSystem fs = FileSystem.get(getConf()); // Create FileSystem Object to delete intermediate folders
		

		FileInputFormat.addInputPaths(job, args[0]+"_rankFinal"); // Path for finally calculated ranks
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Path for sorted top 100 wiki pages
		job.setMapperClass(CleanUpMap.class);
		job.setReducerClass(CleanUpReduce.class);
		job.setMapOutputKeyClass(DoubleWritable.class); // Class for Mapper Output Key
		job.setMapOutputValueClass(Text.class); // Class for Mapper Output Value
		job.setOutputKeyClass(Text.class); // Class for Reducer Output Key
		job.setOutputValueClass(DoubleWritable.class); // Class for Reducer Output Value

		job.setSortComparatorClass(DescendingSort.class);	// Custom Sorter to sort in descending order
		
		//Following line is to stop generating _SUCCESS file
		job.getConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		
		job.setNumReduceTasks(1); // Enforce single reducer to get proper sorted output
		
		int status= job.waitForCompletion(true) ? 0 : 1;
		
		Path delPath = new Path(args[0]+"_temp"); // Deleting Number count output folder
		if(fs.exists(delPath))
			fs.delete(delPath,true);
		
		delPath = new Path(args[0]+"_rankFinal");// Deleting Final rank folder
		if(fs.exists(delPath))
			fs.delete(delPath, true);
		
		for(int i =0;i<10;i++){				// Deleting intermediate rank folders
			delPath = new Path(args[0]+"_rank"+i); 
			if(fs.exists(delPath))
				fs.delete(delPath, true);
		}
		
		return status;
	}

	/*
	 * Mapper for sorting the page rank output
	 * Output: Key - Pagerank value, Value - URL
	 * Here we are writing Key= Pagerank, so that the custom sorter can sort the page ranks in descending order
	 */
	public static class CleanUpMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {
		

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String value = lineText.toString().split("#&#&SEP#&#&")[0];
			String val = (lineText.toString().split("#&#&SEP#&#&")[1]).split("#&#&RSEP#&#&")[0];

			double key = Double.parseDouble(val.trim());

			context.write(new DoubleWritable(key), new Text(value)); // Output (Key, Value) as (PageRank,URL)

		}
	}

	/*
	 * Reducer for sorting page rank output
	 * Output: Key - URL, Value - PageRank
	 */
	public static class CleanUpReduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		@Override
		public void reduce(DoubleWritable word, Iterable<Text> inlinks, Context context)
				throws IOException, InterruptedException {

			for (Text node : inlinks) {
				
					context.write(node, word);								// Write URL and PageRank in output file
					
				
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
