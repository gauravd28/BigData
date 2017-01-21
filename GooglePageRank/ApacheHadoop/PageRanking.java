/**
 * @author Gaurav Dhamdhere
 * Student ID : 800934991
 * 
 * PageRanking
 * 
 * Main Class which will call different MapReduce jobs
 *
 */
package application;

import org.apache.hadoop.util.ToolRunner;

import core.CalculateRank;
import postprocessing.CleanUp;
import preprocessing.LinkGraphGenerator;

public class PageRanking {
	private static final int interationCount = 10;	// Number of page rank iterations
	public static void main(String[] args) throws Exception {

	/*
	 * Following are the calls to different MapReduce jobs which perform
	 * different tasks
	 */
		String[] io = new String[2];
	/*
	 * LinkGraphGenerator finds out total number of nodes and creates out-links graph
	 */
		int res = ToolRunner.run(new LinkGraphGenerator(), args); 
		if (res == 0) {
			/*
			 * CalculateRank actually performs the Page Rank calculation.
			 * This job is run in loop for Number of Iterations (interationCount)
			 * mentioned above. For each iteration, the mapper will take output written by reducer in previous iteration.
			 * For first iteration, mapper will take output of LinkGraphGenerator which has initial rank.
			 */
			for (int i = 1; i <= interationCount; i++) {
				io[0] = args[0] + "_rank" + (i - 1);
				if(i==interationCount)
					io[1] = args[0] + "_rankFinal"; // The directory for final page rank file
				else
					io[1] = args[0] + "_rank" + i; // For each page rank new directory is created
				res = ToolRunner.run(new CalculateRank(), io);
			}
			if(res == 0){
				/*
				 * CleanUp, as name suggests, cleans up the output i.e.
				 * Sorts the pages list in descending order of the ranks and outputs
				 * top 100 results. Also removes intermediate files
				 */
				io[0] = args[0];
				io[1] = args[1];
				res = ToolRunner.run(new CleanUp(), io);
			}
			

		}
		System.exit(res);

	}
}
