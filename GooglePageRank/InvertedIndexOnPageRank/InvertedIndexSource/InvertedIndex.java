/**
 * @author Gaurav Dhamdhere
 * Student ID: 800934991
 * 
 * InvertedIndex
 * 
 * This is a main class which gives call to different map reduce jobs which create
 * inverted index and retrieve pages matching the query keywords, ranked in descending
 * order of their scores.
 */
package application;

import org.apache.hadoop.util.ToolRunner;

import search.Search_Rank;
import tfidf.TermFrequency;

public class InvertedIndex {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TermFrequency(), args); // Find Term Frequency
		if (res == 0) {
			res = ToolRunner.run(new Search_Rank(), args); // Search and Rank matching documents based on the query
		}
		System.exit(res);

	}

}
