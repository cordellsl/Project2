package com.revature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.GradMapper;
import com.revature.reducer.GradReducer;


public class CountryWomenGrad {
	/**
	 * <b>MapReduce for Women's Graduation Rate Globally</b> <br>
	 * ________________________________________________________ <br>
	 * <br>
	 * 
	 * Using MapReduce, find the most recent years for non-null values for 
	 * gross graduation ratio for females in tertiary education up to five years combined. 
	 * Take these percentages and find the average by dividing the sum 
	 * of the percentages by the number of percentages (years accounted for). 
	 * Finally, the countries with the final aggregate percentages less than 30% 
	 * are filtered out of the final results.
	 *
	 * @param args the arguments
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws ClassNotFoundException the class not found exception
	 * @throws InterruptedException the interrupted exception
	 */

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		if(args.length != 2) {
			System.out.println("Graduation Rate: <inputdir> <output dir>" );
			System.exit(-1);
		}

		Job job = new Job();

		// Telling job which class is driver
		job.setJarByClass(CountryWomenGrad.class);
		job.setJobName("CountryWomenGrad");
		
		// Input Output Paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Specify Mapper and Reducer Class
		job.setMapperClass(GradMapper.class);
		job.setReducerClass(GradReducer.class);
		
		// Set combiner class to be used before reducer phase
		// job.setCombinerClass(classname.class);
		
		// Set Partitioner class to be used after Map phase
		// job.setPartitionerClass(WordPartitioner.class);
		
		// Don't forget to set amount of reducers for Partitioner
		job.setNumReduceTasks(1);
		
		// Specify types of final output
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		// Check if job completed
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		
	}

}
