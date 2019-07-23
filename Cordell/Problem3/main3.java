package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Mapper1;
import com.revature.map.Mapper3;
import com.revature.reduce.Reducer1;
import com.revature.reduce.Reducer3;

public class main {
	
	public static void main(String[] args) throws Exception {
		
		if(args.length != 2) {
			System.out.println("WordCount usage: <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		// Telling the job which class is the driver
		job.setJarByClass(main.class);
		
		//job.setJobName("Problem 1");
		job.setJobName("Problem 3");
		
		// Input and output paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		// Specify the Mapper and Reducer class 
		//job.setMapperClass(Mapper1.class);
		job.setMapperClass(Mapper3.class);
		
		//job.setReducerClass(Reducer1.class);
		job.setReducerClass(Reducer3.class);
		
		//job.setCombinerClass(Combiner1.class);
		
		job.setNumReduceTasks(1);
		//job.setNumReduceTasks(0);
		
		// Specify the types of the final output
		job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(DoubleWritable.class);
		
		job.setOutputValueClass(Text.class);
		
		
		
		// Check if job completed
		boolean success = job.waitForCompletion(true);

		System.exit(success ? 0 : 1);
		
	}

}
