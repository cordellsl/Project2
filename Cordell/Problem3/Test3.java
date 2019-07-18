package com.revature.test;

import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// Import Mapper and Reducer from com.revature.*
import com.revature.map.Mapper3;

public class Test3 {
	
	// Initializing a MapDriver with Input and Output values of Problem 1
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	
	//private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	
	//private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		
		Mapper3 mapper = new Mapper3();  // Creating instance of our mapper
		mapDriver = new MapDriver<>();   // Creating instance of MapDriver
		mapDriver.setMapper(mapper);     // Setting MapDriver to execute our mapper
		
		// Set up Reducer
//		SumReducer reducer = new SumReducer();
//		reduceDriver = new ReduceDriver<>();
//		reduceDriver.setReducer(reducer);
		
		// Set up MapReducer
//		mapReduceDriver = new MapReduceDriver<>();
//		mapReduceDriver.setMapper(mapper);
//		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		// map() will take a whole line from the table
		String text = "\"Albania\",\"ALB\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.39418\",\"\",\"\",\"15.0017\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"39.70473\",\"48.23856\",\"51.73209\",\"43.19789\",\"47.20538\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(text));
		
		
		mapDriver.withOutput(new Text("Albania"), new DoubleWritable(47.20538));
		mapDriver.withOutput(new Text("Albania"), new DoubleWritable(43.19789));
		
		mapDriver.runTest();
	}
	
//	@Test
//	public void testReducer() {
////		reduceDriver.withInput(new Text("cat"), Arrays.asList(new IntWritable(1), new IntWritable(1)));
////		
////		reduceDriver.withOutput(new Text("cat"), new IntWritable(2));
////		
////		reduceDriver.runTest();
//	}
//	
//	@Test
//	public void testMapReduce() {
////		mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat   dog"));
////		
////		mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
////		mapReduceDriver.addOutput(new Text("dog"), new IntWritable(1));
//	}
	
	@After
	public void tearDown() {
		mapDriver = null;
//		reduceDriver = null;
//		mapReduceDriver = null;
	}

}
