package com.revature.test;

import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


// Import map() and reduce() from com.revature.x
import com.revature.map.Mapper1;
import com.revature.reduce.Reducer1;

public class Test1 {
	
	// Initializing a MapDriver with Input and Output values of Problem 1
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		
		Mapper1 mapper = new Mapper1();  // Creating instance of our map class
		mapDriver = new MapDriver<>();   // Creating instance of MapDriver
		mapDriver.setMapper(mapper);     // Setting MapDriver to execute our map()
		
		// Set up Reducer
		Reducer1 reducer = new Reducer1();  // Creating instance of our reduce class
		reduceDriver = new ReduceDriver<>();// Creating instance of ReduceDriver
		reduceDriver.setReducer(reducer);   // Setting ReducerDriver to execute our reduce()
		
		// Set up MapReducer
		mapReduceDriver = new MapReduceDriver<>(); // Create instance of mapReduceDriver
		mapReduceDriver.setMapper(mapper);         // Give mapReduceDriver our map()
		mapReduceDriver.setReducer(reducer);       // Give mapReduceDriver our reduce()
	}
	
	@Test
	public void testMapper() {
		// map() will take a whole line from the table
		String text = "\"Albania\",\"ALB\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.39418\",\"\",\"\",\"15.0017\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"39.70473\",\"48.23856\",\"51.73209\",\"43.19789\",\"47.20538\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(text));
		
		
		mapDriver.withOutput(new Text("Albania"), new DoubleWritable(47.20538));
		mapDriver.withOutput(new Text("Albania"), new DoubleWritable(43.19789));
		mapDriver.withOutput(new Text("Albania"), new DoubleWritable(51.73209));
		mapDriver.withOutput(new Text("Albania"), new DoubleWritable(48.23856));
		mapDriver.withOutput(new Text("Albania"), new DoubleWritable(39.70473));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		// Three inputs of 25%
		reduceDriver.withInput(new Text("Albania"), Arrays.asList(new DoubleWritable(25), new DoubleWritable(25), new DoubleWritable(25)));
		
		// Average should be 25%
		reduceDriver.withOutput(new Text("Albania"), new DoubleWritable(25));
		
		reduceDriver.runTest();
	}
	@Test
	public void testReducer2() {
		// Three inputs all above 30%
		reduceDriver.withInput(new Text("Albania"), Arrays.asList(new DoubleWritable(41), new DoubleWritable(32), new DoubleWritable(62)));
		
		// No output because average would be above 30%
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		// map() will take a whole line from the table
		String text = "\"Albania\",\"ALB\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.39418\",\"\",\"\",\"15.0017\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"39.7\",\"48.2\",\"51.7\",\"43.1\",\"47.2\",\"\",";
		mapReduceDriver.withInput(new LongWritable(1), new Text(text));
		
		// Average would be above 30% (~45), so there is no output
		
		mapReduceDriver.runTest();
	}
	@Test
	public void testMapReduce2() {
		// map() will take a whole line from the table
		String text = "\"Albania\",\"ALB\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.39418\",\"\",\"\",\"15.0017\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"20.9\",\"30\",\"10.4\",\"15.2\",\"66\",\"\",";
		mapReduceDriver.withInput(new LongWritable(1), new Text(text));
		
		// Average would be above 28.5%
		mapReduceDriver.addOutput(new Text("Albania"), new DoubleWritable(28.5));
		
		mapReduceDriver.runTest();
	}
	
	@After
	public void tearDown() {
		mapDriver = null;
		reduceDriver = null;
		mapReduceDriver = null;
	}

}
