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
import com.revature.reduce.Reducer3;

public class Test3 {
	
	// Initializing a MapDriver with Input and Output values of Problem 1
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		
		Mapper3 mapper = new Mapper3();  // Creating instance of our mapper
		mapDriver = new MapDriver<>();   // Creating instance of MapDriver
		mapDriver.setMapper(mapper);     // Setting MapDriver to execute our mapper
		
		// Set up Reducer
		Reducer3 reducer = new Reducer3();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		// Set up MapReducer
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		// map() will take a whole line from the table
		//String text = "\"Albania\",\"ALB\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.39418\",\"\",\"\",\"15.0017\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"39.70473\",\"48.23856\",\"51.73209\",\"43.19789\",\"47.20538\",\"\",";
		String text = "\"Australia\",\"AUS\",\"Employment to population ratio, 15+, male (%) (national estimate)\",\"SL.EMP.TOTL.SP.MA.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"74.3899993896484\",\"74.370002746582\",\"74.2900009155273\",\"72.5100021362305\",\"69.2600021362305\",\"69.629997253418\",\"69.7399978637695\",\"70.0400009155273\",\"69.4899978637695\",\"70.1500015258789\",\"71.1399993896484\",\"70.5\",\"67.2600021362305\",\"65.75\",\"65.0800018310547\",\"66.2600021362305\",\"67.3600006103516\",\"67.1699981689453\",\"66.7399978637695\",\"66.9300003051758\",\"67.1600036621094\",\"67.4400024414063\",\"66.8499984741211\",\"67.0999984741211\",\"67.3300018310547\",\"67.7300033569336\",\"68.5699996948242\",\"68.870002746582\",\"69.5800018310547\",\"69.75\",\"68.1900024414063\",\"68.6900024414063\",\"68.6399993896484\",\"68.0299987792969\",\"67.3000030517578\",\"66.6800003051758\",\"66.7600021362305\",\"66.5500030517578";
		mapDriver.withInput(new LongWritable(1), new Text(text));
		
		mapDriver.withOutput(new Text("Australia"), new Text("67.4400024414063, 66.7600021362305"));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		reduceDriver.withInput(new Text("Australia"), Arrays.asList(new Text("67.4400024414063, 66.7600021362305")));
		
		reduceDriver.withOutput(new Text("Australia"), new DoubleWritable(-1.00830409335559));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testReducer2() {
		reduceDriver.withInput(new Text("Belgium"), Arrays.asList(new Text("50, 75")));
		
		reduceDriver.withOutput(new Text("Belgium"), new DoubleWritable(50));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		
		String text = "\"Australia\",\"AUS\",\"Employment to population ratio, 15+, male (%) (national estimate)\",\"SL.EMP.TOTL.SP.MA.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"74.3899993896484\",\"74.370002746582\",\"74.2900009155273\",\"72.5100021362305\",\"69.2600021362305\",\"69.629997253418\",\"69.7399978637695\",\"70.0400009155273\",\"69.4899978637695\",\"70.1500015258789\",\"71.1399993896484\",\"70.5\",\"67.2600021362305\",\"65.75\",\"65.0800018310547\",\"66.2600021362305\",\"67.3600006103516\",\"67.1699981689453\",\"66.7399978637695\",\"66.9300003051758\",\"67.1600036621094\",\"67.4400024414063\",\"66.8499984741211\",\"67.0999984741211\",\"67.3300018310547\",\"67.7300033569336\",\"68.5699996948242\",\"68.870002746582\",\"69.5800018310547\",\"69.75\",\"68.1900024414063\",\"68.6900024414063\",\"68.6399993896484\",\"68.0299987792969\",\"67.3000030517578\",\"66.6800003051758\",\"66.7600021362305\",\"66.5500030517578";
		
		mapReduceDriver.withInput(new LongWritable(1), new Text(text));
		
		mapReduceDriver.addOutput(new Text("Australia"), new DoubleWritable(-1.00830409335559));
	}
	
	@After
	public void tearDown() {
		mapDriver = null;
		reduceDriver = null;
		mapReduceDriver = null;
	}

}
