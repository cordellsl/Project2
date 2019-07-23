package com.revature.map;

import java.io.IOException;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Mapper3 extends Mapper<LongWritable, Text, Text, Text>{

	/**
	 * <h3>Mapper Functionality</h3>
	 * _____________________________
	 * <p>
	 * Takes each line of the input file and splits it by comma into an array of strings.
	 * Checks for the indicator code "SL.EMP.TOTL.SP.MA.NE.ZS" in each array then finds the
	 * corresponding values for the years 2000 and the most recent year (max 2016).
	 * Outputs the country name as a Text object and the years as one divided by a comma.
	 * <p>
	 * Output format: [Text: Country Name, Text: (Year 2000 %, Most Recent Year %)]
	 *
	 * @param key <b>(LongWritable):</b> The byte offset.
	 * @param value <b>(Text):</b> Raw text of the line.
	 * @param context <b>(Context):</b> Object denoting the output of the map() function.
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws InterruptedException the interrupted exception
	 */
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// Assume getting row of table 
		String line = value.toString();
		
		String[] columns = line.split("\",\"");
		
		Double year2000;
		Double yearNew;
		
		// loop through columns of row
		for(String i : columns) {
			yearNew = null;
			year2000 = null;
			
			// if row contains % of male employment 
			if(i.equals("SL.EMP.TOTL.SP.MA.NE.ZS")) {
				//SL.EMP.TOTL.SP.FE.NE.ZS   // Same code for female
				
				// loop through the columns that contain percentages 
				for(int j=columns.length-2; j >= 44; j--) {
					
					if(yearNew == null) { // if most recent year not found yet
						if( !(columns[j].equals("") )) { // if the cell in column is not empty
							yearNew = Double.parseDouble(columns[j]);}} // make this value the % for most recent year
					
					// this j value is where the year 2000 is 
					if(j == 44) {
						if(columns[j].equals("")){ // break to the next row in the table if the year 2000 contains no data
							break; } 
						else { // grab data value in year 2000
							year2000 = Double.parseDouble(columns[j]); }}
					
					// if both years have been found, then write to context 
					if( !(year2000 == null) && !(yearNew == null) ) {
						
						String answer = year2000 + ", " + yearNew;
						context.write(new Text( columns[0].substring(1) ), new Text( answer ));}
					
					
				}
			}
		}
	}
}




