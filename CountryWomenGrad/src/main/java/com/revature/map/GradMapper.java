package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GradMapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {
	
	/**
	 * <h3>Mapper Functionality</h3>
	 * _____________________________
	 * <p>
	 * Outputs a country name, percentage pairing for each year instance of the percentage
	 * of women moving on to tertiary education; represented in
	 * the row with the Indicator Code column value "SE.TER.CMPL.FE.ZS".
	 * <p>
	 * Output format: [Text: Country Name, DoubleWritable: Percentage from Particular Year]
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
		int count;
		
		for(String i : columns) {
			if(i.equals("SE.TER.CMPL.FE.ZS")) {
				count = 0;
				
				
				for(int j=columns.length-2; j >= 4; j--) {
					if(count == 5) { break; }
					if(!(columns[j].equals(""))) {
						count ++;
						context.write(new Text(columns[0]), new DoubleWritable( Double.parseDouble(columns[j]) ));
					}
				}
			}
		}
	}
}
