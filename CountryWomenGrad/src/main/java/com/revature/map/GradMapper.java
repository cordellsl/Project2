package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GradMapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {


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
