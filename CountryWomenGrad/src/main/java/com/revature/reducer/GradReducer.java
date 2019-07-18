package com.revature.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GradReducer extends Reducer <Text, DoubleWritable, DoubleWritable, Text> {

	// Will find the averages of each percent of each country and return the countries
	// with women's graduation rate less than 30% 
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException {
	
		int MAX_PERCENT = 100;
		int nonNull = 0;
		double weighted = 0;
		Text prevCountry = key;
		
		// if countries are the same across year
		do{
			// Goes through list and counts non null values
			for(DoubleWritable value : values) {
				if(values != null) {
					System.out.println("Country: " + key);
					System.out.println("Value: " + value);
					nonNull++;
				}
			}
			// weighted for each nation
			weighted = MAX_PERCENT/nonNull;
			System.out.println("Country: " + key);
			System.out.println("Weighted: " + weighted);
			
			
		}while(key == prevCountry); // As long as the prevCountry is the same as the current Country
		
		// NOTE: THIS MIGHT ACTUALLY NOT WORK, NOT TESTED
		// STILL NEED TO KEEP TRACK OF COUNTRIES AND THERE RESPECTIVE VALUES
	}
}
