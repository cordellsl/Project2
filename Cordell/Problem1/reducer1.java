package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {

	// Will find the averages of each percent of each country and return the countries
	// with women's graduation rate less than 30% 
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException {
	
		int MAX_PERCENT = 1;
		int nonNull = 0;
		//double weighted = 0;
		List<Double> nums = new ArrayList<>();
		//Text prevCountry = key;
		
		// if countries are the same across year
		//do{
			// Goes through list and counts non null values
			for(DoubleWritable value : values) {
				if(values != null) {
					//System.out.println("Country: " + key);
					//System.out.println("Value: " + value);
					nums.add(value.get());
					nonNull++;
				}
			}
			// weighted for each nation
			//weighted = (double)MAX_PERCENT/(double)nonNull;
			
			//System.out.println("Weighted: " + weighted);
			double avg = calcAverage(nums);
			if (avg < 30.00)
			{
				System.out.println("Country: " + key);
				System.out.println("Average: "+ avg );
				context.write(new Text(key), new DoubleWritable(avg));
			}
				
			
			
		//}while(key == prevCountry); // As long as the prevCountry is the same as the current Country
		
		// NOTE: THIS MIGHT ACTUALLY NOT WORK, NOT TESTED
		// STILL NEED TO KEEP TRACK OF COUNTRIES AND THERE RESPECTIVE VALUES
	}

	
	public double calcWeightedAverage(List<Double> nums, double weight)
	{
		System.out.println();
		//final number to divide by
		final int AVERAGE_DENOMINATOR = 5;
		//sum of weighted numbers
		double weightedSum = 0.0;
		//iterate through nums and multiply by weight, then add them together
		for (double num : nums)
		{
			//multiply number times weight and add it
			weightedSum += (num * weight);
			System.out.println("num is:  " + num);
			
		}
		//calc average and return
		return weightedSum/AVERAGE_DENOMINATOR;
		
	}
	public double calcAverage(List<Double> nums)
	{
		//final int AVERAGE_DENOMINATOR = 5;
		final int AVERAGE_DENOMINATOR = nums.size();
		double sum = 0.0;
		for(double num : nums)
		{
			sum += num;
		}
		return sum/AVERAGE_DENOMINATOR;
	}
}
