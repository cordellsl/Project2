package com.revature.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.List;


public class MaleEmpReducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {

    @Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException {

    		List<Double> number = new ArrayList<>();
    		
            for(DoubleWritable value : values) {
				if(values != null) {
					System.out.println("Reducer Country: " + key);
					System.out.println("Reducer Value: " + value);
					number.add(value.get());
				}
            }
            
            double difference = calDifference(number);
            System.out.println("Final Difference: " + difference);
    }
    
    
    	// Calculating the Difference of Male Employment
    	public double calDifference(List<Double> number) {
    		
    		System.out.println("First Value: " + number.get(0));
    		System.out.println("Second Value: " + number.get(1) );
    		
    		double difference = number.get(1) - number.get(0);
    		System.out.println("Difference: " + difference);
    		
    		return difference;
     	}
     	
}
