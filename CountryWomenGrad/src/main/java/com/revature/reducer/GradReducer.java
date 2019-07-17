package com.revature.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GradReducer extends Reducer <LongWritable, Text, Text, LongWritable> {

}
