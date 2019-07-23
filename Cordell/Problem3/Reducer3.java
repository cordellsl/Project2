package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer3 extends Reducer <Text, Text, Text, DoubleWritable>{
//public class Reducer3 extends Reducer <Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
//		Australia	67.4400024414063
//		Australia	66.7600021362305
//		Austria	65.620002746582
//		Austria	62.3600006103516
		
//		Central Europe and the Baltics	57.2518687703067  2000
//		Central Europe and the Baltics	59.4969993433222  NEW
//		Euro area	60.1061352481701
//		Euro area	56.5259243797595  NEW
		
		
		List<Text> years = new ArrayList<>();
		
		for(Text i : values) {
			years.add(i);
		}
		
		String string = years.get(0).toString();
		String[] years2 = string.split(",");
		Double year2000 = Double.parseDouble(years2[0]);
		Double yearNew = Double.parseDouble(years2[1]);
		
		//double diff = (((yearNew / year2000) - 1)*100);
		
		Double diff = ((( yearNew - year2000 ) /year2000 ) *100 );
		
		context.write( new Text(key), new DoubleWritable( diff ));
	}

}
