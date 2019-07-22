package CategoryTrend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Q1Reducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {

		// create a map to remember the owner frequency
		// keyed on owner id
		Map<String, Integer> uniqueStr = new HashMap<String,Integer>();
		Map<String, Integer> uniqueID = new HashMap<String,Integer>();
		double countStr = 0.0;
		double countID = 0.0;

		for (Text val: values){
			String dataString = val.toString();
			String videoID = val.toString().split(",")[1];
			if (!uniqueStr.containsKey(dataString)) 
			{
				uniqueStr.put(dataString, 1);
				countStr++;
			}
			else{}
			if (!uniqueID.containsKey(videoID)) 
			{
				uniqueID.put(videoID, 1);
				countID++;	
			}
			else{}	
			}
		double avg = countStr/countID;
		avg = (double)Math.round(avg*100)/100;
		context.write(key, new Text(String.format("%.2f",  avg)));
	}
}


