package CategoryTrend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q1Driver {
	public static void main(String[] args) throws Exception {
		////get Configuration: core-default.xml, core-site.xml
		Configuration conf = new Configuration();
		//get input parameter 
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Q1Driver <in> <out>");
			System.exit(2);
		}

		//set job attribute
		Job job = new Job(conf, "tag owner inverted list");
		job.setNumReduceTasks(3);
		job.setJarByClass(Q1Driver.class);
		job.setMapperClass(Q1Mapper.class);
		job.setReducerClass(Q1Reducer.class);
		//reducer output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//input path	
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		//output path should be empty,
		//otherwise occur error: org.apache.hadoop.mapred.FileAlreadyExistsException
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
