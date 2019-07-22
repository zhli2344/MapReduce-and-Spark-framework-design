package CategoryTrend;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Q1Mapper extends Mapper<Object, Text, Text, Text> {
	final String header = "video_id,trending_date,category_id,category,publish_time,views,likes,dislikes,comment_count,ratings_disabled,video_error_or_removed,country";
	private Text word = new Text(),owner = new Text();
	
	// a mechanism to filter out non ascii tags
	static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); 
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		if (value.toString().contains(header)) {
			return;
		}
		String[] dataArray = value.toString().split(","); //split the data into array
		if (dataArray.length < 12){ //  record with incomplete data
			return; // don't emit anything
		}
		String category = dataArray[3];//category
		String countryVideoId = dataArray[11]+","+dataArray[0];//country and video_id

		if (category.length() > 0){
			if (asciiEncoder.canEncode(category)){
				word.set(category);
				owner.set(countryVideoId);
				context.write(word,owner);
			}
			else{}
			}
	}
}

