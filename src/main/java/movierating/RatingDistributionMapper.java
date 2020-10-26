package movierating;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RatingDistributionMapper extends Mapper<Object,Text,Text,IntWritable>{
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		context.getCounter("lines", "count").increment(1);
		String words[] = value.toString().split("\\s+");
		context.write(new Text(words[2]), new IntWritable(1));
	}
}