package movierating;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingDistributionReducer extends Reducer <Text, IntWritable, Text, IntWritable>{
	
	
	@Override
	protected void cleanup(
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}

	@Override
	protected void setup(
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		super.setup(context);
		
	}

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> iter = values.iterator();
		while(iter.hasNext()) {
			sum += iter.next().get();
		}
		context.write(key, new IntWritable(sum));
	}


}
