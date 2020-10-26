package movierating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RatingDistributionDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length < 2) {
			System.out.println("usage: RatingDistributionDriver <input-dir> <output-dir>");
			System.exit(-1);
		}
		int res = ToolRunner.run(new RatingDistributionDriver(), args);
		System.exit(res);	

	}
	
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "RatingDistribution");
		job.setJarByClass(getClass());
		
		job.setMapperClass(RatingDistributionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(RatingDistributionReducer.class);
		job.setReducerClass(RatingDistributionReducer.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}

