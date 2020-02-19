package scoreAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ScoreDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(ScoreDriver.class);
		
		job.setMapperClass(ScoreMapper.class);
		job.setReducerClass(ScoreReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.43.50:9000/input/score0218.csv"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.43.50:9000/output/result0218"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
