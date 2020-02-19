package scoreAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ScoreMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
	private Text examid = new Text();
	private FloatWritable score = new FloatWritable();
	
	@Override
	protected void map(LongWritable key1, Text value1, Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		String[] strs = value1.toString().split(",");
		examid.set(strs[0]);
		for(int i=3; i<=5; i++) {
			score.set(Float.parseFloat(strs[i]));
			context.write(examid, score);
		}
	}
}
