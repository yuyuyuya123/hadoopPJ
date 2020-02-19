package scoreAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>{

	FloatWritable maxScore=new FloatWritable();
	
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text,FloatWritable,Text,FloatWritable>.Context context)
			throws IOException, InterruptedException{
		//求最大值
		float max=0;
		for (FloatWritable val:values) {
			if(val.get()>max){
				max=val.get();
			}
		}
		maxScore.set(max);
		context.write(key, maxScore);
	}
		
		
}
	
	

