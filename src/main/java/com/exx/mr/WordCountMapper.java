package com.exx.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class WordCountMapper {

	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,LongWritable>
	{
		private final static LongWritable one=new LongWritable(1);
		private Text word=new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter arg3)
				throws IOException {
			String line=value.toString();
			StringTokenizer tokenizer=new StringTokenizer(line);
			
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
		
	}

}
