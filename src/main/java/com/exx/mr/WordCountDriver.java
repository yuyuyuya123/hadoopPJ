package com.exx.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.exx.mr.WordCountMapper.Map;
import com.exx.mr.WordCountReduce;
public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		//布置作用
		JobConf conf = new JobConf(WordCountMapper.class);
		conf.setJobName("wordcount");
		
		//在Windows上配置Hadoop home winutils所在位置
		conf.set("hadoop.home.dir", "D://hadoop/bin");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(WordCountReduce.class);
		conf.setReducerClass(WordCountReduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.0.106:9000/input/"));
		FileOutputFormat.setOutputPath(conf, new Path("hdfs://192.168.0.106:9000/output/result.txt"));
		JobClient.runJob(conf);
	}

}

































