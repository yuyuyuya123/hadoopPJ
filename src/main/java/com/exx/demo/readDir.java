package com.exx.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class readDir {
	public static void ShowPath(FileSystem hdfs, Path path) {
		try {

			if (hdfs == null || path == null) {
				return;
			}
			// 获取文件列表
			FileStatus[] files = hdfs.listStatus(path);
			// 展示文件信息
			for (int i = 0; i < files.length; i++) {
				try {
					if (files[i].isDirectory()) {
						System.out.println(">>>" + files[i].getPath());
						// 递归调用
						ShowPath(hdfs, files[i].getPath());
					} else if (files[i].isFile()) {
						System.out.println("   " + files[i].getPath() + " ,length:" + files[i].getLen());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
public static void main(String[] args) {
		
		//创建配置对象
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
		conf.set("dfs.replication"," 1");
		FileSystem fs=null;
		
		try {
			//通过配置对象获取文件系统
			fs = FileSystem.get(conf);
			//读取 从根目录开始的 所有目录和文件，并输出文件列表
			ShowPath(fs,new Path("/bdback"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			if(fs != null)
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
			
	}

}
