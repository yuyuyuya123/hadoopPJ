package com.exx.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class exam {

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
	
	public static void ReadFile(FileSystem hdfs,File localFile,Path localPath,Configuration conf) throws IOException{

		if (hdfs == null || localPath == null) {
			return;
		}
		
		//通过流进行操作
		FileInputStream fin=new FileInputStream(localFile);
		final float fileSize=fin.available()/65536;
		
		System.out.println(">>文件上传开始");
		FSDataOutputStream fout=hdfs.create(localPath,new Progressable() {
			//pro_before:前一个进度值    pro_after：后一个进度值
			long count=0;
			int pro_before=0;
			int pro_after=0;
			public void progress() {
				if(count/fileSize<=1){
					count++;
				}
				
				pro_after=(int)((count*1.0/fileSize)*100);
				
				if(pro_after!=pro_before){
					System.out.println("总进度:"+pro_after+"%");
				}
				
				pro_before=pro_after;
			}
		});
		IOUtils.copyBytes(fin, fout, conf);
		IOUtils.closeStream(fout);
		IOUtils.closeStream(fin);
		
		System.out.println(">>文件传输完毕！");
	}


	public static void main(String[] args) {
		
		//创建配置对象
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.0.106:9000");
		conf.set("dfs.replication"," 1");
		FileSystem fs=null;
		
		try {
			//通过配置对象获取文件系统
			fs = FileSystem.get(conf);
			//输出交互提示信息
			System.out.println("请选择操作（输入操作的提示数字）："+"\n"+"1.查看文件列表"+"\n"+"2.上传一个文件");
			Scanner sc = new Scanner(System.in);
			int i=sc.nextInt();
			
			if(i==1){
				//1：读取 从根目录开始的 所有目录和文件，并输出文件列表
				ShowPath(fs,new Path("/"));
			}else if(i==2){
				//2：根据用户输入的  本地文件路径  和  将上传的路径  进行文件上传
				System.out.println("请输入要上传的文件所在本地路径：");
				File localFile=new File(sc.next());
				System.out.println("请输入要上传的位置所在路径：");
				Path remotePath=new Path(sc.next());
				if(localFile.exists()){
					ReadFile(fs,localFile,remotePath,conf);
				}else{
					//若将要上传的文件不存在，则输出提示
					System.out.println("此文件不存在，请确认路径！");
				}
				
			}else{
				//其余非法输入不予处理
				System.out.println("输入不合法，请重新输入！");
			}
			
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
