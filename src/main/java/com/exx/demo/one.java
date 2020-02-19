package com.exx.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class one {
	public static void aa(){
		Configuration conf = new Configuration(); 
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000"); 
		conf.set("dfs.replication", "1");
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem. get (conf);
			Path srcPath = new Path("D:/big_data_wp/helloHadoop.txt"); 
			Path dstPath=new Path("/user/"); 
			fileSystem.copyFromLocalFile(srcPath, dstPath); 
			fileSystem.close(); 
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void create() throws IOException{
		//1.创建配置对象
				/**
				 * fs.defaultFS:默认文件系统的名称
				 * dfs.replication:client参数，即node level个数，副本个数
				 */
				Configuration conf=new Configuration();
				conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
				conf.set("dfs.replication", "1");
				
				//2.通过配置对象获取文件系统
				/**
				 * FileSystem:通用文件系统的抽象基类，可以作为一个分布式文件系统的实现
				 */
				FileSystem fs = FileSystem.get(conf);
				
				
				//3.创建源文件和目标路径
				Path path=new Path("/test/Hello2.txt");
				
				//4.通过流进行操作
				/**
				 * OutputStreamWriter的write():61 62
				 * DataOutputStream的writeUTF():00 01 61 00 01 62
				 * 即:writeUTF()写出一个UTF-8编码的字符串前面会加上2个字节的长度标识，已标识接下来的多少个字节是属于本次方法所写入的字节数。
				 */
				FSDataOutputStream fout=fs.create(path,true);
				
				fout.writeUTF("Hello!");
				if(fs.exists(path)){
					System.out.println("文件成功创建！");
				}
				
				fout.flush();
				fout.close();
				
				//5.关闭文件系统
				fs.close();
	}
	
	public static void delete() throws IOException{
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
		conf.set("dfs.replication"," 1");
		FileSystem fs=FileSystem.get(conf);
		Path path=new Path("/test/Hello2.txt");
		if(fs.exists(path)){
			//delete()的第二个参数是 允许递归删除
			boolean b = fs.delete(path,true);
			System.out.println("执行成功："+b);
		}else{
			System.out.println("指定的文件不存在！");
		}
		fs.close();
	}
	
	public static void upLoad() throws IOException{
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
		conf.set("dfs.replication", "1");
		FileSystem fs=FileSystem.get(conf);
		Path sourcePath=new Path("D:/uu.txt");
		Path targetPath=new Path("/test/up.txt");
		fs.copyFromLocalFile(sourcePath, targetPath);
		
		if(fs.exists(targetPath)){
			System.out.println("文件已存在！");
		}
		
		fs.close();
	} 
	
	
	  public static void upLoad2() throws IOException {
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
		conf.set("dfs.replication", "1");
		FileSystem fs=FileSystem.get(conf);
		
		Path targetPath=new Path("/test/up2.txt");
		File file=new File("D:/uu.txt");
		
		FileInputStream fin=new FileInputStream(file);
		FSDataOutputStream fout=fs.create(targetPath);
		IOUtils.copyBytes(fin, fout, conf);
		
		
		if(fs.exists(targetPath)){
			System.out.println("文件已存在！");
		}
		
		IOUtils.closeStream(fin);
		IOUtils.closeStream(fout);
		fs.close();
	}
	
	
	public static void fileStatus() throws IOException{
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
		conf.set("dfs.replication", "1");
		FileSystem fs=FileSystem.get(conf);
		Path path=new Path("/test/up.txt");
		//Path path=new Path("/test");
		if(fs.exists(path)){
			FileStatus status = fs.getFileStatus(path);
			System.out.println(path+">>"+status);
		}else{
			System.out.println("指定的文件或目录不存在！");
		}
			
		fs.close();
	}
	
	public static void fileStatus2() throws IOException{
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
		conf.set("dfs.replication", "1");
		FileSystem fs=FileSystem.get(conf);
		Path path=new Path("/test");
		if(fs.exists(path)){
			 FileStatus[] statuses = fs.listStatus(path);
			 for(FileStatus val:statuses){
				 if(val.isDirectory()){
					 System.out.println(path+">>【Dir】"+val);
				 }else{
					 System.out.println(path+">>"+val);
				 }
			 }
		}else{
			System.out.println("指定的文件或目录不存在！");
		}
			
		fs.close();
	}
	
	public static void mkdir() throws IOException{
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
		conf.set("dfs.replication", "1");
		FileSystem fs=FileSystem.get(conf);
		Path path=new Path("/test/mm");
		boolean flag = fs.mkdirs(path);
		if(flag){
			System.out.println("目录创建成功！");
		}else{
			System.out.println("目录创建失败！");
		}
		fs.close();
		
	}
	
	
	public static void read() throws IOException{
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
		conf.set("dfs.replication", "1");
		FileSystem fs=FileSystem.get(conf);
		Path path=new Path("/test/helloHadoop.txt");
//		Path path=new Path("/user/upp.txt");
//		Path path=new Path("/user/myfirst.txt");
		
		FSDataInputStream fin=fs.open(path);
		String text = fin.readUTF();
		System.out.println(">>>"+text);
		
		fin.close();
		fs.close();
	}
	
	
	public static void rename() throws IOException{
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
		conf.set("dfs.replication", "1");
		FileSystem fs=FileSystem.get(conf);
		
		Path sourcePath=new Path("/test/up.txt");
		Path targetPath=new Path("/user/upp.txt");
		
		if(fs.exists(sourcePath)){
			boolean flag = fs.rename(sourcePath, targetPath);
			System.out.println(">>>执行成功:"+flag);
		}else{
			System.out.println("指定的文件不存在!");
		}
		
		fs.close();
	}
	
	//下载的文件为什么是空的？
	public static void downLoad() throws IOException{
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
		conf.set("dfs.replication", "1");
		
		FileSystem fs=FileSystem.get(conf);
		Path targetPath=new Path("D:/upp.txt");
		Path sourcePath=new Path("/user/myfirst.txt");
		
		if(fs.exists(sourcePath)){
			fs.copyToLocalFile(sourcePath, targetPath);
		}else{
			System.out.println(">>>指定的文件不存在！");
		}
		
		fs.close();
	} 
	
	public static void progessWrite() throws IOException{
		//配置对象
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
		conf.set("DFS.replication", "1");
		
		//文件系统对象
		FileSystem fs=FileSystem.get(conf);
		
		//操作的文件或目录
		Path path=new Path("/user/yourhello3.txt");
		
		//使用流进行操作
		byte[] buff=">>hello***helloasdd*ghfh".getBytes();
		FSDataOutputStream fout=null;
		if(fs.exists(path)){
			fout = fs.append(path,0,new Progressable() {
				public void progress() {
					System.out.println("#");					
				}
			});
		}else{
			fout=fs.create(path, new Progressable() {
				public void progress() {
					System.out.println("·");
				}
			});
		}
		
		//关闭相关流
		fout.write(buff);
		fout.flush();
		fout.close();
		fs.close();
	}
	
	public static void progessUp() throws IOException{
		//配置对象
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.43.50:9000");
		conf.set("DFS.replication", "1");
		
		//文件系统对象
		FileSystem fs=FileSystem.get(conf);
		
		//操作的文件或目录
		Path path=new Path("/user/cccc.txt");
		File file=new File("D:\\eclipse\\eclipse-jee-mars-2-win32-x86_64\\eclipse\\plugins\\hadoop-eclipse-plugin-2.6.0.jar");
		
		//通过流进行操作
		/**
		 * FileInputStream:从文件系统中读取，单位是字节
		 * DataInputStream:数据输入流，读取的是java基本数据类型，构造函数传入的参数是基础输入流InputStream
		 * FSDataInputStream:由FileSystem对象中的open()方法返回，是继承了java.io.DataInputStream接口的一个特殊类,并支持随机访问,可以从流中的任意位置读取数据
		 */
		FileInputStream fin=new FileInputStream(file);
		final float fileSize=fin.available()/65536;
		
		System.out.println(">>文件上传开始");
		FSDataOutputStream fout=fs.create(path,new Progressable() {
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
		/**
		 * IOUtils类:该所有成员字段和方法都是静态的，因此在标准编程中不需要创建IOUtils类的对象，而是通过类名和适当的方法名来使用它。
		 */
		IOUtils.copyBytes(fin, fout, conf);
		IOUtils.closeStream(fout);
		IOUtils.closeStream(fin);
		fs.close();
		
		System.out.println(">>文件传输完毕！");
	}
	
	public static void progessUp2() throws IOException{
		//配置对象
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.0.110:9000");
		conf.set("DFS.replication", "1");
		
		//文件系统对象
		FileSystem fs=FileSystem.get(conf);
		
		//操作的文件或目录
		Path path=new Path("/user/testupprogress.txt");
		File file=new File("D:\\eclipse\\eclipse-jee-mars-2-win32-x86_64\\eclipse\\plugins\\hadoop-eclipse-plugin-2.6.0.jar");
		
		//通过流进行操作
		FileInputStream fin=new FileInputStream(file);
		final float fileSize=fin.available()/65536;
		
		System.out.println(">>文件上传开始");
		FSDataOutputStream fout=fs.create(path,new Progressable() {
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
		fs.close();
		
		System.out.println(">>文件传输完毕！");
	}
	
	
	public static void main(String[] args) throws IOException{
//		aa();
//		create();
//		delete();
//		upLoad();
//		upLoad2();
//		fileStatus();
//		fileStatus2();
//		mkdir();
//		read();
//		rename();
//		downLoad();
//		progessWrite();
		progessUp();
	}
}
