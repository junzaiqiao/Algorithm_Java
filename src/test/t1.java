package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Scanner;







import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.tomcat.jni.Time;




public class t1{
	private static String inputfilename="hdfs://master:8020/ocdc/algorithm/kmeans/output/part-r-00000";
	private static String outputfilename="hdfs://master:8020/ocdc/algorithm/kmeans/output";
	private static int k=3;
	private static int l=15;
	private static HashMap<String,String> center;
	
	public static Object selectCenter(Configuration conf,String file) throws IOException{
		center= new HashMap<String,String>();
		long count=0;
		long tmp;
		//存放生成的随机数
		HashSet<Object> hs = new  HashSet<Object>();
		//利用uri创建输入流
		FileSystem fs = FileSystem.get(URI.create(file),conf);
		
		FSDataInputStream  fsins = fs.open(new Path(file));
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fsins));

//		Scanner sc = new Scanner(fsins,"utf-8");
//		while( sc.hasNextLine() ){
//			String ltmp = sc.nextLine();
//			count+=1;
//		}
//
////		//生成并保存k个随机数
////		while (hs.size() < t1.k){
////			tmp=(long) (Math.random() * count);
////			if (tmp != 0){
////				hs.add(tmp);
////			}
////		}
////		System.out.println(hs);
////
////		//回到文件开始位置
////		fsins.seek(0);
		
		long i=1;
		int j=1;
		String line=br.readLine();
		while(line != null){
			System.out.println(line);
			line=br.readLine();
			i+=1;
		}

		

		
		fs.close();
		return 0;
		
	}
	

	


	public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException{
		
		t1 t = new t1();
		Configuration conf = new Configuration();
			
//		@SuppressWarnings("deprecation")//该批注的作用是给编译器一条指令，告诉它对被批注的代码元素内部的某些警告保持静默
		Job  job = new Job(conf,"KMeans");
		conf.addResource(new Path("/app/hadoop-2.7.0/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/app/hadoop-2.7.0/etc/hadoop/hdfs-site.xml"));
		
		int ret=(int)t1.selectCenter(conf,t1.inputfilename);
		if( ret != 0){
			System.out.println("Initialization Select Center Failure!");
			System.exit(255);
		}

		
		
	}
	
}



