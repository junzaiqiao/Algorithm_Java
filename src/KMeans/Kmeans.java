package KMeans;
/**
 * @introduce :
 * 算法原理
 * kmeans的计算方法如下：
 * 1 随机选取k个中心点
 * 2 遍历所有数据，将每个数据划分到最近的中心点中
 * 3 计算每个聚类的平均值，并作为新的中心点
 * 4 重复2-3，直到这k个中线点不再变化（收敛了），或执行了足够多的迭代
 * 时间复杂度：O(n*m*k*l)
 * 空间复杂度：O(m*n)
 * 其中l为每个元素字段个数，m为数据量，n为迭代次数。一般n,k,l均可认为是常量，所以时间和空间复杂度可以简化为O(m)，即线性的。
 * @author :  lisj 
 * @create :  2016-07-02
 * 
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Kmeans {
	private static String inputpath = "hdfs://master:8020/ocdc/algorithm/kmeans/input/kmdata.txt";
	private static String outputpath = "hdfs://master:8020/ocdc/algorithm/kmeans/output";
	private static int k = 3;   //kmeans
	private static int n = 30;  //kmeans的迭代次数
	private static int l = 15;  //kmeans中待观测的聚类向量的维度
	private static boolean check=false;  //kmeans 检查中心是否相同的标识(false标示不同，反之则相同)
	private static HashMap<String, String> center;     //kmeans存储中心向量的hashmap
	private static HashMap<String, String> newcenter ;     //kmeans存储中心向量的hashmap
	/**
	 * KMeans selectCenter
	 * 从待测试的向量中随机取三个向量作为簇中心
	 * @param String[]  String[]
	 * @throws IOException
	 * @return 状态
	 */

	public Object selectCenter(Configuration conf, String file)
			throws IOException {
		center = new HashMap<String, String>();

		long count = 0;
		long tmp;
		// 存放生成的随机数
		HashSet<Object> hs = new HashSet<Object>();
		// 利用uri创建输入流
		FileSystem fs = FileSystem.get(URI.create(file), conf);

		FSDataInputStream fsins = fs.open(new Path(file));

		BufferedReader br = new BufferedReader(new InputStreamReader(fsins));

		Scanner sc = new Scanner(fsins, "utf-8");
		while (sc.hasNextLine()) {
			String ltmp = sc.nextLine();
			count += 1;
		}

		// 生成并保存k个随机数
		while (hs.size() < Kmeans.k) {
			tmp = (long) (Math.random() * count);
			if (tmp != 0) {
				hs.add(tmp);
			}
		}
		System.out.println(hs);

		// 回到文件开始位置
		fsins.seek(0);

		long i = 1;
		int j = 1;
		String line = null;
		do {
			line = br.readLine();
			if (hs.contains(i)) {
				center.put(Integer.toString(j), line);
				j += 1;
			}
			i += 1;
		} while (line != null);

		System.out.println(Kmeans.center);

		if (sc != null || br != null) {
			br.close();
			return 0;
		} else
			return 1;

	}
	/**
	 * KMeans distanceCenter
	 * 计算连个向量的距离余弦距离计算
	 * @param String[]  String[]
	 * @throws IOException
	 * @return null
	 */

	public static Double distanceCenter(String[] newcenter, String[] oldcenter) {
		Double a = 0.0;
		Double b = 0.0;
		Double ab = 0.0;
		Double dis = 0.0;

		for (int n = 1; n < Kmeans.l; n++) {
			ab = ab + Double.parseDouble(newcenter[n])
					* Double.parseDouble(oldcenter[n]);
			a = a + Math.pow(Double.parseDouble(newcenter[n]), 2);
			b = b + Math.pow(Double.parseDouble(oldcenter[n]), 2);
		}
		// 判断分母是否为0
		if (a * b == 0) {
			return dis;
		}

		dis = ab / Math.sqrt(a * b);

		return dis;

	}
	/**
	 * KMeans compareCenter
	 * 比较新，旧中心，不同则用新中心替换旧中心
	 * @param HashMap  HashMap
	 * @throws IOException
	 * @return null
	 */
	public void compareCenter(HashMap<String , String> oldcenter,HashMap<String , String> newcenter){
		boolean flag1=true;
		for( int hk=1; hk <= Kmeans.k ; hk++ ){
			String[] oldcentlist = oldcenter.get(String.valueOf(hk)).split(",");
			
			String[] newcentlist = newcenter.get(String.valueOf(hk)).split(",");
			
			for( int i=1;i < Kmeans.l ;i++ ){
//				System.out.println("oldcentlist="+i+"=" +oldcentlist[i]);
//				System.out.println("newcentlist="+i+"=" +newcentlist[i]);
				if( oldcentlist[i].equals( newcentlist[i]) !=true ){
					flag1=false;
					break;
					
				}
			}
			if( flag1 == false ){
				break;
			}
		}
		if( flag1 == false  ){
			Kmeans.check=false;
			for( int ck=1; ck<=Kmeans.k ;ck++){
				Kmeans.center.put(String.valueOf(ck), newcenter.get(String.valueOf(ck)));
			}
			
		}else{
			Kmeans.check=true;
		}
		
	}
	

	/**
	 * KMeans map
	 * mapreduce中的map计算每个向量属于哪个簇
	 * @param args
	 * @throws IOException
	 */
	public static class kmeansMap extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// 待观测的new对象
			String[] line = value.toString().split(",");

			Double maxdis = 0.0;
			int index = -1;
			// 获取old中心
			for (int q = 1; q <= Kmeans.k; q++) {

				String[] oldcenterline = Kmeans.center.get(String.valueOf(q))
						.split(",");
				Double cos_res = Kmeans.distanceCenter(line, oldcenterline);
				if (maxdis <= cos_res) {
					maxdis = cos_res;
					index = q;
				}
			}
			context.write(new LongWritable(index), value);

		}
	}

	/**
	 * KMeans Combine
	 * mapreduce中的combine 在本地主机上计算每个簇的向量和（增加combine处理的目的是为了在本地处理合并数据减少主机间网络传输）
	 * @param args
	 * @throws IOException
	 */
	public static class kmeansCombine extends
			Reducer<LongWritable, Text, LongWritable, Text> {
		protected void combine(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			Double[] sumarray = new Double[Kmeans.l];
			StringBuffer tmpvalue = new StringBuffer();
			long num = 0;
			// 计算簇中向量的和

			for (Text value : values) {

				String[] array = value.toString().split(",");
				for (int i = 1; i < Kmeans.l; i++) {
					sumarray[i] = sumarray[i] + Double.parseDouble(array[i]);
				}
				num = num + 1;

			}
			// 列表第一个位置放每个簇中向量的数量
			sumarray[0] = (double) num;

			for (int j = 0; j < Kmeans.l; j++) {
				tmpvalue.append(sumarray[j]);
				if (j < Kmeans.l - 1) {
					tmpvalue.append(",");
				}
			}
            
			context.write(key, new Text(tmpvalue.toString()));
		}
	}

	/**
	 * KMeans reduce
	 * mapreduce中的reduce计算簇中新的中心向量
	 * @param args
	 * @throws IOException
	 */
	public static class kmeansReduce extends
			Reducer<LongWritable, Text, LongWritable, Text> {
		protected void reduce(LongWritable key, Iterable<Text> values,
				Context context ) throws IOException, InterruptedException {

			Double[] sumarray = new Double[Kmeans.l];
			Double[] avgarray = new Double[Kmeans.l];
			StringBuffer tmpvalue = new StringBuffer();

			long num = 0;
			// 初始化sumarray数组
			for (int m = 0; m < Kmeans.l; m++) {
				sumarray[m] = 0.0;
			}

			for (Text value : values) {
				String[] varrayStrings = value.toString().split(",");
				for (int i = 1; i < Kmeans.l; i++) {
					sumarray[i] = sumarray[i]
							+ Double.parseDouble(varrayStrings[i]);
				}
				num = Long.parseLong(varrayStrings[0]);
				num = num + 1;

			}
			sumarray[0] = (double) num;
			for (int j = 1; j < Kmeans.l; j++) {
				avgarray[j] = sumarray[j] / sumarray[0];
			}

			for (int j = 0; j < Kmeans.l; j++) {
				tmpvalue.append(avgarray[j]);
				if (j < Kmeans.l - 1) {
					tmpvalue.append(",");
				}
			}
//			//将生成的新的中心点存储到hasmap中，待比较
//			Kmeans.newcenter.put(key.toString(),tmpvalue.toString());
			
			context.write(key, new Text(tmpvalue.toString()));
		}
	}
	public void storeCenter(Configuration conf,FileSystem dfs ,String centerpath){
		try {
			FileStatus[] filelist = dfs.listStatus(new Path (centerpath));
			for(int i=0 ;i < filelist.length ; i++){
				//只要文件
				if (!filelist[i].isDir()) {  
					String completepathfile=filelist[i].getPath().toString();
					
					FileSystem centerfs = FileSystem.get(URI.create(completepathfile), conf);

					FSDataInputStream fsinstream = centerfs.open(new Path(completepathfile));
					BufferedReader bufr = new BufferedReader(new InputStreamReader(fsinstream));
					String line=bufr.readLine();
					while( line != null ){
						
						System.out.println(line);
						String[] tmpstr=line.toString().split("\t");
						Kmeans.newcenter.put(tmpstr[0],tmpstr[1]);
						line = bufr.readLine();
					}
					fsinstream.close();
				}
			}
				
			
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void kmeansDriver(Configuration conf,Kmeans kmeans) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		newcenter = new HashMap<String,String>();
		FileSystem fs = FileSystem.get(URI.create(Kmeans.outputpath), conf);
        int i=1;
		while( true ){
			Job job = new Job(conf, "KMeans_"+String.valueOf(i));
			job.setJarByClass(Kmeans.class);
			job.setJobName("Kmeans_"+String.valueOf(i));
			
			FileInputFormat.addInputPath(job, new Path(Kmeans.inputpath));
			FileOutputFormat.setOutputPath(job, new Path(Kmeans.outputpath));
			
			

			// 判断输出目录是否存在，如果存在则删掉，delete(,true)加true是为了删掉目录
			if (fs.exists(new Path(Kmeans.outputpath))) {
				fs.delete(new Path(Kmeans.outputpath), true);
			}
			
			FileInputFormat.setInputPaths(job, Kmeans.inputpath);
			//拆分文件
//			FileInputFormat.setMinInputSplitSize(job, 50);
//			FileInputFormat.setMaxInputSplitSize(job, 100);
    	
			job.setMapperClass(kmeansMap.class);
			
			if( Kmeans.check == false && i<=Kmeans.n ){
				job.setCombinerClass(kmeansCombine.class);
				job.setReducerClass(kmeansReduce.class);
				
				job.setMapOutputKeyClass(LongWritable.class);
				job.setMapOutputValueClass(Text.class);
				
				//设置Reduce num
				job.setNumReduceTasks(1);
				
				job.setOutputKeyClass(LongWritable.class);
				job.setOutputValueClass(Text.class);
				
				i++;

				job.waitForCompletion(true);

				//将reduce结果集（中心点向量） 存储在newcenter中
				kmeans.storeCenter(conf,fs,Kmeans.outputpath);
				
				
				//比较新，旧中心是否一致，不一致则用新簇中心点替换旧簇中心点
				kmeans.compareCenter(Kmeans.center,Kmeans.newcenter);
				
				
			}
			else{
				job.setMapOutputKeyClass(LongWritable.class);
				job.setMapOutputValueClass(Text.class);
				
				//设置Reduce num
				job.setNumReduceTasks(1);
				
				job.setOutputKeyClass(LongWritable.class);
				job.setOutputValueClass(Text.class);
				job.waitForCompletion(true);
				break;
			}
			
		}
		
		
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Kmeans kmeans = new Kmeans();
		Configuration conf = new Configuration();


		int ret = (int) kmeans.selectCenter(conf, Kmeans.inputpath);
		if (ret != 0) {
			System.out.println("Initialization Select Center Failure!");
			System.exit(255);
		}

		kmeans.kmeansDriver(conf,kmeans);

	}

}
