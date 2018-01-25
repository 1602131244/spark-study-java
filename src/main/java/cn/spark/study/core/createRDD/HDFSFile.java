package cn.spark.study.core.createRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 使用HDFS文件创建RDD
 * 案例：统计文本文件字数
 * @author GYJ
 * 2017-10-30
 */
public class HDFSFile {
	
	public static void main(String[] args) {
		//创建SparkConf
		SparkConf conf = new SparkConf()
							.setAppName("HDFSFile");
		
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//使用JavaSparkContex及其子类的textFile()，针对HDFS文件创建RDD
		JavaRDD<String> lines = sc
				.textFile("hdfs://spark1:9000/spark.txt");
		
		//统计文本文件的字数
		JavaRDD<Integer> lineLeng = lines.map(new Function<String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(String v1) throws Exception {
				
				return v1.length();
			}
			
		});
		
		int counts = lineLeng.reduce(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		});
		
		//打印
		System.out.println("HDFS文件文本总字数 ： "+counts);
		//关闭JavaSparkContext
		sc.close();
		
	}
}
