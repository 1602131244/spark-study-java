package cn.spark.study.core.persistence;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * RDD持久化
 * @author GYJ
 * 2017-11-02
 */
public class Persist {
	public static void main(String[] args) {
		//创建SparkConf
		SparkConf conf = new SparkConf()
							 .setAppName("Persist")
							 .setMaster("local");
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//获取本地文件创建RDD
		//cache()  和  persist() 大的使用是有规则的，
		//必须在transformation或者textFile创建了一个RDD后，连续调用cache()或者persist()才可以
		//如果你先创建一个RDD ,然后单独另起一行执行cache()或者persist()方法，是没有用的
		//而且会报错，大量的文件会丢失
		JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//access_2013_05_31.log")
				                  .cache();
		
		//对lines执行第一次count操作
		long beginTime = System.currentTimeMillis();
		long counts = lines.count();
		long endTime = System.currentTimeMillis();
		//打印结果
		System.out.println("cost1：  "+(endTime-beginTime)+ " milliseconds . ");
		System.out.println("Text File have "+counts+ " lines . ");
		
		
		//对lines执行第二次count操作
		beginTime = System.currentTimeMillis();
		counts = lines.count();
		endTime = System.currentTimeMillis();
		//打印结果
		System.out.println("cost:2：  "+(endTime-beginTime)+ " milliseconds . ");
		System.out.println("Text File have "+counts+ " lines . ");
		//关闭JavaSparkContext
		sc.close();
	}
}
