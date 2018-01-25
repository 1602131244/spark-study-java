package cn.spark.study.core.wordcount;



import java.util.Arrays;

import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;


/**
 * 将Java开发的WordCount的程序部署到Spark集群上运行
 * @author GYJ
 *
 */
public class WordCountCluster {
	public static void main(String[] args) {
		//编写spark应用程序
		//本地执行，是可以直接在eclipse中的main方法,执行的
		
		//第一步：创建SparkConf对象，设置 Spark应用的配置信息
		//如果要在Spark集群上运行，需要修改的只有两个地方
		//第一，将SparkConf的setMaster()方法删掉，默认它自己会连接
		//第二，针对的不是本地文件，修改为hadoop hdfs上的真正的的存储大数据文件
		
		//实际执行步骤：
		//1.将spark.txt文件上传到hdfs文件系统上
		//2.使用我们在pom.xml配置的maven插件对工程进行打包
		//3.将打包后的spark工程的jar包上传到Linux机器执行
		//4.编写spark-submit脚本
		//5.执行spark-submit脚本，提交spark应用至集群执行
		SparkConf sparkConf = new SparkConf()
					.setAppName("WordCountCluster");
					
		
		//第二步：创建JavaSparkContext对象
		
		JavaSparkContext sc =new JavaSparkContext(sparkConf);
		
		//第三步，要针对一个输入源（hdfs文件，本地文件，等等），创建一个初始的RDD
		
		JavaRDD<String> lines = sc
				.textFile("hdfs://spark1:9000/spark.txt");
  		
		//第四步，对初始的RDD进行transformation操作，也就是一些计算操作
		
		JavaRDD<String> words =lines.flatMap(new FlatMapFunction<String, String>() {
			
			private static final long serialVersionUID = 1L;

			
			public Iterator<String> call(String line) throws Exception {
				
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
	
		
		//接着。需要将每个单词映射为<单词，1>的key-value对形式
		JavaPairRDD<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String word)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, Integer>(word, 1);
					}
				});
		
		//接着需要单词作为key,统计每个单词出现的次数
		JavaPairRDD<String, Integer> wordCounts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return v1+v2;
					}
				});
		
		//接着，最后，可以使用一中叫做action操作，比如说，foreach 来触发程序的执行
		wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Integer> wordCount) throws Exception {
				System.out.println(wordCount._1+" appeared "+wordCount._2+" times .");
			}
		});
		
		sc.close();
		
	}
}
