package cn.spark.study.streaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 基于HDFS文件的WordCount的实时程序
 * @author GYJ
 * 2018-1-16
 */
public class HDFSWordCount {

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf()
							.setMaster("local[2]")
							.setAppName("HDFSWordCount");
		
	
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
		
		//首先，使用JavaStreamingContext的textFileStream（）方法，针对HDFS目录创建输入数据流
		
		JavaDStream<String> lines  = jssc.textFileStream("hdfs://spark1:9000/wordcount_dir");
		
		//执行WordCount操作
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String line) throws Exception {
				
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		
	    JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
			
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
	    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			public Integer call(Integer t1, Integer t2) throws Exception {
				// TODO Auto-generated method stub
				return t1+t2;
			}
		});
	    wordCounts.print();
	    Thread.sleep(5000);
	    
	    
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
