package cn.spark.study.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * 基于kafka的Wordcount实时程序
 * @author GYJ
 * 2018-1-16
 */
public class kafkaWordCount {
 
	public static void main(String[] args) throws Exception{
		SparkConf conf = new SparkConf()
							.setMaster("local[2]")
							.setAppName("kafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
		
		//使用KafkaUtils.createDirectStream（）方法，创建针对kafkade 的数据输入流
		
		Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
		topicThreadMap.put("WordCount", 1);
		
		JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
				jssc, 
				"192.168.1.107:2181,192.168.1.108:2181,192.168.1.109:2181", 
				"DefaultConsumerGroup", 
				topicThreadMap);
		
		//然后开发WordCount逻辑
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(Tuple2<String, String> t)
					throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(t._2.split(" ")).iterator();
				}
			});
		
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

		
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairDStream<String, Integer> wordCounts  = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
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
