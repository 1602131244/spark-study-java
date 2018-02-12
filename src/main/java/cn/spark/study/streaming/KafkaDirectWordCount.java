package cn.spark.study.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * 基于kafka direct方式的实时的WordCount程序
 * @author GYJ
 * 2018-1-25
 *
 */
public class KafkaDirectWordCount {
	
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf()
							.setMaster("local[2]")
							.setAppName("KafkaDirectWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
		
		//首先创建一份Kafka参数的map
		
		Map<String,String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list",
				"192.168.1.107:9092,192.168.1.108:9092,192.168.1.109:9092");
		//创建一个set,里面放入，要读取的topic
		//这个就是我们所说的，它自己给你做的很好，可以并行读取多个topic
		Set<String> topics = new HashSet<String>();
		topics.add("WordCount");
		
		//创建输入DStream
		JavaPairDStream<String, String> lines = KafkaUtils.createDirectStream(
				jssc, 
				String.class, 
				String.class, 
				StringDecoder.class, 
				StringDecoder.class, 
			    kafkaParams, 
				topics);
		
		//执行WordCount操作
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(Tuple2<String, String> line)
					throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(line._2.split(" ")).iterator();
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
				return t1+t2;
			}
		});
		
		Thread.sleep(5000);
		wordCounts.print();
		
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
