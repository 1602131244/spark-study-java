package cn.spark.study.streaming;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 热点搜索词滑动统计，每隔10秒
 * @author GYJ
 * 2018-2-11
 *
 */
public class WindowHotWord {
	public static void main(String[] args) throws Exception {
		SparkConf conf= new SparkConf()
							.setAppName("WindowHotWord")
							.setMaster("local[2]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf ,Durations.seconds(5));
		
		//说明一下，这里的搜索日志的格式，
		//leo hello
		//tom world
		JavaReceiverInputDStream<String> searchLogDStream = jssc.socketTextStream("spark1", 9999);
		
		//将搜索日志转换成，只有一个搜索词即可，
		
		JavaDStream<String> searchWordDStream = searchLogDStream.map(new Function<String, String>() {

			private static final long serialVersionUID = 1L;

			public String call(String searchLog) throws Exception {
				// TODO Auto-generated method stub
				return searchLog.split(" ")[1];
			}
		});
		
		//将搜索词映射为（searchWord，1）tuple2模式
		
		JavaPairDStream<String, Integer> searchWordPairDStream = searchWordDStream.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String searchWord) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(searchWord, 1);
			}
		});
		
		//针对(searchWord，1)的tuple2格式的DStream，执行reduceByKeyAndWindow,滑动窗口操作
		
		//第二个参数，是窗口长度，这里是60秒
		//第三个参数，是滑动间距，这里是10秒
		//也就是说，每隔10秒钟，将最近60秒的数据，作为一个窗口，进行内部的RDD的聚合
		//然后统一对一个RDD进行后续计算
		//所以说，这里的意思，就是，之前的searchWordPairDStream为止，其实都是不会立即进行计算的
		//而是，只是放在那里，
		//然后,等待我们的滑动间隔到了以后，10秒钟到了，会将之前60秒的RDD，因为一个batch间隔是，5秒,所以之前
		//60秒，就有12个RDD，给聚合起来，然后统一执行reduceByKey操作
		//所以这里的reduceByKeyAndWindow，是针对每个窗口执行计算的，而不是针对某个DStream中的RDD
		JavaPairDStream<String, Integer> searchWordCountDStream = searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			public Integer call(Integer V1, Integer V2) throws Exception {
				
				return V1+V2;
			}
		}, Durations.seconds(60),Durations.seconds(10));
		
		
		//到这里为止，就已经可以做到，每隔10秒钟，出来之前60秒的搜集到的单词的统计次数
		
		//执行transform操作，因为，一个窗口，就是一个60秒的数据，会变成一个RDD，然后，对这一个RDD
		//根据每隔搜索词的词频进行排序，然后获取排名前三的热点搜索词
		JavaPairDStream<String, Integer> finalDStream = searchWordCountDStream.transformToPair(new Function<JavaPairRDD<String,Integer>, JavaPairRDD<String,Integer>>() {

			private static final long serialVersionUID = 1L;

			public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> searchWordCountsRDD)
					throws Exception {
				// 执行搜索词和出现频率的反转
				JavaPairRDD<Integer, String> countSearchWordRDD = searchWordCountsRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Integer, String>(tuple._2, tuple._1);
					}
				});
				
				//然后执行降序排序
				JavaPairRDD<Integer,String>  sortedCountSearchWordRDD = countSearchWordRDD.sortByKey(false);
				
				//然后再次执行反转，编程searchWord， count 这种格式
				JavaPairRDD<String, Integer> sortedSearchWordCountRDD = sortedCountSearchWordRDD.mapToPair(new PairFunction<Tuple2<Integer,String>,String, Integer>() {

					
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, Integer>(tuple._2, tuple._1);
					}
				});
				
				//然后用take()获取排名前三的数据
				List<Tuple2<String, Integer>> hotSearchWordCounts = sortedSearchWordCountRDD.take(3);
			    for (Tuple2<String, Integer> wordCount : hotSearchWordCounts) {
					System.out.println( wordCount._1+ "" + wordCount._2);
				}
		
				return searchWordCountsRDD;
			}
		});
		
		
		//这个无关紧要，只是为了触发job的执行，所以必须有output操作
		finalDStream.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
