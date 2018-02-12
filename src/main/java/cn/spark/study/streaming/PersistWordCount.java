package cn.spark.study.streaming;

import java.sql.Connection;

import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;












import cn.spark.study.streaming.conn.ConnectionPool;
import scala.Tuple2;

/**
 * 基于持久化机制的实时WordCount程序
 * @author GYJ
 * 2018-2-12
 */
public class PersistWordCount {
	public static void main(String[] args) throws Exception {
		SparkConf conf =new SparkConf()
		                    .setMaster("local[2]")
		                    .setAppName("PersistWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
		
		jssc.checkpoint("hdfs://spark1:9000/wordcount_checkpoint");
				
		JavaReceiverInputDStream<String> lines =  jssc.socketTextStream("spark1", 9999);
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String line) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		
		JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(
				
				new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

					private static final long serialVersionUID = 1L;
					
		            public Optional<Integer> call(List<Integer> values,
							Optional<Integer> state) throws Exception {
		            	Integer newValues =0;
		            	
		            	if(state.isPresent()){
		            		newValues = state.get();
		            	}
		            	
		            	for(Integer value:values){
		            		newValues += value;
		            	}
						return Optional.of(newValues);
					}
		});
		
		//每次得到当前所有单词的统计次数之后，将其写入MYSQL存储，进行持久化， 
		//以便后续的J2EE应用程序进行显示
		wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {

			private static final long serialVersionUID = 1L;

			public void call(JavaPairRDD<String, Integer> wordCountRDD)
					throws Exception {
				//调用RDD的foreachPartition（）方法
				wordCountRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {

					private static final long serialVersionUID = 1L;

					public void call(Iterator<Tuple2<String, Integer>> wordCounts)
							throws Exception {
						// 给每个partition,获取一个连接
						Connection conn = ConnectionPool.getConnection();
						
						//遍历partition中的数据，使用一个连接，插入数据库
						Tuple2<String, Integer> wordCount = null;
						while (wordCounts.hasNext()) {
							wordCount = wordCounts.next();
							
							
							String sql = "insert into wordcount(word,count) "
									+ "Values(' " +wordCount._1 + " ',"+wordCount._2 +")";
							
							Statement stmt = conn.createStatement();
							stmt.executeUpdate(sql);
							
						}
						
						
						
						//用完以后，将连接还回去
						ConnectionPool.returnConnection(conn);
						
					}
				});
				
			}
		});
		
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
