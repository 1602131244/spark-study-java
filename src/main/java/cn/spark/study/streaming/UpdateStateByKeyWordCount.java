package cn.spark.study.streaming;

import java.util.Arrays;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;




import scala.Tuple2;

/**
 * 基于UpdateStateByKey算子实现缓存机制的实时WordCount程序
 * @author GYJ
 *
 */
public class UpdateStateByKeyWordCount {
	public static void main(String[] args) throws Exception {
		SparkConf conf =new SparkConf()
		                    .setMaster("local[2]")
		                    .setAppName("UpdateStateByKeyWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
		//第一点，如果要使用updateStateByKey算子，就必须设置一个checkpoint目录，开启checkpoint机制
		//这样的话才能把每一个Key对应的state除了内存中有，那么是不是也要checkpoint一份
		//因为你要长期保存一份key的state的话，那么spark Streaming是要求必须checkpoint，以便于在
		//内存数据丢失的时候，可以从checkpoint中恢复数据
		
		//开启checkpoint机制很简单，只要调用jssc的checkpoint（）方法，设置一个hdfs目录即可
		jssc.checkpoint("hdfs://spark1:9000/wordcount_checkpoint");
		
		
		//然后实现基础的WordCount逻辑
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
		
		
		//到了这里就不一样了，之前的话，直接就是pairs.reduceByKey()
		//然后，就可以得到每个时间段的batch对应的RDD，计算出来的单词计数
		//然后，可以打印出那么时间段的单词计数
		//但是，有个问题，如果要统计某个单词的全局的计数？
		//就是说统计出来，从程序启动开始，到现在为止，一个单词出现的次数，那么用之前的方式就不好实现
		//就必须基于Redis这种缓存，或者MySQL这种DB，来实现累加
		
		
		//但是，updateStateByKey，就可以实现直接通过spark维护一份每个单词的全局统计次数
		
		JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(
				//这里的Optional,相当于scala中的样例类，就是Option，可以这么理解
				//它代表了一个值的存在状态，可能存在，也可能不存在
				new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

					private static final long serialVersionUID = 1L;
					//这里两个参数，
					//实际上，对于每个单词，每次batch计算的时候，都会调用这个函数
					//第一个参数，values,相当于是这个batch中，这个Key的新的值，可能有多个吧
					//比如说一个hello，可能有2个1，（hello，1），（hello，1），那么传入的就是（1,1）
					//第二个参数，就是指的是这个Key之前的状态，state，其中泛型的类型是你自己制定的
					
		            public Optional<Integer> call(List<Integer> values,
							Optional<Integer> state) throws Exception {
						// 首先自定义一个全局的单词技术
		            	Integer newValues =0;
		            	//其次，判断，state是否存在，如果不存在，说明是一个Key第一次出现，
		            	//如果存在，说明这个Key之前已经统计过
		            	if(state.isPresent()){
		            		newValues = state.get();
		            	}
		            	
		            	//接着，将本次新出现的值，都累加到这个newValues上去，就是一个Key目前的全局统计
		            	for(Integer value:values){
		            		newValues += value;
		            	}
						return Optional.of(newValues);
					}
		});
		
		//到这里为止，相当是每个batch过来是，计算到pairs DStream ，就会执行全局的updateStateByKey算子
		//updateStateByKey返回的JavaPairDStream，其实就是代表了每个Key的全局的计数
		
		
		//打印
		wordCounts.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
