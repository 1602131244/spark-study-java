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
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 
 * 实时WordCount程序
 * @author GYJ
 * 2018-1-12
 */
public class WordCount {
	
	public static void main(String[] args) throws Exception {
		//创建SparkConf对象
		//但是这里有点不同，我们是要给它设置一个master属性，但是我们测试的时候是local模式
		//local后面必须跟一个方括号，里面填写一个数字，数字代表了，我们用几个线程来执行我们的
		//Spark Streaming程序
		
		SparkConf conf = new SparkConf()
		                     .setMaster("local[2]")
		                     .setAppName("WordCount");
		
		//创建JavaStreamingContext对象
		//该对象，类似与Spark Core 中的JavaSparkContext,类似 与Spark SQL 中SQLContext
		
		//该对象出了接收一个SparkConf对象以外
		//还必须接收一个batch interval 参数，也就是说，每收集多长时间的数据，划分为一个batch，进行处理
		//这里设置一秒
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(1));
		
		
		//首先，创建DStream，代表了一个从数据源（比如kafka,socket）来的持续不断的实时数据流
		//调用JavaStreamingContext的socketTextStream()方法，可以创建一个数据源为Socket网络端口的数据流，
		//JavaRexeiverInputDStream，代表了一个输入的DStream
		//socketTextStream（）方法接收两个基本的参数，第一个是监听那个主机上的端口，第二个是监听那哥端口
		
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		
		//到这里为止，你可以理解为JavaReceiverInputDStream中的，每隔一秒，会有一个RDD，其中封装了
		//这一秒发送过来的数据
		//RDD的元素类型为String，即一行一行的文本
		//所以，这里JavaRecevierInputDStream的泛型类型<String>,其实就代表了它底层的RDD的泛型类型
		
		
		//开始对接收到的数据，执行计算，使用Spark Core 提供的算子，执行应用在DStream中即可
		//在底层，实际上会对DStream中的一个一个的RDD，执行我们应用在DStream上的算子
		//产生的新RDD，会作为新DStream中的RDD
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String line) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		
		//这个时候，每秒的数据，一行一行的文本，就会被拆分成多个单词，words DStream中的RDD的元素类型
		//即为一个一个的单词
		
		
		//接着，开始进行flatMap，reduceByKey操作
		JavaPairDStream<String, Integer>  paris = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String,Integer>(word,1);
			}
		});
		
		//这里，正好说明一下，其实大家可以看到，用Spark Streaming开发程序，和Spark Core很相像
		//唯一不同的是Spark Core 中的JavaRDD，JavaPairRDD 都变成了JavaDStream，JavaPairDStream
		
		JavaPairDStream<String, Integer> wordCounts = paris.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		});
		
		//到此为止，我们就实现了实时的WordCount程序了
		//大家总结一下思路，加深一下印象
		//每秒中发送到指定的socket的端口的数据，都会被lines DStream 接收到
		//然后lines DStream 会把每秒的数据，也就是一行一行的文本，诸如hello world，封装为一个RDD
		//然后呢，就会对每一秒钟对应的RDD，执行后续的一系列的算子操作，
		//比如，对lines RDD执行了flatMap之后，得到了一个words RDD ， 作为words DStream 中的一个RDD
		//以此类推，直到生成最后一个，wordCounts RDD ,作为WordCounts DStream 中的一个RDD
		//但是，一定要注意，Spark Streaming的计算模型，就决定了，我们必须中间缓存的控制，
		//比如写入Redis等缓存
		//它的计算模型跟Strom是完全不同的，Strom是自己编写的一个一个程序，运行在节点上，相当于一个
		//一个的对象，可以自己在对象中控制缓存
		//但是Spark本身是函数式编程的计算模型，所以，比如words或paris DStream 中，没法在实例变量中
		//进行缓存
		//此时，就只能将最后计算出的WordCounts中一个一个的RDD，写入外部的缓存，或持久化DB
		
		
		
		//最后，每次计算完，都打印一下，这一秒钟，单词计数的情况，
		//并休眠5秒钟，以便我们测试和观察
		
		
		wordCounts.print();
		Thread.sleep(5000);
		
		
		
		
		//首先对JavaStreamingContext进行一下后续处理
		//必须调用javaStreamingContext的start（）方法，整个SparkStraming Application才会启动
		//否则是不会执行的
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
		
	}
}
