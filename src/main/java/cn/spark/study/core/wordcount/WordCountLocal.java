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
 * 本地测试的WordCount程序
 * @author GYJ
 *
 */
public class WordCountLocal {
	public static void main(String[] args) {
		//编写spark应用程序
		//本地执行，是可以直接在eclipse中的main方法,执行的
		
		//第一步：创建SparkConf对象，设置 Spark应用的配置信息
		//使用setMaster(),可以设置Spark应用程序要连接的Spark集群master节点的URL
		//但是设置为local,则代表在本地运行
		SparkConf sparkConf = new SparkConf()
					.setAppName("WordCountLocal")
					.setMaster("local");
		
		//第二步：创建JavaSparkContext对象
		/**
		 *  1.在Spark中，SparkContext是Spark所有功能的一个入口，无论是用Java，Scala，Python编写
				都必须要有一个SparkContext，它的主要作用，包括初始化Spark应用程序所需的一些应用组件,包括：
				调度器（DAGSchedule，TaskSchedule）、还会去Spark Master节点上进行注册，等等
			2.SparkContext 是Spark应用中最最重要的一个对象
			3.但是在Spark中，编写不同类型的Spark应用程序，使用的SparkContext是不同的，如果
				如果使用scala，使用的就是原生的SparkContext对象，
				但是如果使用的是Java，那么就是JavaSparkContext对象
				如果是开发Spark SQL程序，那么就是SQLContext，HiveContext
				如果开发的是Spark Streaming程序，那么就是它独有的SparkContext
				以此类推。		
		 */
		JavaSparkContext sc =new JavaSparkContext(sparkConf);
		
		//第三步，要针对一个输入源（hdfs文件，本地文件，等等），创建一个初始的RDD
		//输入源中的数据会打散，分配到RDD的的每个partition中，从而形成一个初始的分布式的数据集
		//这里是本地测试，所以针对的是本地文件
		//SparkContext中，用于根据文件类型的输入源创建RDD的方法，叫做textFile()方法
		//在Java中，创建的RDD都叫做JavaRDD
		//在这里，RDD中，有元素这种概念，如果是hdfs文件或者本地文件呢，创建的RDD，
		//每一个元素就相当于是文件的一行
		JavaRDD<String> lines = sc
				.textFile("C://Users//Administrator//Desktop//spark.txt");
  		
		//第四步，对初始的RDD进行transformation操作，也就是一些计算操作
		//通常操作会通过创建function，配合RDD的map，flatMap等算子来执行
		//function,通常，如果比较简单，则创建指定的Function的匿名内部类
		//但是Function比较复杂，则会单独创建一个类，作为实现这个Function接口的类
		//现将每一行拆分成单个的单词
		//FlatMapFunction,有两个泛型参数，分别代表了输入和输出
		//我们这里，输入时String，因为是一行一行的输入，输出，也是String，因为是每一行文本
		//这里先简要介绍flatMap算子作用，其实就是，将RDD的一个元素，拆分成一个或多个元素
		
		JavaRDD<String> words =lines.flatMap(new FlatMapFunction<String, String>() {
			
			private static final long serialVersionUID = 1L;

			
			public Iterator<String> call(String line) throws Exception {
				
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
	
		
		//接着。需要将每个单词映射为<单词，1>的key-value对形式
		//因为只有这样，才能根据单词作为key,来进行单词出现的次数进行累加
		//mapToPair,其实将每个元素，映射为<v1,v2>为 tuple2这样类型的元素
		//这里tuple2就是scala中的tuplede 类型,包含两个参数
		//mapTopair这个算子，要求的是与PairFunction配合使用，第一个泛型参数代表输入类型
			//第二个和第三个泛型参数，输出是tuple2的第一个值和第二个值的类型
		//JavaPairRDD 两个泛型参数分别代表了tuple2元素的第一个值和第二个值的类型
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
		//这里使用reduceByKey算子，对每个Key的对应的value，都进行reduce操作
		//比如JavaPairRDD中有几个元素，分别为<hello,1>,<word,1>,<hello,1>,<hello,1>
		//reduce 操作，相当于第一个值和第二个值进行计算，然后再将结果与第三个值进行计算
		//比如这里的hello,那就相当于，首先是1+1=2，然后在将2+1=3
		//最后返回的JavaPairRDD的元素也是tuple2，但是第一个值就是每个Key，第二个值就是每个Key的Value
		//reduce 之后的结果，就相当于每个单词出现的次数
		JavaPairRDD<String, Integer> wordCounts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return v1+v2;
					}
				});
		
		//到这里为止，通过几个Spark的算子操作，已经统计出了单词的次数
		//但是，之前我们使用的flatMap，mapTopair，reduceByKey这种操作，都叫做transformation操作
		//一个spark应用中，光是有transformation操作，是不会执行的，必须有action操作
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
