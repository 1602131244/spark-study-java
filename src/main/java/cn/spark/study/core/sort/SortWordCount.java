package cn.spark.study.core.sort;

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
 * 排序的WordCount程序
 * 
 * @author GYJ 2017-11-03
 */
public class SortWordCount {
	public static void main(String[] args) {
		// 创建SparkCconf 和JavaSparkContext
		SparkConf conf = new SparkConf().setAppName("SortWordCount").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 创建lines RDD，
		JavaRDD<String> lines = sc
				.textFile("C://Users//Administrator//Desktop//spark.txt");

		// 将行数据拆分成每个单词
		JavaRDD<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {

					private static final long serialVersionUID = 1L;

					public Iterator<String> call(String line) throws Exception {
						// TODO Auto-generated method stub
						return Arrays.asList(line.split(" ")).iterator();
					}
				});
		// 将单词组合成Tuple2()格式
		JavaPairRDD<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String v1)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, Integer>(v1, 1);
					}
				});
		// 将每个单词统计出现的次数
		JavaPairRDD<String, Integer> wordCounts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					public Integer call(Integer v1, Integer v2)
							throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				});
		//到这里为止，就得到了每个单词的出现的次数
		//但是问题是，我们的新需求是，要讲每个单词出现的次数，将降序排序
		//wordCounts 内的元素是Tuple2<String,Integer>：(hello,3),(you,2)
		//我们需要将RDD转换成(3,hello),(2,you)格式，才能对根据单词出现的次数进行排序
		
		//进行Key-Value 反转映射
		
		
		
		JavaPairRDD<Integer, String> countWords = wordCounts.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, String> call(Tuple2<String, Integer> v1)
					throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, String>(v1._2, v1._1);
			}
		});
		// 按照Key进行排序
		JavaPairRDD<Integer, String> sortCountWords = countWords.sortByKey(false);
		
		//再次将value-key进行反转映射
		JavaPairRDD<String, Integer> sortWordCounts = sortCountWords.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(Tuple2<Integer, String> t)
					throws Exception {
				return new Tuple2<String, Integer>(t._2,t._1);
			}
		});
		//获取到降序排序的结果
		// 遍历排序结果
		sortWordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1 + " appears: " + t._2 + " times . ");

			}
		});
		// 关闭JavaSparkContext
		sc.close();
	}
}
