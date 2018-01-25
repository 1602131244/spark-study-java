package cn.spark.study.core.sort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import cn.spark.study.core.sort.impl.SecondarySortKey;

/**
 * 二次排序
 * 1.自定义实现的key,要实现Ordered接口和Serializable接口，在Key中实现自己对多个列的排序算法
 * 2.将包含文本的RDD，映射成可key为自定义key,value为文本的JavaPairRDD
 * 3.使用sortByKey算子，按照自定义的Key进行排序
 * 4.再次映射，提出自定义的key，只保留文本行
 * @author GYJ
 * 2017-11-03
 */
public class SecondarySort {
	public static void main(String[] args) {
		//创建SparkConf  和 JavaSparkContext
		SparkConf conf = new SparkConf()
							 .setAppName("SecondarySort")
							 .setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//获取行数据，创建RDD
		JavaRDD<String> lines = sc
				.textFile("C://Users//Administrator//Desktop//sort.txt");
		
		JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(
				new PairFunction<String, SecondarySortKey, String>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
				String[] lineSplited = line.split(" ");
				SecondarySortKey key = new SecondarySortKey(
						Integer.valueOf(lineSplited[0]), 
						Integer.valueOf(lineSplited[1]) );
				
				
				return new Tuple2<SecondarySortKey, String>(key, line);
			}
		});
		
		//进行排序
		
		JavaPairRDD<SecondarySortKey, String> sortPairs = pairs.sortByKey();
		
		//映射
		
		JavaRDD<String> sortedLine = sortPairs.map(
				new Function<Tuple2<SecondarySortKey,String>, String>() {

				
					private static final long serialVersionUID = 1L;

					public String call(Tuple2<SecondarySortKey, String> t)
							throws Exception {
						
						return t._2;
					}
		});
		
		
		//打印
		sortedLine.foreach(new VoidFunction<String>() {
			
			private static final long serialVersionUID = 1L;

			public void call(String t) throws Exception {
				System.out.println("sorted result : " + t);
				
			}
		});
		
		
		
		//关闭JavaSparkContext
		sc.close();
	}
}
