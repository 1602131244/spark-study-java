package cn.spark.study.core.sort;

import java.util.List;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
/**
 * 取最大的前三个数字
 * @author GYJ
 * 2017-11-03
 * 
 */
public class Top3 {
	public static void main(String[] args) {
		//创建SparkConf  和 JavaSparkContext
		SparkConf conf = new SparkConf()
							 .setAppName("Top3")
							 .setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//获取行数据，创建RDD
		JavaRDD<String> lines = sc
				.textFile("C://Users//Administrator//Desktop//top.txt");
		
		JavaPairRDD<Integer, String>  pairs = lines.mapToPair(new PairFunction<String, Integer, String>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, String> call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, String>(Integer.valueOf(v1),v1);
			}
		});
		
		JavaPairRDD<Integer, String> sortPairs = pairs.sortByKey(false);
		
		JavaRDD<String> sortNumbers = sortPairs.map(
				new Function<Tuple2<Integer,String>, String>() {

		
			private static final long serialVersionUID = 1L;

			public String call(Tuple2<Integer, String> t) throws Exception {
				// TODO Auto-generated method stub
				return t._2;
			}
		});
		List<String> sortedNumberList = sortNumbers.take(3);
		for (String num : sortedNumberList) {
			System.out.println("List Top 3: "+ num);
		}
		
		//关闭sc
		sc.close();
				
	}
}
