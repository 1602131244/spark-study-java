package cn.spark.study.core.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
/**
 * 统计每行出现的次数
 * @author GYJ
 * 2017-10-30
 */
public class LineCount {
	public static void main(String[] args) {
		//创建SparkConf
		SparkConf conf = new SparkConf()
						 .setAppName("LineCount")
						 .setMaster("local");
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//初始化RDD，lines,每个元素是一行文本
		JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//hadoop.txt");
		
		//对lines RDD 执行pariToMap算子，将每行映射成（lines，1）这种<K,V>形式
		//然后后面对每行出现的次数
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
		//对pairs RDD 执行reduceByKey算子，算出每一行出现的次数
		JavaPairRDD<String, Integer> lineCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		});
		
		//输出lineCounts
		lineCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
		
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Integer> t) throws Exception {
				
				System.out.println(t._1+ " appears " + t._2 +" times .");
			}
		});
		
		//关闭JavaSparkContext
		sc.close();
		
	}
}
