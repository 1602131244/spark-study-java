package cn.spark.study.core.sort;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 分组统计:对每个班级内的学生成绩，取出前三名，（分组取 Top n）
 * 
 * @author GYJ 
 * 2017-11-06
 */

public class GroupTop3 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("GroupTop3").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc
				.textFile("C://Users//Administrator//Desktop//score.txt");

		// 每行数据切割创建元组
		JavaPairRDD<String, Integer> pairs = lines
				.mapToPair(new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String v1)
							throws Exception {
						String[] lineSplited = v1.split(" ");
						return new Tuple2<String, Integer>(lineSplited[0],
								Integer.valueOf(lineSplited[1]));
					}
				});
		//分组
		JavaPairRDD<String, Iterable<Integer>> groupPairs = pairs.groupByKey();
		
		//统计前三数值
		JavaPairRDD<String, Iterable<Integer>> top3Scores = groupPairs.mapToPair(
				new PairFunction<Tuple2<String,Iterable<Integer>>, String,Iterable<Integer>>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String,Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> classScore)
					throws Exception {
				Integer[] top3 = new Integer[3];
				String className = classScore._1;
				Iterator<Integer> scores = classScore._2.iterator();
				while (scores.hasNext()){
					Integer score = scores.next();
					for(int i = 0;i<3 ;i++){
						if(top3[i] == null){
							top3[i] = score;
							break;
						}else if(score > top3[i]){
							for(int j =2; j>i; j--){
								top3[j] = top3[j-1];
							}
							top3[i] = score ;
							break;
						}
					}
				}
				return new Tuple2<String, Iterable<Integer>>(
						className,Arrays.asList(top3));
			}
		}); 
		
		//打印结果
		top3Scores.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			
			
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
				System.out.println("className : "+t._1);
				Iterator<Integer> scoreIterable = t._2.iterator();
				while(scoreIterable.hasNext()){
					Integer scoreInteger =  scoreIterable.next();
					System.out.println("score : " + scoreInteger);
					
				}
				System.out.println("=====================================");
			}
		});
		
		
		
		// 关闭 JavaSparkContext
		sc.close();
	}

}
