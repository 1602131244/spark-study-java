package cn.spark.study.core.action;

import java.util.Arrays;

import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * action 操作实战
 * @author GYJ
 * 2017-11-01
 *
 */
public class ActionOperation {
	public static void main(String[] args) {
//		reduce();
//		collect();
		count();
//		take();
//		saveAsTextFile();
//		countByKey();
	}
	@SuppressWarnings("unused")
	private static void reduce(){
		//创建SaprkConf
		SparkConf conf = new SparkConf()
						 .setAppName("reduce")
						 .setMaster("local");
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//有一个集合1到10,10个数字，现在对10个数字进行累加
		List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberRDD = sc.parallelize(numberList);
		
		//使用reduce操作对集合中的数字进行累加
		//reduce 操作原理：
				//首先将第一个元素和第二个元素传入call()方法，进行计算，会获取一个结果，比如：1+2=3
				//接着将改结果和下一个元素传入call()方法，进行计算，比如3+3=6
				//以此类推
		//所以reduce操作的本质，就是聚合，将多个元素聚合成一个元素
		int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
		
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {

				return v1 + v2;
			}
		});
		//打印结果
		System.out.println("List's total is : " + sum);
		
		//关闭JavaSparkContext
		sc.close();
	}
	
	@SuppressWarnings("unused")
	private static void collect(){
		// 创建SaprkConf
		SparkConf conf = new SparkConf()
						.setAppName("collect")
						.setMaster("local");
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//有一个集合1到10,10个数字，现在对10个数字进行累加
		List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberRDD= sc.parallelize(numberList);
		
		//使用map操作对 所有集合乘以2
		JavaRDD<Integer> doubleNumbers = numberRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1) throws Exception {

				return v1 * 2;
			}
		});
		
		//不用foreach action 操作，在远程集群上遍历RDD中的元素
		//而使用collect操作，将分布在远程集群上的doubleNumbers RDD 的数据拉取到本地
		//这种方式一般不建议使用,因为RDD中的数据量比较大的话，比如超过一万条
			//那么性能会比较大，因为要从远程走大量的网络传输，将数据传输到本地
			//此外，除了性能差，还可能在RDD中数据量特别大的情况下，发生oom异常，内存溢出
		//因此，通常，还是推荐foreach action 操作，来对最终RDD元素进行处理
		
		List<Integer>  doubleNumberList = doubleNumbers.collect();
		for (Integer num : doubleNumberList) {
			System.out.println("ListArray : "+num);
		}
		//关闭JavaSparkContext
		sc.close();
	}
	
	private static void  count() {
		// 创建SaprkConf
		SparkConf conf = new SparkConf()
						.setAppName("count")
						.setMaster("local");
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
//		//有一个集合1到10,10个数字，现在对10个数字进行累加
//		List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
//		JavaRDD<Integer> numberRDD= sc.parallelize(numberList);
		
		//初始化RDD，lines,每个元素是一行文本
		JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//spark.txt");
		
		//对RDD进行count操作，统计它有多少个元素
//		long count = numberRDD.count();
		long count = lines.count(); 
		//打印结果
		System.out.println("TextFile hava " + count  + " lines");
		//关闭JavaSparkContext
		sc.close();
	}
	
	@SuppressWarnings("unused")
	private static void take(){
		// 创建SaprkConf
		SparkConf conf = new SparkConf()
						.setAppName("take")
						.setMaster("local");
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//有一个集合1到10,10个数字，现在对10个数字进行累加
		List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberRDD= sc.parallelize(numberList);
		
		//take操作，与collect类似，也是从远程集群上获取RDD的数据，
		//collect 是获取RDD所有的数据，take只是获取RDD的前N个 数据
		List<Integer> top3Numbers = numberRDD.take(3);
		
		for (Integer num : top3Numbers) {
			System.out.println("ListArray : "+num);
		}
		//关闭JavaSparkContext
		sc.close();
	}
	
	@SuppressWarnings("unused")
	private static void saveAsTextFile(){
		// 创建SaprkConf
		SparkConf conf = new SparkConf()
						.setAppName("saveAsTextFile");
					
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//有一个集合1到10,10个数字，现在对10个数字进行累加
		List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberRDD= sc.parallelize(numberList);
		
		//使用map操作对 所有集合乘以2
		JavaRDD<Integer> doubleNumbers = numberRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1) throws Exception {

				return v1 * 2;
			}
		});
		
		//直接将文件保存到Hdfs文件系统上
		//但是要注意，我们这里只能指定文件夹，也就是目录
		//那么实际上回保存为part-0000文件
		doubleNumbers.saveAsTextFile("hdfs://spark1:9000/double_number.txt");
		//关闭JavaSparkContext
		sc.close();
	}
	
	@SuppressWarnings("unused")
	private static void countByKey() {
		//创建SparkConf
		SparkConf conf = new SparkConf()
							 .setAppName("countByKey")
							 .setMaster("local");
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//模拟集合
		@SuppressWarnings("unchecked")
		List<Tuple2<String, String>> scoresList = Arrays.asList(
				new Tuple2<String,String>("112011","leo"),
				new Tuple2<String,String>("112012","jack"),
				new Tuple2<String,String>("112011","marry"),
				new Tuple2<String,String>("112012","tom"),
				new Tuple2<String,String>("112012","david"));		
		//并行化集合,创建JavaPairRDD
		JavaPairRDD<String, String> students = sc.parallelizePairs(scoresList);
		
		//对RDD应用countByKey操作，统计每个班的学生人数，也就是每个Key对应的元素个数
		Map<String, Long> studentCounts = students.countByKey();
		for(Map.Entry<String, Long> studentCount:studentCounts.entrySet()){
			System.out.println(studentCount.getKey()+ " : " + studentCount.getValue());
		}
		//关闭JavaSparkContext
		sc.close();

	}
}
