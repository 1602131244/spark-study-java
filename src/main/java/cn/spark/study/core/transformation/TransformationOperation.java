package cn.spark.study.core.transformation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * transformation操作实战
 * 
 * @author GYJ 2017-10-31
 */
public class TransformationOperation {

	public static void main(String[] args) {
//		map();
//		filter();
//		flatMap();
//		groupByKey();
//		reduceByKey();
		sortByKey();
	}

	/**
	 * map算子案例，将集合中的每一个元素都乘以2
	 */
	@SuppressWarnings("unused")
	private static void map() {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
							.setAppName("map")
							.setMaster("local");

		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 通过并行化集合的方式创建RDD，那么调用SparkContext以及其子类，的parallelize()
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

		// 使用map算子将集合中的每个元素都乘以2
		// map算子对任何类型的算子都可以调用
		// Java中，map算子的接收参数为Function对象
		// 创建的Function对象，一定会让你设置第二个泛型参数类型，这个泛型参数类型，就是返回的新元素类型
			//同时call()方法返回的类型，必须与第二个泛型参数同步
		//call()方法内部，对原始RDD每个元素进行各种处理和计算，并返回一个新元素
		//所有的新元素会组成一个新的RDD
		JavaRDD<Integer> multipleNumbersRDD = numberRDD
				.map(new Function<Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					public Integer call(Integer v1) throws Exception {
						return v1 * 2;
					}

				});
		//打印新的RDD:2,4,6,8,10
		multipleNumbersRDD.foreach(new VoidFunction<Integer>() {
	
			private static final long serialVersionUID = 1L;

			public void call(Integer t) throws Exception {
				System.out.println(t);
				
			}
		});
		
		// close JavaSparkContext
		sc.close();
	}
	
	
	/**
	 * filter 算子案例，过滤集合中的偶数
	 */
	@SuppressWarnings("unused")
	private static void filter(){
		// 创建SparkConf
		SparkConf conf = new SparkConf()
						.setAppName("map")
						.setMaster("local");

		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 通过并行化集合的方式创建RDD，那么调用SparkContext以及其子类，的parallelize()
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		//初始化RDD
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		//使用filter算子,过滤其中的偶数
		//filter算子，传入的也是Function ,其他的使用注意点和map一样
		//但是唯一不同的是，call()方法的返回类型是Boolean
		//每一个初始的RDD，都会传入call()方法，此时你可以执行各种自定义的计算逻辑
		//来判断这个元素是否是你想要的
		//如果你想在新到的RDD，保存这个元素，则返回true;不想保留这个元素，则返回false
		
		JavaRDD<Integer> evenNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {

			private static final long serialVersionUID = 1L;
			//在这里，1都10都会传进来，
			//但是根据我们的逻辑，只会保留2,4,6,8,10这个几个偶数，返回true
			//所以，只有偶数保存下来放在新的RDD中
			public Boolean call(Integer v1) throws Exception {
	
				return v1 % 2 ==0;
			}
		});
		
		//新的RDD
		evenNumberRDD.foreach(new VoidFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			public void call(Integer t) throws Exception {
				System.out.println(t);
				
			}
		});
		//关闭 JavaSparkContext
		sc.close();
		
	}
	
	/**
	 * 
	 * flatMap案列：将文本行拆分成多个单词
	 */
	@SuppressWarnings("unused")
	private static void flatMap(){
		//创建SparkConf
		SparkConf conf = new SparkConf()
							.setAppName("flatMap")
							.setMaster("local");
		//创建JavaSaprkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构造集合
		List<String>  lines = Arrays.asList("hello you","hello me","hello world");
		
		//并行化集合创建RDD
		JavaRDD<String> lineRDD = sc.parallelize(lines);
		
		//对lineRDD执行flatMap算子，将每一行文本拆分成多个单词
		//flatMap算子，在java中,接收的参数为FlatMapFunction
		//我们需要自定义FlatMapFunction的第二个泛型参数，代表返回新元素的类型
		//call()方法，返回的类型，不是U，而是Iterator<U>,这里的U也与第二个泛型参数相同
		//filMap其实就是，接收原始RDD中的每个元素，并进行各种逻辑的计算和处理，返回可以返回多个元素
		//多个元素，即封装在Iterator集合汇中，可以使用ArrayList集合等
		//新的RDD中，封装了所有的新元素，也就是说，新的RDD的大小一定是 >= 原始RDD的大小
		JavaRDD<String> wordRDD = lineRDD.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;
			
			//在这里，比如传图第一行：hello you
			//返回的是一个Iterator<String>(hello you) 
			
			public Iterator<String> call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(v1.split(" ")).iterator();
			}
		});
		//打印新的RDD
		wordRDD.foreach(new VoidFunction<String>() {

			
			private static final long serialVersionUID = 1L;

			public void call(String t) throws Exception {
				System.out.println("word's split : " +t);
				
			}
		}); 
		
		
		//关闭JavaSaprkContext
		sc.close();
	}
	
	
	/**
	 *groupByKey案列：按照班级对成绩进行分组
	 * 
	 */
	@SuppressWarnings({ "unchecked", "unused" })
	private static void groupByKey(){
		//创建SparkConf
		SparkConf conf = new SparkConf()
							 .setAppName("groupByKey")
							 .setMaster("local");
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//模拟集合
		List<Tuple2<String, Integer>> scoresList = Arrays.asList(
				new Tuple2<String,Integer>("112011",97),
				new Tuple2<String,Integer>("112012",67),
				new Tuple2<String,Integer>("112011",80),
				new Tuple2<String,Integer>("112012",76));
		
		//并行化集合,创建JavaPairRDD
		JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoresList);
		
		//执行groupByKey算子操作，对每个班级的成绩进行分组
		//groupByKey算子，返回的还是JavaPairRDD
		//但是，JavaPairRDD 的第一个泛型类型不变，第二个泛型类型变为Iterable<>这种集合类型
		//也就是说，按照了Key进行了分组，那么每个Key可能有多个value，此时多个value聚合成了Iterable
		//那么接下来，我们可以通过groupScore这种JavaPairRDD，很方便的处理某个分组内的数据
		JavaPairRDD<String, Iterable<Integer>> groupScore = scores.groupByKey();
		
		//打印groupScore
		groupScore.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Iterable<Integer>> t)
					throws Exception {
				System.out.println("class : " + t._1 );
				Iterator<Integer> ite = t._2.iterator();
				while (ite.hasNext()) {
					System.out.println("score : " + ite.next() );					
				}
				System.out.println("==========================================");
			}
		});
		//关闭JavaSparkContext
		sc.close();
	}
	
	
	/**
	 *reduceByKey案列：统计每个班级的总分
	 * 
	 */
	@SuppressWarnings({ "unchecked", "unused" })
	private static void reduceByKey(){
		//创建SparkConf
		SparkConf conf = new SparkConf()
							 .setAppName("reduceByKey")
							 .setMaster("local");
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//模拟集合
		List<Tuple2<String, Integer>> scoresList = Arrays.asList(
				new Tuple2<String,Integer>("112011",97),
				new Tuple2<String,Integer>("112012",67),
				new Tuple2<String,Integer>("112011",80),
				new Tuple2<String,Integer>("112012",76));
		
		//并行化集合,创建JavaPairRDD
		JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoresList);
		
		//针对scores RDD，执行reduceByKey算子
		//reduceByKey，接收的参数为Function2类型，它有三个泛型参数，实际上代表三个值
		//第一个和第二个泛型类型，代表了原始RDD中的元素的value类型，
				//因此对每个Key进行reduce,都会一次将第一个，第二个value传入，将值与第三个value值传入
		//第三个泛型类型，代表了每次reduce操作返回值的类型，默认也是与原始RDD的value类型相同的
		//reduceByKey算子返回的RDD，还是JavaPairRDD<Key,Value>
		JavaPairRDD<String, Integer> totalScores = scores.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		});
		
		//打印 totalScores
		totalScores.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			private static final long serialVersionUID = 1L;
			//对每个Key，都会将其value，依次传入call方法
			//从而聚合出每个Key对应的一个Value
			//然后，将每个Key对应的Value，组合成一个Tuple2，作为新RDD元素
			public void call(Tuple2<String, Integer> t) throws Exception {
				
				System.out.println("class : "+t._1 + " totalScores : " + t._2);
			}
		});
		//关闭JavaSparkContext
		sc.close();
	}
	
	
	/**
	 * sortByKey案列：按照学生的分数进行排序
	 */
	@SuppressWarnings({ "unchecked" })
	private static void sortByKey(){
		//创建SparkConf
		SparkConf conf = new SparkConf()
							 .setAppName("sortByKey")
							 .setMaster("local");
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//模拟集合
	
		List<Tuple2<Integer,String>> sortList = Arrays.asList(
				new Tuple2<Integer,String>(65, "leo"),
				new Tuple2<Integer,String>(50, "tom"),
				new Tuple2<Integer,String>(100, "marry"),
				new Tuple2<Integer,String>(80, "jack"));
		
		//并行化集合,创建JavaPairRDD
		JavaPairRDD<Integer,String> scores = sc.parallelizePairs(sortList);
		
		//针对scores RDD，执行sortByKey算子
		//sortByKey，其实就是根据Key进行排序，可以手动进行升序或者降序
		//返回的，还是JavaPairRDD，其中的元素内容，都是和原始的RDD一模一样
		//但是就是RDD的元素顺序不同了
		JavaPairRDD<Integer,String> sortScores = scores.sortByKey(false);
		
		//打印 totalScores
		sortScores.foreach(new VoidFunction<Tuple2<Integer,String>>() {

			private static final long serialVersionUID = 1L;

			public void call(Tuple2<Integer, String> t) throws Exception {
				
				System.out.println(t._2 + " Grades : " + t._1);
		
			}
		});
		//关闭JavaSparkContext
		sc.close();
	}
	
}
