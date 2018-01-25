package cn.spark.study.core.broadcast;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * 广播变量
 * @author GYJ
 * 2017-11-02
 */
public class BroadcastVariable {
	public static void main(String[] args) {
		//创建SparkConf
		SparkConf conf = new SparkConf()
						 .setAppName("BroadcastVariable")
						 .setMaster("local");
		//创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//在Java中，创建共享变量，就是调用spark中的broadcast()方法，
		//获取的返回结果是Broadcast<T>类型
		
		final int factor = 3;
		final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);
		
		List<Integer> numberList = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numberList);
		
		JavaRDD<Integer> multipleNumbers = numberRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1) throws Exception {
				//调用共享变量时，调用value()方法，即可获取背部封装的值
				int factor = factorBroadcast.value();
				return v1 * factor;
			}
		});
		multipleNumbers.foreach(new VoidFunction<Integer>() {
			
			private static final long serialVersionUID = 1L;

			public void call(Integer t) throws Exception {
				System.out.println(t);
				
			}
		});
		//关闭JavaSparkContext
		sc.close();
	}
}
