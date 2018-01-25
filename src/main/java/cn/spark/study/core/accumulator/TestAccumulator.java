package cn.spark.study.core.accumulator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * accumulator累加变量
 * @author GYJ
 * 2017-11-02
 */
@SuppressWarnings("deprecation")
public class TestAccumulator {
	public static void main(String[] args) {
		//创建SparkConf
		SparkConf conf= new SparkConf()
							.setAppName("TestAccumulator")
							.setMaster("local");
		//创建JavaSaprkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//创建Accumulator变量
		//需要调用JavaSparkContext中的accumulator()方法
		final Accumulator<Integer> sumAccumlator = sc.accumulator(0);
		List<Integer> numberList= Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numberList);
		
		//打印数据
		numberRDD.foreach(new VoidFunction<Integer>() {
		
			private static final long serialVersionUID = 1L;

			public void call(Integer t) throws Exception {
				//然后在函数内部，就可以对Accumulator变量，调用add()方法，进行累加
				sumAccumlator.add(t);
				
			}
		});
		//在driver程序中，可以调用value()方法获取其值
		System.out.println(sumAccumlator.value());
		//关闭JavaSparkContext
		sc.close();
	}
}
