package cn.spark.study.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;






import scala.Tuple2;

/**
 * 基于transform的实时黑名单过滤
 * @author GYJ
 * 2018-1-30
 *
 */
public class TransformBlackList {

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf()
		.setMaster("local[2]")
		.setAppName("TransformBlackList");


		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(5));
		//用户对我们的网站的广告可以 进行点击
		//点击之后，进行实时计费，点击一次，算一次钱
		//但是，对于那些无良商家刷广告的人，那么我们有一个黑名单，
		//只要黑名单中的用户点击的广告，我们就给过滤掉
		
		
		
		//先做一份模拟的黑名单RDD
		List<Tuple2<String, Boolean>> blackListData = new ArrayList<Tuple2<String,Boolean>>();
		blackListData.add(new Tuple2<String, Boolean>("tom", true));
		final JavaPairRDD<String, Boolean> blackListRDD = jssc.sparkContext().parallelizePairs(blackListData);
		
		
		
		
		
		//这里的日志格式，就简化一下，就是date username格式
		JavaReceiverInputDStream<String> adsClickLogDstream = jssc.socketTextStream("spark1", 9999);
		
		
		//所以先对输入的数据，进行转换操作，变成（username，date username）
		//以便于后面，对每个batch ,与定义好的黑名单RDD进行join 操作
		JavaPairDStream<String, String> usersAdsClickLogDStream = adsClickLogDstream.mapToPair(new PairFunction<String, String, String>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, String> call(String adsClickLog) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, String>(adsClickLog.split(" ")[1],adsClickLog);
			}
		});
		
		//然后，就可以执行transformation操作了，将每个batch的RDD ,与黑名单的RDD进行join，filter，map等操作
		
		//实时进行黑名单过滤
		JavaDStream<String> validAdsClickLogDStream = usersAdsClickLogDStream.transform(new Function<JavaPairRDD<String,String>, JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;

			public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD)
					throws Exception {
				//这里为什么用左外连接，
				//因为并不是每个用户都存在于黑名单
				//所以，如果直接用join，那么没有存在于黑名单中的数据，会无法join到
				//那就丢弃掉了
				//所以这里用leftOuterJoin ,哪怕一个user不在黑名单RDD中，没有join到
				//也还是会保存下来的
				JavaPairRDD<String, Tuple2<String,Optional<Boolean>>>  joinedRDD= 
						userAdsClickLogRDD.leftOuterJoin(blackListRDD);
				
				//连接之后，执行filter算子
				JavaPairRDD<String, Tuple2<String,Optional<Boolean>>> filterRDD = 
						joinedRDD.filter(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, Boolean>() {

							private static final long serialVersionUID = 1L;

							public Boolean call(
									Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple)
									throws Exception {
								// 这里的tuple,就是每个用户，对应的访问日志，和在黑名单中的
								//状态
								
								if(tuple._2._2().isPresent() && tuple._2._2.get()){
									return false;
								}
								return true;
							}
						});
				//此时，filterRDD 中，就只剩下没有被黑名单过滤的用户点击
				
				//进行map操作，转换成我们想要的格式
				
				JavaRDD<String> validAdsClickLogRDD = filterRDD.map(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, String>() {

					private static final long serialVersionUID = 1L;

					public String call(
							Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple)
							throws Exception {
						return tuple._2._1;
					}
				});
				return validAdsClickLogRDD;
			}
		});
		
		//打印有效的广告点击日志
		//其实在真是的应用场景中，这里后面就可以写入kafka，ActiveMQ等这种中间件消息队列
		//然后再开发一个专门的后台服务，作为广告计费服务，执行实时的广告计费，这里就是指拿到了有效的广告点击
		
		Thread.sleep(5000);
		validAdsClickLogDStream.print();
		
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}
}
