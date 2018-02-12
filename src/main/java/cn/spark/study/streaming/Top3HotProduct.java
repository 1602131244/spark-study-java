package cn.spark.study.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 与spark SQL整合使用
 * 每隔10秒，统计最近60秒大的，每隔种类的每个商品的点击次数，
 * 然后统计出每个种类top3热门的商品
 * @author GYJ
 * 2018-2-12
 *
 */
@SuppressWarnings("deprecation")
public class Top3HotProduct {

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf()
		.setMaster("local[2]")
		.setAppName("Top3HotProduct");

        JavaStreamingContext jssc = new JavaStreamingContext(
        		conf,
        		Durations.seconds(1));
        
        //首先看一下输入日志的格式
        //name product category
        //leo iphone mobile_phone
        
        
        //首先获取数据输入流
        //spark streaming 基于socket，
        //因为方便
        //其实，企业里面最长的，是Kafka数据源，
        //不同的数据源，不同之处，只是在创建输入的DStream的那一点点代码
        //核心是在于之后的Spark streaming 的实时计算
        //所以只要掌握各个案例和功能的使用
        
        //获取输入数据流
         
        JavaReceiverInputDStream<String> productClickLogsDStream = jssc.socketTextStream("localhost", 9999);
        
        //然后做一个映射，将每个种类的每个商品，映射为（category_product,1）的这种格式
        //从而在后面可以使用window操作，对窗口中的这种格式的数据，进行reduceByKey操作
        //从而统计出来，一个窗口中的每个种类的每个商品的，点击次数
        
        JavaPairDStream<String, Integer> categoryProductPairDStream = productClickLogsDStream.mapToPair(new  PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String productClickLog) throws Exception {
			    String[] productClickLogSplited = productClickLog.split(" ");
			    
				return new Tuple2<String, Integer>(
						productClickLogSplited[2] + "_" + productClickLogSplited[1],
						1);
			}
		});
        
        
        //然后执行window操作
        //到这里就可以做到，每隔10秒钟，对最近60秒的数据，执行reduceByKey操作
        //计算出来这60秒内，每个种类，每个商品的点击次数
        JavaPairDStream<String, Integer> categoryProductCountsDStream = categoryProductPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			public Integer call(Integer V1, Integer V2) throws Exception {
				// TODO Auto-generated method stub
				return V1+V2;
			}
		}, Durations.seconds(60),Durations.seconds(10));
        
        
        //然后针对60秒内的每个种类的每个商品的点击次数
        //foreachRDD ,在内部使用Spark SQL执行，top3热门商品的统计数据
        
        categoryProductCountsDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
			
			
			private static final long serialVersionUID = 1L;

			public void call(JavaPairRDD<String, Integer> categoryProductCount) throws Exception {
				// 将改RDD，转换为JavaRDD<Row>的格式
				JavaRDD<Row> categoryProductCountRowRDD = categoryProductCount.map(new Function<Tuple2<String,Integer>, Row>() {

					private static final long serialVersionUID = 1L;

					public Row call(Tuple2<String, Integer> categoryProductCount)
							throws Exception {
						String category = categoryProductCount._1.split("_")[0];
						String product = categoryProductCount._1.split("_")[1];
						Integer count = categoryProductCount._2;
						return RowFactory.create(category,product,count);
					}
				});
			
				//然后执行DataFrame转换
				
				List<StructField> structFields = new ArrayList<StructField>();
				structFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
				structFields.add(DataTypes.createStructField("product", DataTypes.StringType, true));
				structFields.add(DataTypes.createStructField("click_count", DataTypes.IntegerType, true));
				
				StructType strcuType = DataTypes.createStructType(structFields);
				
                HiveContext hiveContext = new HiveContext(categoryProductCount.context());
                
                Dataset<Row> categoryProductCountDF = hiveContext.createDataFrame(
                		categoryProductCountRowRDD, 
                		strcuType);
                
                
                
                
                //将60秒内的每个种类的每个商品的点击次数的数据，注册为一个临时表
                
                categoryProductCountDF.createOrReplaceTempView("product_click_log");
                
                //执行SQL语句
                //针对临时表，统计出来每个种类的下，点击次数排名前三的热名商品
                
                String sqlString = "SELECT category,product,click_count "
                		+ "FROM ("
                			+ "SELECT "
                				+ "category,"
                				+ "product,"
                				+ "click_count,"
                				+ "row_number() OVER (PARTITION BY category ORDER BY click_count DESC) rank "
                		    + "FROM prouct_click_log"
                		+ ") tmp "
                	    + " where rank <=3";
                Dataset<Row> top3ProductDF = hiveContext.sql(sqlString);
                
                
                //这里说明一下， 在企业场景中，可以不是打印的
                //而是将数据保存到Redis缓存，或者是MySQL DB中
                
                top3ProductDF.show();
			}
		});
        
        
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
	}
}
