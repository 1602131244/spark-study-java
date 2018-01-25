package cn.spark.study.sql.parquet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;



/**
 * Parquet数据源之合并元数据
 * @author GYJ
 * 2017-12-7
 */
public class ParquetMergeSchema {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("ParquetMergeSchema")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();
		List<Square> squares = new ArrayList<Square>();
		for (int value = 1; value <= 5; value++) {
		  Square square = new Square();
		  square.setValue(value);
		  square.setSquare(value * value);
		  squares.add(square);
		}

		// Create a simple DataFrame, store into a partition directory
		Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
		squaresDF.write().mode(SaveMode.Append).parquet("hdfs://spark1:9000/spark-study/Square-java");

		List<Cube> cubes = new ArrayList<Cube>();
		for (int value = 6; value <= 10; value++) {
		  Cube cube = new Cube();
		  cube.setValue(value);
		  cube.setCube(value * value * value);
		  cubes.add(cube);
		}

		
		Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
		cubesDF.write().mode(SaveMode.Append).parquet("hdfs://spark1:9000/spark-study/Square-java");
        
		// Read the partitioned table
		Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("hdfs://spark1:9000/spark-study/Square-java");
		mergedDF.printSchema();
		mergedDF.show();

		
	}
	
	public static class Square implements Serializable {

		private static final long serialVersionUID = 1L;
		private int value;
		private int square;

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public int getSquare() {
			return square;
		}

		public void setSquare(int square) {
			this.square = square;
		}

		@Override
		public String toString() {
			return "Square [value=" + value + ", square=" + square + "]";
		}

		// Getters and setters...

	}

	public static class Cube implements Serializable {
		
		private static final long serialVersionUID = 1L;
		private int value;
		private int cube;

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public int getCube() {
			return cube;
		}

		public void setCube(int cube) {
			this.cube = cube;
		}

		@Override
		public String toString() {
			return "Cube [value=" + value + ", cube=" + cube + "]";
		}

	}
}
