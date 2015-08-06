package com.abhijit;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {

	public static void main(String[] args) {

		String logFile = "/home/vc_hadoop/spark-1.4.0-bin-hadoop2.6/README.md"; 
		JavaSparkContext sc = new JavaSparkContext("local", "Simple App",
				"$YOUR_SPARK_HOME", new String[]{"target/ScalaApp.jar"});
		JavaRDD<String> logData = sc.textFile(logFile).cache();

		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("a"); }
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("b"); }
		}).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

	}

}
