package org.systemexception.sparktutorial.main;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author leo
 * @date 23/01/16 18:26
 */
public class Main {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("org.systemexception.sparktutorial").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
	}
}
