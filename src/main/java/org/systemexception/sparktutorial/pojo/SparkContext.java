package org.systemexception.sparktutorial.pojo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author leo
 * @date 23/01/16 22:18
 */
public class SparkContext {

	private final SparkConf sparkConf;
	private final JavaSparkContext sparkContext;

	public SparkContext() {
		sparkConf = new SparkConf().setAppName("org.systemexception.sparktutorial").setMaster("local");
		sparkContext = new JavaSparkContext(sparkConf);
	}

	public JavaSparkContext getSparkContext() {
		return sparkContext;
	}
}
