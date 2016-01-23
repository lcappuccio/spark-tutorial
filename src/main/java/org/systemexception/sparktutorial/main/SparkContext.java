package org.systemexception.sparktutorial.main;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author leo
 * @date 23/01/16 22:18
 */
public class SparkContext {

	private static final SparkConf sparkConf = new SparkConf().setAppName("org.systemexception.sparktutorial")
			.setMaster("local");
	private static final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

	public JavaSparkContext getSparkContext() {
		return sparkContext;
	}
}
