package org.systemexception.sparktutorial;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.systemexception.sparktutorial.pojo.SparkContext;

import java.util.Arrays;

/**
 * @author leo
 * @date 23/01/16 18:26
 */
public class Main {

	private static final Logger logger = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {
		logger.info("Create Spark Context");
		SparkContext sparkContext = new SparkContext();
		JavaSparkContext sc = sparkContext.getSparkContext();

		logger.info("Parallelize list of integers");
		JavaRDD<Integer> ints = sc.parallelize(Arrays.asList(1,2,3,4));
		JavaRDD<Integer> result = ints.map(x -> x * x);
		for (Integer integer: result.collect()) {
			logger.info("Item: " + integer);
		}

		logger.info("Parallelize Strings");
		JavaRDD<String> strings = sc.parallelize(Arrays.asList("Hello World", "Goodbye World"));
		JavaRDD<String> words = strings.flatMap(x -> Arrays.asList(x.split(" ")));
		for (String word: words.collect()) {
			logger.info("Words: " + word);
		}

		logger.info("Reduce Integers");
		Integer reduced = ints.reduce((x, y) -> x + y);
		logger.info("Reduced: " + reduced);

		logger.info("Map to pair");
		JavaPairRDD<String, Long> pairRDD = strings.zipWithIndex();
		logger.info("Paired: " + pairRDD.collect());
	}
}
