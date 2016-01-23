package org.systemexception.sparktutorial.pojo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author leo
 * @date 23/01/16 22:18
 */
public class SparkContext {

	private final SparkConf sparkConf;
	private final JavaSparkContext sparkContext;
	private final Logger logger;

	public SparkContext() {
		sparkConf = new SparkConf().setAppName("org.systemexception.sparktutorial").setMaster("local");
		sparkContext = new JavaSparkContext(sparkConf);
		logger = sparkConf.log();
	}

	public JavaSparkContext getSparkContext() {
		return sparkContext;
	}

	public void processFile(String fileName, String outputFolder) {
		logger.info("Start processing...");
		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = sparkContext.textFile(fileName);

		// Java 8 with lambdas: split the input string into words
		JavaRDD<String> words = input.flatMap(s -> Arrays.asList( s.split( " " ) ) );

		// Java 8 with lambdas: transform the collection of words into pairs (word and 1) and then count them
		JavaPairRDD<String, Integer> counts = words.mapToPair(t -> new Tuple2( t, 1 ) ).reduceByKey( (x, y) -> (int)x + (int)y );

		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFolder);
		logger.info("End processing");
	}
}