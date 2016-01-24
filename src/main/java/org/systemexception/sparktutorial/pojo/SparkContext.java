package org.systemexception.sparktutorial.pojo;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author leo
 * @date 23/01/16 22:18
 */
public class SparkContext {

	private final JavaSparkContext sparkContext;

	public SparkContext() {
		SparkConf sparkConf = new SparkConf().setAppName("org.systemexception.sparktutorial").setMaster("local");
		PropertyConfigurator.configure(Thread.currentThread().getContextClassLoader()
				.getResource("spark_log4j.properties"));
		sparkContext = new JavaSparkContext(sparkConf);
	}

	public JavaSparkContext getSparkContext() {
		return sparkContext;
	}

	public void countWords(String fileName, String outputFolder) {
		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = sparkContext.textFile(fileName);

		// Java 8 with lambdas: split the input string into words
		JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")));

		// Java 8 with lambdas: transform the collection of words into pairs (word and 1) and then count them
		JavaPairRDD<String, Integer> counts = words.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) ->
				(int) x + (int) y);

		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFolder);
	}

	public void countChars(String fileName, String outputFolder) {
		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = sparkContext.textFile(fileName);

		// Java 8 with lambdas: split the input string into chars
		List<String> fileContent = input.collect();
		List<String> stringBuffer = new ArrayList<>();
		for (String fileLine: fileContent) {
			for (char fileChar: fileLine.toCharArray()) {
				stringBuffer.add(String.valueOf(fileChar));
			}
		}
		JavaRDD<String> chars = sparkContext.parallelize(stringBuffer);

		// Java 8 with lambdas: transform the collection of chars into pairs (word and 1) and then count them
		JavaPairRDD<String, Integer> counts = chars.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) ->
				(int) x + (int) y);

		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFolder);
	}
}
