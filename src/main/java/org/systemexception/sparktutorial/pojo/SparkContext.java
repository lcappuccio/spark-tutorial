package org.systemexception.sparktutorial.pojo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author leo
 * @date 23/01/16 22:18
 */
public class SparkContext {

	private static final Logger logger = LoggerFactory.getLogger(SparkContext.class);
	private transient final JavaSparkContext sparkContext;

	public SparkContext() {
		SparkConf sparkConf = new SparkConf().setAppName("org.systemexception.sparktutorial").setMaster("local");
		PropertyConfigurator.configure(Thread.currentThread().getContextClassLoader().getResource("log4j.properties"));
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
		JavaPairRDD<String, Integer> counts = words.mapToPair(t -> new Tuple2(t, 1))
				.reduceByKey((x, y) -> (int) x + (int) y);

		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFolder);
	}

	public void countChars(String fileName, String outputFolder) {
		JavaRDD<String> input = sparkContext.textFile(fileName);
		List<String> fileContent = input.collect();
		List<String> stringBuffer = new ArrayList<>();
		for (String fileLine : fileContent) {
			for (char fileChar : fileLine.toCharArray()) {
				stringBuffer.add(String.valueOf(fileChar));
			}
		}
		JavaRDD<String> chars = sparkContext.parallelize(stringBuffer);
		JavaPairRDD<String, Integer> counts = chars.mapToPair(t -> new Tuple2(t, 1))
				.reduceByKey((x, y) -> (int) x + (int) y);
		counts.saveAsTextFile(outputFolder);
	}

	public void filterFile(String fileName, String outputFolder, String textToFilter) {
		JavaRDD<String> input = sparkContext.textFile(fileName);
		JavaRDD<String> filteredLines = input.filter(s -> s.contains(textToFilter));
		filteredLines.saveAsTextFile(outputFolder);
	}

	public void sequenceFile(String fileName, String outputFolder) {
		// Will fail but leave as an example
		JavaPairRDD<Text, IntWritable> hadoopData = sparkContext.sequenceFile(fileName, Text.class, IntWritable.class);
		JavaPairRDD<String, Integer> hadoopDataConverted = hadoopData.mapToPair(
				(PairFunction<Tuple2<Text, IntWritable>, String, Integer>) textIntWritableTuple2 ->
						new Tuple2<String, Integer>(textIntWritableTuple2._1.toString()
								, textIntWritableTuple2._2.get()));
		hadoopDataConverted.saveAsTextFile(outputFolder);
	}

	public void loadJson(String fileName, String outputFolder) {
		logger.info("Load JSON file");
		SQLContext sqlContext = new SQLContext(sparkContext);
		DataFrame data = sqlContext.read().json(fileName);
		JavaRDD dataRdd = data.toJavaRDD();
		dataRdd.saveAsTextFile(outputFolder);
	}

	public int countBlanksAccumulator(String fileName, String outputFolder) {
		logger.info("Generate accumulator for blank lines in uuid-list.txt");
		JavaRDD<String> file = sparkContext.textFile(fileName);
		final Accumulator<Integer> blanksCounter = sparkContext.accumulator(0);
		JavaRDD<String> fileMap = file.flatMap(
				(FlatMapFunction<String, String>) line -> {
					if (("").equals(line)) {
						blanksCounter.add(1);
					}
					return Arrays.asList(line.split(" "));
				});
		fileMap.saveAsTextFile(outputFolder);
		return blanksCounter.value();
	}
}
