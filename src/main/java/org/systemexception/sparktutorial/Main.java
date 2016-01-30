package org.systemexception.sparktutorial;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.systemexception.sparktutorial.pojo.SparkContext;
import scala.Tuple2;

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
		JavaPairRDD<Long, String> pairRDD = sc.parallelizePairs(Arrays.asList(
				new Tuple2(1,"Hello World"), new Tuple2(2,"Goodbye World")));
		logger.info("Paired: " + pairRDD.collect());
		JavaPairRDD<Long, String> filteredPairRDD = pairRDD.filter(t -> t._2.contains("Hello"));
		logger.info("Paired filtered: " + filteredPairRDD.collect());

		logger.info("Map to pair and reduce");
		JavaPairRDD<String, Long> simpsonsPair = sc.parallelizePairs(Arrays.asList(
				new Tuple2("Homer", 1L), new Tuple2("Marge", 3L), new Tuple2("Homer", 4L),
				new Tuple2("Bart", 1L), new Tuple2("Lisa", 5L), new Tuple2("Bart", 3L)));
		JavaPairRDD<String, Tuple2<Long, Long>> simpsonsMapValued = simpsonsPair.mapValues(
				(Function<Long, Tuple2<Long, Long>>) aLong -> new Tuple2(aLong, 1L)
		);
		logger.info("Paired and mapped: " + simpsonsMapValued.collect());
		logger.info("Reduce by key");
		JavaPairRDD<String, Tuple2<Long, Long>> simponsMapReduced = simpsonsMapValued.reduceByKey(
				(Function2<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>) (longLongTupleX, longLongTupleY)
						-> new Tuple2<>(longLongTupleX._1 + longLongTupleY._1, longLongTupleX._2 + longLongTupleY._2)
		);
		logger.info("Reduced by key: " + simponsMapReduced.collect());

		logger.info("Join customers and purchases");
		JavaPairRDD customers = sc.parallelizePairs(Arrays.asList(
				new Tuple2("AAA", "Homer Simpson"), new Tuple2("AAB", "Marge Simspon"),
				new Tuple2("AAC", "Ned Flanders"), new Tuple2("AAD", "Lenny Leonard")));
		JavaPairRDD purchases = sc.parallelizePairs(Arrays.asList(new Tuple2("AAA", 1000L), new Tuple2("AAD", 2000L)));
		JavaPairRDD customerPurchases = customers.join(purchases);
		logger.info("Customer join purchases: " + customerPurchases.collect());
		JavaPairRDD customerRightOuterPurchases = customers.rightOuterJoin(customerPurchases);
		logger.info("Customer right join purchases: " + customerRightOuterPurchases.collect());
		JavaPairRDD customerLeftOuterPurchases = customers.leftOuterJoin(customerPurchases);
		logger.info("Customer left join purchases: " + customerLeftOuterPurchases.collect());

		JavaPairRDD unsortedData = sc.parallelizePairs(Arrays.asList(
				new Tuple2("D","Data4"), new Tuple2("A", "Data1"), new Tuple2("C", "Data3"), new Tuple2("B", "Data2")));
		logger.info("Unsorted data: " + unsortedData.collect());
		JavaPairRDD sortedData = unsortedData.sortByKey();
		logger.info("Sorted data: " + sortedData.collect());
	}
}
