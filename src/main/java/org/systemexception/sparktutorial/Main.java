package org.systemexception.sparktutorial;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.systemexception.sparktutorial.pojo.SparkContext;

import java.util.Arrays;

/**
 * @author leo
 * @date 23/01/16 18:26
 */
public class Main {

	public static void main(String[] args) {
		SparkContext sparkContext = new SparkContext();
		JavaSparkContext sc = sparkContext.getSparkContext();

		JavaRDD<Integer> ints = sc.parallelize(Arrays.asList(1,2,3,4));
		JavaRDD<Integer> result = ints.map(x -> x * x);
		for (Integer integer: result.collect()) {
			System.out.println(integer);
		}

		JavaRDD<String> strings = sc.parallelize(Arrays.asList("Hello World", "Goodbye World"));
		JavaRDD<String> words = strings.flatMap(x -> Arrays.asList(x.split(" ")));
		for (String word: words.collect()) {
			System.out.println(word);
		}

		Integer reduced = ints.reduce((x, y) -> x + y);
		System.out.println(reduced);
	}
}
