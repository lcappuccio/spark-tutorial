package org.systemexception.sparktutorial.test;

import org.junit.After;
import org.junit.Test;
import org.systemexception.sparktutorial.pojo.SparkContext;

import static junit.framework.TestCase.assertTrue;

/**
 * @author leo
 * @date 23/01/16 22:19
 */
public class SparkContextTest {

	private SparkContext sut;

	@After
	public void tearDown() {
		sut.getSparkContext().close();
	}

	@Test
	public void sut_exists() {
		sut = new SparkContext();

		assertTrue(sut != null);
		assertTrue(sut.getSparkContext() != null);
	}

}