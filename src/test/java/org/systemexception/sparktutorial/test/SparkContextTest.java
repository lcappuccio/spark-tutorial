package org.systemexception.sparktutorial.test;

import org.junit.AfterClass;
import org.junit.Test;
import org.systemexception.sparktutorial.pojo.SparkContext;

import java.io.File;
import java.net.URL;
import java.time.LocalDateTime;

import static junit.framework.TestCase.assertTrue;

/**
 * @author leo
 * @date 23/01/16 22:19
 */
public class SparkContextTest {

	private static SparkContext sut;

	@AfterClass
	public static void tearDown() {
		sut.getSparkContext().close();
	}

	@Test
	public void sut_exists() {
		sut = new SparkContext();

		assertTrue(sut != null);
		assertTrue(sut.getSparkContext() != null);
	}

	@Test
	public void sut_processes_file() {
		URL url = this.getClass().getResource("/lorem_ipsum.txt");
		File testFile = new File(url.getFile());

		assertTrue(testFile.exists());

		String test_output_folder = "target" + File.separator + LocalDateTime.now().toString() + "_test_output";
		sut.processFile(testFile.getAbsolutePath(), test_output_folder);

		assertTrue(new File(test_output_folder).exists());
	}

}