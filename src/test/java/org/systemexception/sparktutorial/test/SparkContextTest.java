package org.systemexception.sparktutorial.test;

import org.junit.AfterClass;
import org.junit.Test;
import org.systemexception.sparktutorial.pojo.SparkContext;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

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
	public void sut_count_words() throws IOException {
		URL url = this.getClass().getResource("/test_file.txt");
		File testFile = new File(url.getFile());

		assertTrue(testFile.exists());

		String test_output_folder = "target" + File.separator + convertTime(System.currentTimeMillis())
				+ "_words_output";
		sut.countWords(testFile.getAbsolutePath(), test_output_folder);

		assertTrue(new File(test_output_folder).exists());
		String output = Arrays.toString(readOutputFile(test_output_folder).toArray());
		assertTrue(output.contains("(aaaa,1)"));
		assertTrue(output.contains("(bbb,1)"));
		assertTrue(output.contains("(cc,1)"));
		assertTrue(output.contains("(d,2)"));
	}

	@Test
	public void sut_count_chars() throws IOException {
		URL url = this.getClass().getResource("/test_file.txt");
		File testFile = new File(url.getFile());

		assertTrue(testFile.exists());

		String test_output_folder = "target" + File.separator + convertTime(System.currentTimeMillis())
				+ "_char_output";
		sut.countChars(testFile.getAbsolutePath(), test_output_folder);

		String output = Arrays.toString(readOutputFile(test_output_folder).toArray());
		assertTrue(new File(test_output_folder).exists());
		assertTrue(output.contains("(a,4)"));
		assertTrue(output.contains("(b,3)"));
		assertTrue(output.contains("(c,2)"));
		assertTrue(output.contains("(d,2)"));
	}

	private String convertTime(long time) {
		Date date = new Date(time);
		Format format = new SimpleDateFormat("yyyyMMddHHmmss");
		return format.format(date);
	}

	private List<String> readOutputFile(String fileName) throws IOException {
		return Files.readAllLines(Paths.get(fileName + File.separator + "part-00000"), Charset.defaultCharset());
	}

}