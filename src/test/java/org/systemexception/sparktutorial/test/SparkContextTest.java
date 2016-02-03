package org.systemexception.sparktutorial.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
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

	@BeforeClass
	public static void setUp() {
		sut = new SparkContext();

		assertTrue(sut != null);
		assertTrue(sut.getSparkContext() != null);
	}

	@AfterClass
	public static void tearDown() {
		sut.getSparkContext().close();
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

	@Test
	public void sut_filter_lines() throws IOException {
		URL url = this.getClass().getResource("/test_file.txt");
		File testFile = new File(url.getFile());

		assertTrue(testFile.exists());

		String test_output_folder = "target" + File.separator + convertTime(System.currentTimeMillis())
				+ "_filter_output";
		sut.filterFile(testFile.getAbsolutePath(), test_output_folder, "aaaa");

		String output = Arrays.toString(readOutputFile(test_output_folder).toArray());
		assertTrue(new File(test_output_folder).exists());
		assertTrue("[aaaa]".equals(output));
	}

	@Test
	public void sut_load_sequence_file() throws Exception {
		URL url = this.getClass().getResource("/sequence-test-file-0");
		File testFile = new File(url.getFile());

		assertTrue(testFile.exists());

		String test_output_folder = "target" + File.separator + convertTime(System.currentTimeMillis())
				+ "_sequence_output_0";
		sut.sequenceFile(testFile.getAbsolutePath(), test_output_folder);
		String output = Arrays.toString(readOutputFile(test_output_folder).toArray());
		assertTrue(new File(test_output_folder).exists());
		assertTrue("[(Red,5)]".equals(output));

		url = this.getClass().getResource("/sequence-test-file-1");
		testFile = new File(url.getFile());

		assertTrue(testFile.exists());

		test_output_folder = "target" + File.separator + convertTime(System.currentTimeMillis())
				+ "_sequence_output_1";
		sut.sequenceFile(testFile.getAbsolutePath(), test_output_folder);
		output = Arrays.toString(readOutputFile(test_output_folder).toArray());
		assertTrue(new File(test_output_folder).exists());
		assertTrue("[(Green,0), (Blue,20)]".equals(output));
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