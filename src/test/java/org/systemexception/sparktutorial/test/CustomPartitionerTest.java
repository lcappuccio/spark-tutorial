package org.systemexception.sparktutorial.test;

import org.junit.Test;
import org.systemexception.sparktutorial.pojo.CustomPartitioner;

import static org.junit.Assert.assertTrue;

/**
 * @author leo
 * @date 30/01/16 17:52
 */
public class CustomPartitionerTest {

	private CustomPartitioner sut = new CustomPartitioner();

	@Test
	public void delete_this_later() {
		assertTrue(0 == sut.numPartitions());
		assertTrue(0 == sut.getPartition("any object"));
	}
}