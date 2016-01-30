package org.systemexception.sparktutorial.pojo;


import org.apache.spark.Partitioner;

/**
 * @author leo
 * @date 30/01/16 17:47
 */
public class CustomPartitioner extends Partitioner {

	@Override
	public int numPartitions() {
		return 0;
	}

	@Override
	public int getPartition(Object o) {
		return 0;
	}
}
