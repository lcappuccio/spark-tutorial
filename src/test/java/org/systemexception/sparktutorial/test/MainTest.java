package org.systemexception.sparktutorial.test;

import org.junit.Test;
import org.systemexception.sparktutorial.Main;

/**
 * @author leo
 * @date 01/03/16 22:15
 */
public class MainTest {

	private static Main sut = new Main();

	@Test
	public void should_not_fail() {
		sut.main(null);
	}
}