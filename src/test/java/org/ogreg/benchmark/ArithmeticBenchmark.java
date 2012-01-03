package org.ogreg.benchmark;

import java.util.Random;

import org.testng.annotations.Test;

@Test(groups = "benchmark")
public class ArithmeticBenchmark {

	public void testDivisionVsShift() throws Exception {
		byte[] rnd = new byte[10000000];
		new Random(0).nextBytes(rnd);

		{
			long before = System.nanoTime();
			int value = 0;
			for (int i = 0; i < rnd.length; i++) {
				value = rnd[i] / 8;
			}
			System.out.println(value);
			System.err.println("Division: " + ((System.nanoTime() - before) / 1000000.0) + "ms");
		}
		
		{
			long before = System.nanoTime();
			int value = 0;
			for (int i = 0; i < rnd.length; i++) {
				value = rnd[i] >> 3;
			}
			System.out.println(value);
			System.err.println("Shift: " + ((System.nanoTime() - before) / 1000000.0) + "ms");
		}
	}
}
