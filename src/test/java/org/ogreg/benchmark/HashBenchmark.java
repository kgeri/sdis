package org.ogreg.benchmark;

import java.security.MessageDigest;
import java.util.Random;

import org.testng.annotations.Test;

@Test(groups = "benchmark")
public class HashBenchmark {

	public void testSHA1() throws Exception {
		MessageDigest d = MessageDigest.getInstance("SHA-1");
		Random r = new Random(0);

		byte[] input = new byte[4096];
		r.nextBytes(input);

		for (int i = 0; i < 100000; i++) {
			d.digest(input);
		}
	}
	
	public void testSHA256() throws Exception {
		MessageDigest d = MessageDigest.getInstance("SHA-256");
		Random r = new Random(0);

		byte[] input = new byte[4096];
		r.nextBytes(input);

		for (int i = 0; i < 100000; i++) {
			d.digest(input);
		}
	}
}
