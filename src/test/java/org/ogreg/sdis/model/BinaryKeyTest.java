package org.ogreg.sdis.model;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Arrays;
import java.util.Random;

import org.ogreg.sdis.model.BinaryKey;
import org.testng.annotations.Test;

/**
 * Tests the immutability and the accessors of the {@link BinaryKey}.
 * 
 * @author gergo
 */
@Test(groups = "functional")
public class BinaryKeyTest {

	private static final Random Rnd = new Random(0);

	/**
	 * Ensures that the key is immutable.
	 */
	public void testImmutability() throws Exception {

		// Changing the original array should not affect the key
		byte[] bytes = new byte[20];
		BinaryKey key = new BinaryKey(bytes);
		assertEquals(key.toString(), "0000000000000000000000000000000000000000");
		bytes[0] = 1;
		assertEquals(key.toString(), "0000000000000000000000000000000000000000");
	}

	/**
	 * Ensures that {@link BinaryKey#compareTo(BinaryKey)} adheres to the contract.
	 */
	public void testCompare() throws Exception {
		// These are not proofs, just spot-checking

		// sgn(a.compareTo(b)) == -sgn(b.compareTo(a))
		for (int i = 0; i < 100; i++) {
			BinaryKey a = randomKey(), b = randomKey();
			assertEquals(Math.signum(a.compareTo(b)), -Math.signum(b.compareTo(a)));
		}

		// IF a.compareTo(b) > 0 AND b.compareTo(c) > 0 THEN a.compareTo(c) > 0
		for (int i = 0; i < 100; i++) {
			BinaryKey a = randomKey(), b = randomKey(), c = randomKey();

			if (a.compareTo(b) > 0 && b.compareTo(c) > 0) {
				assertTrue(a.compareTo(c) > 0);
			}
		}

		// IF a.compareTo(b) = 0 THEN sgn(a.compareTo(c)) == sgn(b.compareTo(c))
		// IF a.compareTo(b) = 0 THEN a.equals(b)
		for (int i = 0; i < 100; i++) {
			BinaryKey a = randomKey(), b = new BinaryKey(a.toByteArray()), c = randomKey();

			if (a.compareTo(b) == 0) {
				assertEquals(Math.signum(a.compareTo(c)), Math.signum(b.compareTo(c)));
				assertEquals(a, b);
			}
		}
	}

	/**
	 * Tests {@link BinaryKey#hashCode()} and {@link BinaryKey#equals(Object)} just for coverage.
	 */
	public void testHashCodeEquals() throws Exception {
		BinaryKey a = randomKey();

		assertEquals(a, a);
		assertFalse(a.equals(null));
		assertFalse(a.equals(""));

		assertEquals(a.hashCode(), a.hashCode());
		assertEquals(a.hashCode(), a.clone().hashCode());
	}

	/**
	 * Ensures that various cloning operations are done properly.
	 */
	public void testCloning() throws Exception {
		int[] volat = new int[5];
		BinaryKey a = new BinaryKey(volat);
		BinaryKey b = a.clone();

		// Cloning creates a new object
		assertEquals(a, b);
		assertNotSame(a, b);

		// Cloning copies the internal representation
		volat[0] = 1;
		assertNotEquals(a, b);

		// toByteArray is the same as the input array
		byte[] input = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9 };
		BinaryKey c = new BinaryKey(input);
		assertTrue(Arrays.equals(c.toByteArray(), input));

		// toByteArray clones the contents
		byte[] aarr = a.toByteArray();
		byte[] barr = a.toByteArray();
		assertEquals(aarr, barr);
		assertNotSame(aarr, barr);
	}

	/**
	 * Tests {@link BinaryKey#xor(BinaryKey)}.
	 */
	public void testXOR() throws Exception {
		assertEquals(Arrays.toString(key(0, 0, 0, 0, 1).xor(key(0, 0, 0, 0, 1))), "[0, 0, 0, 0, 0]");
		assertEquals(Arrays.toString(key(1, 0, 1, 0, 0).xor(key(0, 0, 0, 0, 1))), "[1, 0, 1, 0, 1]");
		assertEquals(Arrays.toString(key(0xFFFFFFFF, 0, 0, 0, 1).xor(key(0x7FFFFFFF, 0, 0, 0, 1))),
				"[-2147483648, 0, 0, 0, 0]");
	}

	/**
	 * Ensures error cases are handled properly.
	 */
	public void testErrors() throws Exception {

		// Key byte arrays should be a multiple of 4
		try {
			new BinaryKey(new byte[19]);
			fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
		}

		// Keys should be exactly 20 bytes long
		try {
			new BinaryKey(new byte[24]);
			fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
		}
	}

	BinaryKey randomKey() {
		int[] value = new int[BinaryKey.LENGTH_INTS];
		for (int i = 0; i < BinaryKey.LENGTH_INTS; i++) {
			value[i] = Rnd.nextInt();
		}
		return new BinaryKey(value);
	}

	BinaryKey key(int... value) {
		return new BinaryKey(value);
	}
}
