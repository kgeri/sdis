package org.ogreg.sdis;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.Random;

import org.testng.annotations.Test;

/**
 * Tests {@link CommonUtil}s methods for edge cases.
 * 
 * @author gergo
 */
@Test(groups = "functional")
public class CommonUtilTest {

	private static final Random Rnd = new Random(0);

	/**
	 * Ensures that conversion between byte and int arrays works correctly.
	 */
	public void testFromToIntArray() throws Exception {
		byte[] a = new byte[] { -128, -1, 0, 1, 127, 0, 0, 0 };
		int[] b = new int[] { 0x80FF0001, 0x7F000000 };

		// Edge case test
		assertEquals(CommonUtil.toIntArray(a), b);
		assertEquals(CommonUtil.toByteArray(b), a);

		// Random tests
		for (int i = 0; i < 1000; i++) {
			Rnd.nextBytes(a);
			b[0] = Rnd.nextInt();
			b[1] = Rnd.nextInt();

			assertEquals(CommonUtil.toByteArray(CommonUtil.toIntArray(a)), a);
			assertEquals(CommonUtil.toIntArray(CommonUtil.toByteArray(b)), b);
		}

		// Error test
		try {
			CommonUtil.toIntArray(new byte[5]);
			fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
		}
	}
}
