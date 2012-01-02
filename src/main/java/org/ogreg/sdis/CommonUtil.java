package org.ogreg.sdis;

/**
 * Various helper methods for common tasks.
 * 
 * @author gergo
 */
// TODO may use Apache commons later
public class CommonUtil {
	private static final char[] HexChars = "0123456789ABCDEF".toCharArray();

	/**
	 * Converts the <code>bytes</code> array to a hexadecimal, uppercase {@link String} representation.
	 * 
	 * @param bytes
	 * @return
	 */
	public static String toHexString(byte[] bytes) {
		char[] chars = new char[bytes.length * 2];
		int v;
		for (int j = 0; j < bytes.length; j++) {
			v = bytes[j] & 0xFF;
			chars[j * 2] = HexChars[v / 16];
			chars[j * 2 + 1] = HexChars[v % 16];
		}
		return new String(chars);
	}
}
