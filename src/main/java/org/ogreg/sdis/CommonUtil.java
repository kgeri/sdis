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
		for (int i = 0; i < chars.length; i += 2) {
			v = bytes[i / 2] & 0xFF;
			chars[i] = HexChars[v / 16];
			chars[i + 1] = HexChars[v % 16];
		}
		return new String(chars);
	}

	/**
	 * Converts the <code>values</code> array to a hexadecimal, uppercase {@link String} representation.
	 * 
	 * @param bytes
	 * @return
	 */
	public static String toHexString(int[] values) {
		char[] chars = new char[values.length * 8];
		int value, v;
		for (int i = 0; i < chars.length; i += 8) {
			value = values[i / 8];

			v = (value >> 24) & 0xFF;
			chars[i] = HexChars[v / 16];
			chars[i + 1] = HexChars[v % 16];

			v = (value >> 16) & 0xFF;
			chars[i + 2] = HexChars[v / 16];
			chars[i + 3] = HexChars[v % 16];

			v = (value >> 8) & 0xFF;
			chars[i + 4] = HexChars[v / 16];
			chars[i + 5] = HexChars[v % 16];

			v = value & 0xFF;
			chars[i + 6] = HexChars[v / 16];
			chars[i + 7] = HexChars[v % 16];
		}
		return new String(chars);
	}

	/**
	 * Packs the <code>bytes</code> to an int array.
	 * 
	 * @param bytes
	 * @return A new int array holding the same bytes as the input array
	 * @throws IllegalArgumentException
	 *             if the input length is not a multiple of 4
	 */
	public static int[] toIntArray(byte[] bytes) {
		if (bytes.length % 4 != 0) {
			throw new IllegalArgumentException("The input length must be a multiple of 4 (was " + bytes.length + ")");
		}

		int[] ret = new int[bytes.length / 4];
		int v;
		for (int i = 0; i < bytes.length; i += 4) {
			v = (bytes[i] & 0xFF) << 24;
			v |= (bytes[i + 1] & 0xFF) << 16;
			v |= (bytes[i + 2] & 0xFF) << 8;
			v |= (bytes[i + 3] & 0xFF);
			ret[i / 4] = v;
		}

		return ret;
	}

	/**
	 * Unpacks the <code>value</code> to a byte array.
	 * 
	 * @param value
	 * @return A new byte array holding the same integers as the input array
	 */
	public static byte[] toByteArray(int[] value) {
		byte[] ret = new byte[value.length * 4];
		int v;
		for (int i = 0; i < ret.length; i += 4) {
			v = value[i / 4];
			ret[i] = (byte) (v >> 24);
			ret[i + 1] = (byte) (v >> 16);
			ret[i + 2] = (byte) (v >> 8);
			ret[i + 3] = (byte) v;
		}

		return ret;
	}

	/**
	 * Returns the Number of Leading Zeros of the binary value specified in an int[].
	 * 
	 * @param value
	 * @return
	 */
	public static int nlz(int[] value) {
		int len = value.length;
		for (int i = 0; i < len; i++) {
			int d = value[i];
			if (d != 0) {
				return i * Integer.SIZE + Integer.numberOfLeadingZeros(d);
			}
		}
		return len * Integer.SIZE;
	}
}
