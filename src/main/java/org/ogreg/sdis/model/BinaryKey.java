package org.ogreg.sdis.model;

import java.util.Arrays;

import org.ogreg.sdis.CommonUtil;

/**
 * An immutable 160-bit binary key.
 * <p>
 * The value is stored in an <code>int[5]</code>. The int values are treated as if they were unsigned. The value is
 * interpreted as a BigEndian number.
 * <p>
 * Used at various places, but most notably as nodeId, rpcId and message key for the
 * {@link org.ogreg.sdis.kademlia.Server}.
 * 
 * @author gergo
 */
public final class BinaryKey implements Comparable<BinaryKey>, Cloneable {

	/**
	 * The number of integers used to identify nodes and store and retrieve data.
	 */
	public static final int LENGTH_INTS = 5;

	/**
	 * The size in bits of the keys used to identify nodes and store and retrieve data.
	 */
	public static final int LENGTH_BITS = LENGTH_INTS * 4 * 8;

	/**
	 * This mask is used to obtain the value of an int as if it were unsigned.
	 */
	private static final long LONG_MASK = 0xffffffffL;

	private final int[] value;

	public BinaryKey(byte[] bytes) {
		this(CommonUtil.toIntArray(bytes));
	}

	/**
	 * Creates a {@link BinaryKey} using the specified int array.
	 * <p>
	 * Warning: the input array is passed by reference, not copied (for performance)!
	 * 
	 * @param value
	 */
	public BinaryKey(int[] value) {
		if (value.length != LENGTH_INTS) {
			throw new IllegalArgumentException("BinaryKey should be " + LENGTH_INTS * 4 + " bytes long (was "
					+ value.length * 4);
		}
		this.value = value;
	}

	/**
	 * @return A new int[5] holding the result of (this XOR other).
	 */
	public int[] xor(BinaryKey other) {
		int[] xor = new int[LENGTH_INTS];
		for (int i = 0; i < LENGTH_INTS; i++) {
			xor[i] = value[i] ^ other.value[i];
		}
		return xor;
	}

	@Override
	public int compareTo(BinaryKey o) {
		for (int i = 0; i < LENGTH_INTS; i++) {
			int tv = value[i];
			int ov = o.value[i];
			if (tv != ov) {
				return ((tv & LONG_MASK) < (ov & LONG_MASK)) ? -1 : 1;
			}
		}
		return 0;
	}

	@Override
	public BinaryKey clone() {
		int[] copy = new int[LENGTH_INTS];
		System.arraycopy(value, 0, copy, 0, LENGTH_INTS);
		return new BinaryKey(copy);
	}

	/**
	 * Creates a byte array representation from this key.
	 * 
	 * @return
	 */
	public byte[] toByteArray() {
		return CommonUtil.toByteArray(value);
	}

	@Override
	public String toString() {
		return CommonUtil.toHexString(value);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(value);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BinaryKey other = (BinaryKey) obj;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}
}