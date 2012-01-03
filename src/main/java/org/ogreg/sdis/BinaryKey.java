package org.ogreg.sdis;

import java.util.Arrays;

/**
 * An immutable 160-bit binary key.
 * <p>
 * The value is stored in an <code>int[5]</code>. The int values are treated as if they were unsigned.
 * <p>
 * Used at various places, but most notably as nodeId, rpcId and message key for the
 * {@link org.ogreg.sdis.KademliaServer}.
 * 
 * @author gergo
 */
public final class BinaryKey implements Comparable<BinaryKey> {

	/**
	 * The length of the key.
	 */
	private static final int KEY_LENGTH = 5;

	/**
	 * The length of Kademlia keys in bits.
	 */
	public static final int KADEMLIA_KEY_BITS = KEY_LENGTH * 4 * 8;

	/**
	 * This mask is used to obtain the value of an int as if it were unsigned.
	 */
	private static final long LONG_MASK = 0xffffffffL;

	private final int[] value;

	public BinaryKey(byte[] bytes) {
		this(CommonUtil.toIntArray(bytes));
	}

	private BinaryKey(int[] value) {
		if (value.length != KEY_LENGTH) {
			throw new IllegalArgumentException("BinaryKey should be " + KEY_LENGTH * 4 + " bytes long (was "
					+ value.length * 4);
		}
		this.value = value;
	}

	@Override
	public int compareTo(BinaryKey o) {
		for (int i = 0; i < KEY_LENGTH; i++) {
			int tv = value[i];
			int ov = o.value[i];
			if (tv != ov) {
				return ((tv & LONG_MASK) < (ov & LONG_MASK)) ? -1 : 1;
			}
		}
		return 0;
	}

	/**
	 * Returns the floor of the logarithm (<code>i</code>) of the distance between this key and <code>val</code>.
	 * <p>
	 * <code>
	 * 2<sup>i</sup> <= distance(this, val) < 2<sup>i+1</sup>
	 * </code>
	 * 
	 * @return The floor of the logarithm of the distance, -1 if the distance is 0
	 */
	public int logarithmOfDistance(BinaryKey val) {
		// Using that floor(log2(x)) = bits - 1 - nlz(x)
		int nlz = 0;
		for (int i = 0; i < KEY_LENGTH; i++) {
			int d = value[i] ^ val.value[i];
			if (d == 0) {
				nlz += 32;
			} else {
				nlz += Integer.numberOfLeadingZeros(d);
				break;
			}
		}
		return KEY_LENGTH - 1 - nlz;
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