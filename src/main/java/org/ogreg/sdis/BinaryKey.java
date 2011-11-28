package org.ogreg.sdis;

import java.util.Arrays;

/**
 * A 160-bit binary key.
 * <p>
 * Used at various places, but most notably as nodeId, rpcId and message key for the {@link KademliaServer}.
 * 
 * @author gergo
 */
public final class BinaryKey {
	private final byte[] bytes;

	public BinaryKey(byte[] bytes) {
		if (bytes.length != 160) {
			throw new IllegalArgumentException("Keys should be 160 bits long (" + bytes.length + " != 160)");
		}
		this.bytes = bytes;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(bytes);
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
		if (!Arrays.equals(bytes, other.bytes))
			return false;
		return true;
	}
}