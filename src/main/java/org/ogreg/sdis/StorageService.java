package org.ogreg.sdis;

import java.nio.ByteBuffer;


/**
 * Common interface for services which support block storage.
 * 
 * @author gergo
 */
public interface StorageService {

	/**
	 * Returns the data at the specified key, or null if none exists.
	 * 
	 * @param key
	 * @return
	 */
	ByteBuffer load(BinaryKey key);

	/**
	 * Stores the data at the specified key.
	 * 
	 * @param key
	 * @param data
	 */
	void store(BinaryKey key, ByteBuffer data);
}
