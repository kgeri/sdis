package org.ogreg.sdis.storage;

import java.nio.ByteBuffer;
import java.util.Map;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.ogreg.sdis.StorageService;
import org.ogreg.sdis.model.BinaryKey;

/**
 * A simple in-memory storage service (for testing purposes only).
 * 
 * @author gergo
 */
public class InMemoryStorageServiceImpl implements StorageService {

	private final Map<BinaryKey, ByteBuffer> store = new ConcurrentHashMap<BinaryKey, ByteBuffer>();

	@Override
	public ByteBuffer load(BinaryKey key) {
		ByteBuffer buffer = store.get(key);
		return buffer == null ? null : (ByteBuffer) buffer.asReadOnlyBuffer().rewind();
	}

	@Override
	public void store(BinaryKey key, ByteBuffer data) {
		store.put(key, data);
	}
}
