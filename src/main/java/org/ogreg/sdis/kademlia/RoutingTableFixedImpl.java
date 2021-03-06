package org.ogreg.sdis.kademlia;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.ogreg.sdis.CommonUtil;
import org.ogreg.sdis.model.BinaryKey;

/**
 * A fixed-length k-bucket implementation of the {@link RoutingTable}.
 * <p>
 * Stores k-buckets in a fixed table (160 entries) which avoids copying. This implementation is threadsafe. Access to
 * buckets is synchronized using a single read/write lock.
 * 
 * @author gergo
 */
public class RoutingTableFixedImpl implements RoutingTable {

	private final Deque<Contact>[] buckets;

	private final BinaryKey currentNodeId;

	private final int maxBucketSize;

	private final Lock readLock;

	private final Lock writeLock;

	@SuppressWarnings("unchecked")
	public RoutingTableFixedImpl(BinaryKey currentNodeId, int maxBucketSize) {
		this.currentNodeId = currentNodeId;
		this.maxBucketSize = maxBucketSize;
		this.buckets = new Deque[BinaryKey.LENGTH_BITS];
		for (int i = 0; i < buckets.length; i++) {
			buckets[i] = new LinkedList<Contact>();
		}
		ReadWriteLock lock = new ReentrantReadWriteLock();
		this.readLock = lock.readLock();
		this.writeLock = lock.writeLock();
	}

	@Override
	public void update(Contact contact, ReplaceAction action) {
		if (currentNodeId.equals(contact.nodeId)) {
			return;
		}

		Deque<Contact> bucket = getBucket(contact.nodeId);

		try {
			writeLock.lock();

			// If the contact already exists, we update it
			if (bucket.remove(contact)) {
				bucket.addFirst(contact);
				return;
			}

			// If the bucket is full, we invoke the replace operation on the least recently used contact
			if (bucket.size() >= maxBucketSize) {
				Contact old = bucket.removeLast();
				contact = action.replace(old, contact);
			}

			// We update the contact
			bucket.addFirst(contact);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public void remove(Contact contact) {
		if (currentNodeId.equals(contact.nodeId)) {
			return;
		}

		try {
			writeLock.lock();
			getBucket(contact.nodeId).remove(contact);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public List<Contact> getClosestTo(BinaryKey nodeId, int k) {
		List<Contact> results = new ArrayList<Contact>(k);

		// Calculating distance from current node
		int[] distance = currentNodeId.xor(nodeId);
		int[] order = getBucketOrder(distance);

		try {
			readLock.lock();

			outer: for (int idx : order) {
				Deque<Contact> bucket = buckets[idx];

				if (!bucket.isEmpty()) {
					for (Contact contact : bucket) {
						results.add(contact);
						k--;
						if (k == 0) {
							break outer;
						}
					}
				}
			}

			return results;
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * Returns the bucket for the specified key using the XOR metric.
	 * 
	 * @param nodeId
	 * @return
	 * @throws ArrayIndexOutOfBoundsException
	 *             if the nodeId is the same as the {@link #currentNodeId}.
	 */
	private Deque<Contact> getBucket(BinaryKey nodeId) {
		int[] distance = currentNodeId.xor(nodeId);
		int logDistance = BinaryKey.LENGTH_BITS - 1 - CommonUtil.nlz(distance);
		return buckets[logDistance];
	}

	/**
	 * Determines the bucket iteration order from the distance vector.
	 * <p>
	 * First we iterate through all the set bits of the distance, from left to right. Next we iterate through all the
	 * unset bits from right to left - and note the order of these bits. The resulting array holds the indices of the
	 * buckets in ascending order.
	 * 
	 * @param distance
	 * @return
	 */
	static int[] getBucketOrder(int[] distance) {
		int len = distance.length;
		int[] order = new int[len * Integer.SIZE];

		int i0 = order.length - 1;
		int i1 = 0;
		int o = order.length - 1;

		// Iterating through the distance components from left to right
		for (int i = 0; i < len; i++) {
			int v = distance[i];

			// Iterating through the bits from left to right
			for (int j = 0; j < Integer.SIZE; j++, o--) {
				if (v < 0) {
					// If the leftmost bit is set, we add the index from the beginning
					order[i1++] = o;
				} else {
					// Otherwise we add it from the end
					order[i0--] = o;
				}
				v <<= 1;
			}
		}

		return order;
	}
}
