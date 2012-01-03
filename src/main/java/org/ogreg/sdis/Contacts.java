package org.ogreg.sdis;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * A collection storing Kademlia {@link Contact}s in {@link Bucket}s.
 * 
 * @author gergo
 */
class Contacts {

	/**
	 * The array of buckets storing {@link Contact}s. The size of the array is always
	 * {@link BinaryKey#KADEMLIA_KEY_BITS}.
	 */
	private final Bucket[] buckets;

	/**
	 * The current node id of this node, used for distance calculation.
	 */
	private final BinaryKey currentNodeId;

	public Contacts(BinaryKey currentNodeId) {
		this.currentNodeId = currentNodeId;
		this.buckets = new Bucket[BinaryKey.KADEMLIA_KEY_BITS];

		for (int i = 0; i < buckets.length; i++) {
			buckets[i] = new Bucket();
		}
	}

	/**
	 * Adds or updates the specified contact.
	 * 
	 * @param contact
	 * @return The first contact if the corresponding bucket is already full, or null if the new contact was added
	 *         successfully
	 */
	public Contact add(Contact contact) {
		int index = currentNodeId.logarithmOfDistance(contact.nodeId);

		if (index < 0) {
			// This would mean that we are communicating with ourselves, which shouldn't be possible
			throw new IllegalArgumentException(
					"Node distance is zero. You probably tried to add the current node to its own contact list");
		}

		Bucket bucket = buckets[index];
		return bucket.add(contact);
	}

	/**
	 * A collection storing {@link Contact}s sorted by the time of the most recent communication.
	 * 
	 * @author gergo
	 */
	private static class Bucket {

		private final BlockingDeque<Contact> contacts = new LinkedBlockingDeque<Contact>();

		/**
		 * Adds the contact to this bucket.
		 * 
		 * @param contact
		 * @return The first contact if the bucket is already full, or null if the new contact was added successfully
		 */
		public synchronized Contact add(Contact contact) {
			if (!contacts.offerFirst(contact)) {
				return getFirst();
			} else {
				return null;
			}
		}

		/**
		 * Returns the most recent contact in this bucket.
		 * 
		 * @return
		 */
		public synchronized Contact getFirst() {
			return contacts.getFirst();
		}

		/**
		 * Removes the contact from this bucket.
		 * 
		 * @return
		 */
		public synchronized void remove(Contact contact) {
			contacts.remove(contact);
		}
	}
}

/**
 * An immutable Kademlia entity storing contact information about a node.
 * 
 * @author gergo
 */
class Contact {

	final BinaryKey nodeId;

	final InetSocketAddress address;

	public Contact(BinaryKey nodeId, InetSocketAddress address) {
		this.nodeId = nodeId;
		this.address = address;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((address == null) ? 0 : address.hashCode());
		result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
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
		Contact other = (Contact) obj;
		if (address == null) {
			if (other.address != null)
				return false;
		} else if (!address.equals(other.address))
			return false;
		if (nodeId == null) {
			if (other.nodeId != null)
				return false;
		} else if (!nodeId.equals(other.nodeId))
			return false;
		return true;
	}
}