package org.ogreg.sdis.kademlia;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.ogreg.sdis.BinaryKey;

/**
 * A collection storing Kademlia {@link Contact}s in {@link Bucket}s.
 * 
 * @author gergo
 */
class Contacts {

	/**
	 * The maximum number of contacts stored in a Kademlia bucket (k).
	 */
	static final int MAX_CONTACTS = 20;

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
		this.buckets = new Bucket[BinaryKey.LENGTH_BITS];

		for (int i = 0; i < buckets.length; i++) {
			buckets[i] = new Bucket(MAX_CONTACTS);
		}
	}

	/**
	 * Adds or updates the specified contact.
	 * 
	 * @param contact
	 * @return The least recently seen contact if the corresponding bucket is already full, or null if the new contact
	 *         was added successfully
	 */
	public Contact update(Contact contact) {
		return getBucket(contact.nodeId).update(contact);
	}

	/**
	 * Adds the specified contact, removing the least recently used contact if its bucket is full.
	 * 
	 * @param contact
	 */
	public void add(Contact contact) {
		getBucket(contact.nodeId).add(contact);
	}

	/**
	 * Removes the specified contact.
	 * 
	 * @param contact
	 */
	public void remove(Contact contact) {
		getBucket(contact.nodeId).remove(contact);
	}

	/**
	 * Returns {@link #MAX_CONTACTS} nodes closest to the specified key.
	 * 
	 * @param key
	 * @return
	 */
	public List<Contact> getClosestTo(BinaryKey key) {
		// TODO Auto-generated method stub
		return null;
	}

	// Returns the closest bucket for the specified node ID
	private Bucket getBucket(BinaryKey nodeId) {
		int index = currentNodeId.logarithmOfDistance(nodeId);

		if (index < 0) {
			// This would mean that we are communicating with ourselves, which shouldn't be possible
			throw new IllegalArgumentException(
					"Node distance is zero. You probably tried to add the current node to its own contact list");
		}

		return buckets[index];
	}

	/**
	 * A collection storing {@link Contact}s sorted by the time of the most recent communication.
	 * 
	 * @author gergo
	 */
	private static class Bucket {

		private final BlockingDeque<Contact> contacts;

		public Bucket(int maxContacts) {
			this.contacts = new LinkedBlockingDeque<Contact>(maxContacts);
		}

		/**
		 * Adds the contact to this bucket, or updates the existing contact.
		 * 
		 * @param contact
		 * @return The least recently seen contact if the bucket is already full, or null if the new contact was added
		 *         successfully
		 */
		public synchronized Contact update(Contact contact) {
			remove(contact);
			if (!contacts.offerLast(contact)) {
				return contacts.getFirst();
			} else {
				return null;
			}
		}

		/**
		 * Adds the contact to this bucket, removing the least recently used contact if the bucket is full.
		 * 
		 * @param contact
		 */
		public synchronized void add(Contact contact) {
			remove(contact);
			if (!contacts.offerLast(contact)) {
				contacts.removeFirst();
			}
			contacts.offerLast(contact);
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