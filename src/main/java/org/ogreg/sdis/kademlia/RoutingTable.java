package org.ogreg.sdis.kademlia;

import java.net.InetSocketAddress;
import java.util.Collection;

import org.ogreg.sdis.BinaryKey;

/**
 * Common interface for Kademlia routing table implementations.
 * 
 * @author gergo
 */
interface RoutingTable {

	/**
	 * Adds or updates the specified contact.
	 * <p>
	 * If the given table segment is full, <code>action</code> is invoked to determine which contact to use, with an
	 * input parameter of the least recently used contact. The resulting {@link Contact} becomes the most recently used
	 * element of the segment.
	 * 
	 * @param contact
	 * 
	 * @return The least recently seen contact if contact can not be added, or null if it was added successfully
	 */
	void update(Contact contact, ReplaceAction action);

	/**
	 * Removes the specified contact.
	 * 
	 * @param contact
	 */
	void remove(Contact contact);

	/**
	 * Tries to return at most <code>k</code> {@link Contact}s closest to <code>nodeId</code>.
	 * 
	 * @param nodeId
	 * @param k
	 * @return
	 */
	Collection<Contact> getClosestTo(BinaryKey nodeId, int k);
}

/**
 * A replace function to be executed on a given {@link Contact}.
 * 
 * @author gergo
 */
interface ReplaceAction {

	/**
	 * Chooses from <code>old</code> or <code>contact</code>.
	 * 
	 * @param old
	 * @param contact
	 * @return
	 */
	Contact replace(Contact old, Contact contact);
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

	@Override
	public String toString() {
		return "Contact [nodeId=" + nodeId + ", address=" + address + "]";
	}
}
