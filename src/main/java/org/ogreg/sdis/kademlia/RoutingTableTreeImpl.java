package org.ogreg.sdis.kademlia;

import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.ogreg.sdis.BinaryKey;

/**
 * A k-bucket tree based {@link RoutingTable}.
 * <p>
 * This collection is synchronized.
 * 
 * @author gergo
 */
public class RoutingTableTreeImpl implements RoutingTable {

	private final Node root = new Node(true, new LinkedList<Contact>());

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	private final BinaryKey currentNodeId;

	private final int maxBucketSize;

	public RoutingTableTreeImpl(BinaryKey currentNodeId, int maxBucketSize) {
		this.currentNodeId = currentNodeId;
		this.maxBucketSize = maxBucketSize;
	}

	@Override
	public void update(Contact contact, ReplaceAction action) {

		if (currentNodeId.equals(contact.nodeId)) {
			return;
		}

		try {
			lock.writeLock().lock();

			BinaryKey key = contact.nodeId;
			Node node = root;
			int i = 0;

			// find(contact.nodeId)
			for (; node.hasChildNodes() && i < BinaryKey.LENGTH_BITS; i++) {
				node = key.isSet(i) ? node.right : node.left;
			}

			// If the contact already exists, we update it
			if (node.contacts.remove(contact)) {
				node.contacts.addFirst(contact);
				return;
			}

			if (node.contacts.size() >= maxBucketSize) {
				if (node.maySplit) {

					// Splitting node based on the ith bit
					split(node, i);

					// Determining which child to add to
					node = key.isSet(i) ? node.right : node.left;
				} else {

					// If the bucket can't split, we replace the least recently used node with either itself, or the
					// new contact. The selected contact will then be added first
					Contact lru = node.contacts.removeLast();
					contact = action.replace(lru, contact);
				}
			}

			node.contacts.addFirst(contact);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public void remove(Contact contact) {
		try {
			lock.writeLock().lock();
			find(contact.nodeId).contacts.remove(contact);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public Collection<Contact> getClosestTo(BinaryKey key) {
		try {
			lock.readLock().lock();

			// TODO Auto-generated method stub
			return null;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public String toString() {
		return root.toString();
	}

	/**
	 * Splits the <code>node</code> in half - making it a tree node - based on the ith bit.
	 * <p>
	 * If the ith bit in {@link #currentNodeId} is one, then the left hand side of the tree will be "closed" (ie. that
	 * node can not split anymore). And if the ith bit is zero, then the right hand side of the tree will be closed. The
	 * contacts of <code>node</code> will be split between the two child nodes based on their ith bit (zeroes go to the
	 * left side, ones go to the right side).
	 * <p>
	 * This is exactly the same as splitting the key range in half, then moving keys less than the mediator in the left
	 * node, and moving keys greater than the mediator to the right node.
	 * 
	 * @param node
	 * @param i
	 */
	private void split(Node node, int i) {

		// We apply some pointer wizardry to avoid unnecessary copying.
		if (currentNodeId.isSet(i)) {
			// Closing the left side of the tree, because the currentNodeId is to the right
			node.left = new Node(false, new LinkedList<Contact>());
			node.right = new Node(true, node.contacts);
			node.contacts = null;

			// Moving contacts from right to left, if their ith bit is zero
			transferContacts(node.right, node.left, i, false);
		} else {
			// Closing the right side of the tree, because the currentNodeId is to the left
			node.left = new Node(true, node.contacts);
			node.right = new Node(false, new LinkedList<Contact>());
			node.contacts = null;

			// Moving contacts from left to right, if their ith bit is one
			transferContacts(node.left, node.right, i, true);
		}
	}

	/**
	 * Returns the only leaf node which might contain <code>key</code>.
	 * 
	 * @param key
	 * @return
	 */
	private Node find(BinaryKey key) {
		Node node = root;
		for (int i = 0; node.hasChildNodes() && i < BinaryKey.LENGTH_BITS; i++) {
			node = key.isSet(i) ? node.right : node.left;
		}
		return node;
	}

	/**
	 * Transfer contacts from <code>src</code> to <code>dst</code>, if their <code>i</code>th bit equals
	 * <code>value</code>.
	 * 
	 * @param src
	 * @param dst
	 * @param i
	 * @param value
	 */
	private void transferContacts(Node src, Node dst, int i, boolean value) {
		for (Iterator<Contact> it = src.contacts.iterator(); it.hasNext();) {
			Contact c = it.next();
			if (c.nodeId.isSet(i) == value) {
				it.remove();
				dst.contacts.addFirst(c);
			}
		}
	}

	// A routing table node
	private static final class Node {

		// These fields are set iff the node is a bucket
		private Deque<Contact> contacts = null;
		private boolean maySplit;

		// These fields are set iff the node is a tree node
		private Node left;
		private Node right;

		// Creates a new leaf node
		private Node(boolean maySplit, Deque<Contact> contacts) {
			this.maySplit = maySplit;
			this.contacts = contacts;
		}

		// Returns true if the node is a tree node
		private boolean hasChildNodes() {
			return contacts == null;
		}

		@Override
		public String toString() {
			if (hasChildNodes()) {
				return new StringBuilder().append('(').append(left).append(")(").append(right).append(')').toString();
			} else {
				StringBuilder buf = new StringBuilder();
				for (Contact contact : contacts) {
					buf.append(contact.nodeId.toString());
					buf.append(',');
				}
				return buf.length() == 0 ? "" : buf.substring(0, buf.length() - 1);
			}
		}
	}
}
