package org.ogreg.sdis.kademlia;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;

import org.ogreg.sdis.model.BinaryKey;
import org.testng.annotations.Test;

/**
 * Ensures that the {@link RoutingTableFixedImpl} handles {@link Contact}s as expected by the Kademlia specification.
 * 
 * @author gergo
 */
@Test(groups = { "functional" })
public class RoutingTableFixedImplTest {

	/**
	 * Tests that the update and get operations work correctly.
	 */
	public void testUpdate() throws Exception {
		RoutingTableFixedImpl rt = new RoutingTableFixedImpl(key(0, 0, 1, 0, 0), 2);
		ReplaceAction action = new AlwaysReplaceAction();
		
		// This must not have effect
		rt.update(contact(0, 0, 1, 0, 0), action);
		
		// These go to the same bucket
		rt.update(contact(0, 0, 0, 0, 1), action);
		rt.update(contact(0, 0, 0, 1, 1), action);
		
		// Updating already existing node
		rt.update(contact(0, 0, 0, 0, 1), action);
		
		// Bucket overflow, 00011 will be removed
		rt.update(contact(0, 0, 0, 1, 0), action);
		
		// This is further so goes to another bucket
		rt.update(contact(0, 1, 0, 0, 0), action);
		
		assertEquals(rt.getClosestTo(key(0, 0, 1, 0, 0), 2).toString(), // 
				"[Contact [nodeId=0000000000000000000000000000000100000000, address=null]," +
				" Contact [nodeId=0000000000000000000000000000000000000001, address=null]]");
		
		assertEquals(rt.getClosestTo(key(0, 0, 1, 0, 0), 4).toString(), // 
				"[Contact [nodeId=0000000000000000000000000000000100000000, address=null]," +
				" Contact [nodeId=0000000000000000000000000000000000000001, address=null]," +
				" Contact [nodeId=0000000000000001000000000000000000000000, address=null]]");
	}
	
	/**
	 * Tests that the remove operation really removes the contact.
	 */
	public void testRemove() throws Exception {
		RoutingTableFixedImpl rt = new RoutingTableFixedImpl(key(0, 0, 1, 0, 0), 2);
		ReplaceAction action = new AlwaysReplaceAction();
		
		// These go to the same bucket
		rt.update(contact(0, 0, 0, 0, 1), action);
		rt.update(contact(0, 0, 0, 1, 1), action);
		
		// This must not have effect
		rt.remove(contact(0, 0, 1, 0, 0));
		
		// Removing the second contact
		rt.remove(contact(0, 0, 0, 1, 1));
		
		assertEquals(rt.getClosestTo(key(0, 0, 1, 0, 0), 4).toString(), // 
				"[Contact [nodeId=0000000000000000000000000000000000000001, address=null]]");
	}

	/**
	 * Tests that the bucket order is calculated correctly from various distance vectors.
	 */
	public void testGetBucketOrder() throws Exception {
		assertEquals( // 00000000...00
				Arrays.toString(RoutingTableFixedImpl.getBucketOrder(new int[] { 0 })),
				"[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]");
		assertEquals( // 00000000...01
				Arrays.toString(RoutingTableFixedImpl.getBucketOrder(new int[] { 1 })),
				"[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]");
		assertEquals( // 10000000...00
				Arrays.toString(RoutingTableFixedImpl.getBucketOrder(new int[] { 0x80000000 })),
				"[31, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30]");
		assertEquals( // 01010101...01
				Arrays.toString(RoutingTableFixedImpl.getBucketOrder(new int[] { 0x55555555 })),
				"[30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31]");
	}
	
	BinaryKey key(int... value) {
		return new BinaryKey(value);
	}
	
	Contact contact(int... key) {
		return new Contact(key(key), null);
	}
	
	private static class AlwaysReplaceAction implements ReplaceAction {
		@Override
		public Contact replace(Contact old, Contact contact) {
			return contact;
		}
	}
}
