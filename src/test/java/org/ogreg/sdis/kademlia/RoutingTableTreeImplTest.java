package org.ogreg.sdis.kademlia;

import static org.testng.Assert.assertEquals;

import org.ogreg.sdis.BinaryKey;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Ensures that the {@link RoutingTableTreeImpl} stores contacts in the proper
 * data structure.
 * 
 * @author gergo
 */
@Test(groups = "functional", enabled = false)
public class RoutingTableTreeImplTest {

	private RoutingTableTreeImpl table;

	private BinaryKey nodeId;

	private ReplaceAction nop = new ReplaceAction() {
		@Override
		public Contact replace(Contact old, Contact contact) {
			return contact;
		}
	};

	@BeforeTest
	public void setUp() {
		// 0b000...00100
		nodeId = new BinaryKey(new int[] { 0, 0, 0, 0, 4 });
		table = new RoutingTableTreeImpl(nodeId, 2);
	}

	/**
	 * Tests that various update calls modify the internal representation as
	 * expected.
	 */
	public void testUpdate() throws Exception {
		equals(table.toString(), "");

		table.update(contact(1), nop);
		equals(table.toString(), "1");

		table.update(contact(2), nop); // New contact becomes first
		equals(table.toString(), "2,1");

		table.update(contact(3), nop); // Node split because full
		equals(table.toString(), "(1)(3,2)");

		table.update(contact(4), nop); // Nothing should happen
		equals(table.toString(), "(1)(3,2)");
	}

	void equals(String actualKeyList, String expectedKeyList) {
		actualKeyList = actualKeyList.replaceAll("^0+", "");
		actualKeyList = actualKeyList.replaceAll(",0+", ",");
		actualKeyList = actualKeyList.replaceAll("\\(0+", "(");
		assertEquals(actualKeyList, expectedKeyList);
	}

	Contact contact(int i) {
		return new Contact(new BinaryKey(new int[] { 0, 0, 0, 0, i }), null);
	}
}
