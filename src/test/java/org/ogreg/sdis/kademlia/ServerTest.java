package org.ogreg.sdis.kademlia;

import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_FIND_NODE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_PING;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_STORE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_SUCCESS;
import static org.ogreg.sdis.kademlia.Util.message;
import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.ogreg.sdis.CommonUtil;
import org.ogreg.sdis.P2PService;
import org.ogreg.sdis.kademlia.Protocol.Message;
import org.ogreg.sdis.model.BinaryKey;
import org.ogreg.sdis.storage.InMemoryStorageServiceImpl;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.protobuf.ByteString;

/**
 * Tests the behaviour of the Kademlia server.
 * 
 * @author gergo
 */
@Test(groups = "functional")
public class ServerTest {

	private Server alice;
	private Server bob;
	private Server charlie;

	private InMemoryStorageServiceImpl aliceStore;
	private InMemoryStorageServiceImpl bobStore;
	private InMemoryStorageServiceImpl charlieStore;

	@BeforeTest
	public void setUp() {
		aliceStore = new InMemoryStorageServiceImpl();
		alice = new Server(aliceStore, key(0, 0, 0, 0, 1));
		alice.setPortRange(6000, 6100);

		bobStore = new InMemoryStorageServiceImpl();
		bob = new Server(bobStore, key(0, 0, 0, 0, 2));
		bob.setPortRange(6000, 6100);

		charlieStore = new InMemoryStorageServiceImpl();
		charlie = new Server(charlieStore, key(0, 0, 0, 0, 3));
		charlie.setPortRange(6000, 6100);

		alice.start();
		bob.start();
		charlie.start();
	}

	@AfterTest
	public void tearDown() {
		alice.stop();
		bob.stop();
		charlie.stop();
	}

	/**
	 * Ensures that a PING message is processed and a RSP_SUCCESS is returned.
	 */
	public void testPingMessage() throws Exception {
		Message req = message(REQ_PING, alice.getNodeId()).build();
		Message rsp = alice.sendMessageSync(req, bob.getAddress());

		assertEquals(rsp.getNodeId(), bob.getNodeId());
		assertEquals(rsp.getType(), RSP_SUCCESS);
	}

	/**
	 * Ensures that a FIND_NODE message returns the current node and the known neighbours. Also tests that the
	 * {@link P2PService#contact(java.net.InetSocketAddress)} function tries to make contact with the specified address.
	 */
	public void testFindNodeMessage() throws Exception {
		// Only Bob knows Charlie, Alice will ask Bob
		bob.contact(charlie.getAddress());

		Message req = message(REQ_FIND_NODE, alice.getNodeId()).setKey(keyBS(0, 0, 0, 0, 7)).build();
		Message rsp = alice.sendMessageSync(req, bob.getAddress());

		assertEquals(rsp.getNodeId(), bob.getNodeId());
		assertEquals(rsp.getType(), RSP_SUCCESS);
		assertEquals(rsp.getNodesCount(), 2);
		assertBSEquals(rsp.getNodes(0).getNodeId(), charlie.getNodeId());
	}

	/**
	 * Ensures that a STORE message stores the specified data chunk at the appropriate node.
	 */
	public void testStoreMessage() throws Exception {
		ByteBuffer data = data(4096);
		ByteString dataBS = ByteString.copyFrom(data);
		BinaryKey dataKey = Util.checksum(dataBS);
		ByteString dataKeyBS = ByteString.copyFrom(dataKey.toByteArray());

		Message req = message(REQ_STORE, alice.getNodeId()).setKey(dataKeyBS).setData(dataBS).build();
		Message rsp = alice.sendMessageSync(req, bob.getAddress());

		assertEquals(rsp.getNodeId(), bob.getNodeId());
		assertEquals(rsp.getType(), RSP_SUCCESS);
		assertEquals(bobStore.load(dataKey), data.rewind());
	}

	BinaryKey key(int... value) {
		return new BinaryKey(value);
	}

	ByteString keyBS(int... value) {
		return ByteString.copyFrom(CommonUtil.toByteArray(value));
	}

	ByteBuffer data(int size) {
		byte[] bytes = new byte[size];
		for (int i = 0; i < size; i++) {
			bytes[i] = (byte) i;
		}
		return ByteBuffer.wrap(bytes);
	}

	void assertBSEquals(ByteString actual, ByteString expected) {
		assertEquals(CommonUtil.toHexString(actual.toByteArray()), CommonUtil.toHexString(expected.toByteArray()));
	}
}
