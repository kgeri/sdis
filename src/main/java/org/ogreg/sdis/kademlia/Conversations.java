package org.ogreg.sdis.kademlia;

import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_FIND_NODE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_FIND_VALUE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_PING;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_STORE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_SUCCESS;
import static org.ogreg.sdis.kademlia.Util.message;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.ogreg.sdis.kademlia.Protocol.Message;
import org.ogreg.sdis.kademlia.Protocol.MessageType;
import org.ogreg.sdis.kademlia.Protocol.Node;
import org.ogreg.sdis.model.BinaryKey;
import org.ogreg.sdis.util.Conversations.NettyConversation;
import org.ogreg.sdis.util.Conversations.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * Various conversation types for the Kademlia protocol.
 * 
 * @author gergo
 */
class Conversations {

	private static final Logger log = LoggerFactory.getLogger(Conversations.class);

	// A simple conversation for sending a single message and waiting for the response
	static final class SingleFrameConversation extends NettyConversation<Frame, Frame> {

		protected static final State REQUEST_SENT = new State();

		private final Frame request;

		private final InetSocketAddress targetAddress;

		public SingleFrameConversation(ClientBootstrap client, Frame request, InetSocketAddress targetAddress) {
			super(client, request.getMessage().getRpcId());
			this.request = request;
			this.targetAddress = targetAddress;
		}

		@Override
		protected State onStarted() {
			send(request, targetAddress);
			return REQUEST_SENT;
		}

		@Override
		protected State onResponse(State state, Frame message, InetSocketAddress address) {
			succeed(message);
			return FINISHED;
		}

		@Override
		protected void onConnectionFailed(InetSocketAddress address, Throwable cause) {
			log.error("Failed to send message to {}. Connection failed.", address);
		}

		@Override
		protected void onSendFailed(InetSocketAddress address, Frame message, Throwable cause) {
			log.error("Failed to send {} to {}. Write failed.", message.getType(), address);
		}

		@Override
		protected void onSendSucceeded(InetSocketAddress address, Frame message) {
			log.debug("Successfully sent {} to {}", message.getType(), address);
		}
	}

	// Conversation for contacting a node
	static final class ContactConversation extends NettyConversation<Frame, Boolean> {

		protected static final State PING_SENT = new State();

		private final ByteString nodeId;

		private final InetSocketAddress sourceAddress;

		private final InetSocketAddress targetAddress;

		public ContactConversation(ClientBootstrap client, ByteString nodeId, InetSocketAddress sourceAddress,
				InetSocketAddress targetAddress) {
			super(client, Util.generateByteStringId());
			this.nodeId = nodeId;
			this.sourceAddress = sourceAddress;
			this.targetAddress = targetAddress;
		}

		@Override
		protected State onStarted() {
			send(new Frame(message(REQ_PING, nodeId, sourceAddress, id).build()), targetAddress);
			return PING_SENT;
		}

		@Override
		protected State onResponse(State state, Frame message, InetSocketAddress address) {
			succeed(message.getMessage().getType() == MessageType.RSP_SUCCESS);
			return FINISHED;
		}
	}

	// Conversation for finding something iteratively
	static abstract class IterativeFindConversation<T> extends NettyConversation<Frame, T> {

		protected static final State ITERATING = new State();

		private final RoutingTable routingTable;

		protected final BinaryKey key;

		protected final ByteString keyBS;

		protected final KademliaProperties props;

		private final Queue<Contact> contacts;

		private final Set<Contact> seen;

		private final Frame request;

		protected final ByteString sourceNodeId;

		protected final InetSocketAddress sourceAddress;

		public IterativeFindConversation(ClientBootstrap client, RoutingTable routingTable, KademliaProperties props,
				ByteString sourceNodeId, InetSocketAddress sourceAddress, BinaryKey key) {
			super(client, Util.generateByteStringId());
			this.routingTable = routingTable;
			this.sourceNodeId = sourceNodeId;
			this.sourceAddress = sourceAddress;
			this.key = key;
			this.keyBS = ByteString.copyFrom(key.toByteArray());
			this.props = props;
			this.contacts = new PriorityQueue<Contact>(props.maxContacts, new Util.ContactDistanceComparator(key));
			this.seen = new HashSet<Contact>(props.maxContacts * 2);
			this.request = createRequest();
		}

		/**
		 * Implementors must create the message used for iteration here.
		 * 
		 * @return
		 */
		protected abstract Frame createRequest();

		/**
		 * Implementors must specify in this method what to do with one successful response received during the
		 * iteration.
		 * 
		 * @param message
		 *            The message received (response is always success)
		 * @param contact
		 *            The contact from which the message was received
		 * @return {@link #ITERATING} if the iteration should continue, or any other state to continue work in that
		 *         state
		 */
		protected abstract State onStepSuccess(Frame message, Contact contact);

		/**
		 * Implementors must specify what happens after the iteration is completed.
		 * 
		 * @param state
		 * @param message
		 * @param address
		 * @return The state to continue with
		 */
		protected abstract State onIterationComplete(State state, Frame message, InetSocketAddress address);

		@Override
		protected State onStarted() {
			// Initially the queue is filled using the routing table
			// Contacts are sorted by nearest first
			for (Contact contact : routingTable.getClosestTo(key, props.maxParallelism)) {
				send(request, contact.address);
			}

			return ITERATING;
		}

		@Override
		protected State onResponse(State state, Frame message, InetSocketAddress address) {
			if (state == ITERATING) {
				if (message.getType() == RSP_SUCCESS) {
					Message msg = message.getMessage();
					Contact contact = Util.toContact(msg.getNodeId(), msg.getAddress(), msg.getPort());

					seen.add(contact);

					State newState = onStepSuccess(message, contact);
					if (newState != ITERATING) {
						return newState;
					}

					for (Node node : message.getMessage().getNodesList()) {
						Contact newContact = Util.toContact(node.getNodeId(), node.getAddress(), node.getPort());
						if (!seen.contains(newContact)) {
							contacts.add(newContact);
						}
					}
				}

				// Continuing iteration
				processNextContact();

				return ITERATING;
			} else {
				return onIterationComplete(state, message, address);
			}
		}

		@Override
		protected void onConnectionFailed(InetSocketAddress address, Throwable cause) {
			processNextContact();
		}

		@Override
		protected void onSendFailed(InetSocketAddress address, Frame message, Throwable cause) {
			processNextContact();
		}

		private synchronized void processNextContact() {
			Contact contact = contacts.poll();
			if (contact != null) {
				send(request, contact.address);
				// TODO how to handle unresponsive nodes?
			}
		}
	}

	// Base Conversation for finding the nearest props.maxContacts nodes iteratively
	static abstract class BaseIterativeFindNodeConversation<T> extends IterativeFindConversation<T> {

		private final List<Contact> results;

		public BaseIterativeFindNodeConversation(ClientBootstrap client, RoutingTable routingTable,
				KademliaProperties props, ByteString sourceNodeId, InetSocketAddress sourceAddress, BinaryKey key) {
			super(client, routingTable, props, sourceNodeId, sourceAddress, key);
			this.results = new ArrayList<Contact>(props.maxContacts);
		}

		@Override
		protected Frame createRequest() {
			Message message = message(REQ_FIND_NODE, sourceNodeId, sourceAddress, id).setKey(keyBS).build();
			return new Frame(message);
		}

		@Override
		protected final State onStepSuccess(Frame response, Contact contact) {
			results.add(contact);
			if (results.size() >= props.maxContacts) {
				return onContactsFound(results);
			}
			return ITERATING;
		}

		/**
		 * Implementors can specify what to do with the found {@link Contact}s here.
		 * 
		 * @param contacts
		 * @return The next state to continue processing in
		 */
		protected abstract State onContactsFound(List<Contact> contacts);
	}

	// Conversation for finding the nearest props.maxContacts nodes iteratively
	static final class IterativeFindNodeConversation extends BaseIterativeFindNodeConversation<List<Contact>> {

		public IterativeFindNodeConversation(ClientBootstrap client, RoutingTable routingTable,
				KademliaProperties props, ByteString sourceNodeId, InetSocketAddress sourceAddress, BinaryKey key) {
			super(client, routingTable, props, sourceNodeId, sourceAddress, key);
		}

		@Override
		protected State onContactsFound(List<Contact> contacts) {
			succeed(contacts);
			return FINISHED;
		}

		@Override
		protected State onIterationComplete(State state, Frame message, InetSocketAddress address) {
			return FINISHED;
		}
	}

	// Conversation for finding the first value for a given key iteratively
	static final class IterativeFindValueConversation extends IterativeFindConversation<ByteBuffer> {

		public IterativeFindValueConversation(ClientBootstrap client, RoutingTable routingTable,
				KademliaProperties props, ByteString sourceNodeId, InetSocketAddress sourceAddress, BinaryKey key) {
			super(client, routingTable, props, sourceNodeId, sourceAddress, key);
		}

		@Override
		protected Frame createRequest() {
			Message message = message(REQ_FIND_VALUE, sourceNodeId, sourceAddress, id).setKey(keyBS).build();
			return new Frame(message);
		}

		@Override
		protected State onStepSuccess(Frame response, Contact contact) {
			if (!response.hasData()) {
				log.debug("Contact responded SUCCESS but no data to a FIND_VALUE: {}", contact);
				return ITERATING;
			}

			ChannelBuffer data = response.getData();

			// TODO Make sure that this does not copy the contents
			ByteBuffer buffer = data.toByteBuffer();
			BinaryKey computedKey = Util.checksum(buffer);

			if (!computedKey.equals(key)) {
				// Either someone was nasty, or there was a transfer error
				log.error("Data chunk cheksum mismatch (" + key + " != " + computedKey + ")");
				return ITERATING;
			}

			// We stop if we found the result
			succeed(buffer);
			return FINISHED;
		}

		@Override
		protected State onIterationComplete(State state, Frame message, InetSocketAddress address) {
			return FINISHED;
		}
	}

	// Conversation for finding the nearest props.maxContacts nodes iteratively, and storing some data chunk on them
	static final class IterativeStoreConversation extends BaseIterativeFindNodeConversation<BinaryKey> {

		protected static final State STORING = new State();

		private final ByteBuffer data;

		public IterativeStoreConversation(ClientBootstrap client, RoutingTable routingTable, KademliaProperties props,
				ByteString sourceNodeId, InetSocketAddress sourceAddress, BinaryKey key, ByteBuffer data) {
			super(client, routingTable, props, sourceNodeId, sourceAddress, key);
			this.data = data;
		}

		@Override
		protected State onContactsFound(List<Contact> contacts) {
			Message message = message(REQ_STORE, sourceNodeId, sourceAddress, id).setKey(keyBS).build();
			Frame request = new Frame(message, ChannelBuffers.wrappedBuffer(data));
			for (Contact contact : contacts) {
				send(request, contact.address);
			}
			return STORING;
		}

		@Override
		protected State onIterationComplete(State state, Frame message, InetSocketAddress address) {
			if (message.getType().equals(RSP_SUCCESS)) {
				log.debug("Store succeeded for: {}", address);
				// TODO Expect some redundancy here, only succeed after a number of STOREs succeeded
				succeed(key);
				return FINISHED;
			} else {
				log.error("Store failed for: {}", address);
				// TODO Detect and handle failures (when all STOREs failed - currently we would wait until timeout)
				return STORING;
			}
		}
	}
}
