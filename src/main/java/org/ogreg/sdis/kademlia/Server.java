package org.ogreg.sdis.kademlia;

import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_FIND_NODE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_FIND_VALUE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_PING;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_STORE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_IO_ERROR;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_SUCCESS;
import static org.ogreg.sdis.kademlia.Util.message;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.ogreg.sdis.P2PService;
import org.ogreg.sdis.StorageService;
import org.ogreg.sdis.kademlia.Protocol.Message;
import org.ogreg.sdis.kademlia.Protocol.Message.Builder;
import org.ogreg.sdis.kademlia.Protocol.MessageType;
import org.ogreg.sdis.kademlia.Protocol.Node;
import org.ogreg.sdis.kademlia.Util.ContactWithDistance;
import org.ogreg.sdis.model.BinaryKey;
import org.ogreg.sdis.util.Conversations;
import org.ogreg.sdis.util.Conversations.NettyConversation;
import org.ogreg.sdis.util.Properties;
import org.ogreg.sdis.util.ProtobufFrameDecoder;
import org.ogreg.sdis.util.ProtobufFrameEncoder;
import org.ogreg.sdis.util.ShutdownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

/**
 * The service endpoint listening for incoming requests from the P2P network.
 * 
 * @author gergo
 */
public class Server extends AbstractService implements P2PService {

	private static final Logger log = LoggerFactory.getLogger(Server.class);

	/**
	 * The first port on which the server tries to listen (inclusive).
	 */
	private int portFrom;

	/**
	 * The last port on which the server listens (inclusive).
	 */
	private int portTo;

	/**
	 * The storage service used for loading and saving blocks of data.
	 */
	private final StorageService store;

	/**
	 * The current Node's node ID.
	 */
	private final ByteString nodeId;

	/**
	 * The Kademlia routing table used by this node.
	 */
	private final RoutingTable routingTable;

	private final KademliaProperties props;

	private ServerBootstrap server;
	private ClientBootstrap client;

	private ChannelGroup channels = null;
	private InetSocketAddress address = null;
	private ExecutorService executor = null;
	private Conversations<Frame> conversations = null;

	private ReplaceAction updateAction;

	public Server(StorageService store, BinaryKey nodeId, Properties properties) {
		this.props = new KademliaProperties(properties);
		this.store = store;
		this.nodeId = ByteString.copyFrom(nodeId.toByteArray());
		this.routingTable = new RoutingTableFixedImpl(nodeId, props.maxContacts);

		// Update action: if the appropriate k-bucket is full, then the recipient pings the k-bucket’s least-recently
		// seen node to decide what to do
		updateAction = new ReplaceAction() {
			@Override
			public Contact replace(Contact leastRecent, Contact newContact) {
				// TODO Async replace
				boolean success = false;
				try {
					Message message = message(REQ_PING, Server.this.nodeId, leastRecent.address,
							Util.generateByteStringId()).build();
					Frame frame = new Frame(message);
					ListenableFuture<Frame> future = sendMessageASync(frame, address);
					Frame response = future.get(props.responseTimeOutMs, TimeUnit.MILLISECONDS);
					success = response.getMessage().getType().equals(RSP_SUCCESS);
				} catch (TimeoutException e) {
					log.debug("Ping timed out for: {}", leastRecent);
				} catch (InterruptedException e) {
					throw new ShutdownException(e);
				} catch (ExecutionException e) {
					log.error("Ping FAILED for: " + leastRecent, e.getCause());
				}

				// If the least-recently seen node responds, the new contact is discarded
				// If the least-recently seen node fails to respond, the new contact is added
				return success ? leastRecent : newContact;
			}
		};
	}

	@Override
	protected void doStart() {
		executor = Executors.newCachedThreadPool();

		ChannelFactory serverChannelFactory = new NioServerSocketChannelFactory(executor, executor, props.serverThreads);
		server = new ServerBootstrap(serverChannelFactory);
		server.setOption("connectTimeoutMillis", props.responseTimeOutMs);
		server.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline p = Channels.pipeline();
				p.addLast("frameDecoder", new ProtobufFrameDecoder(Message.getDefaultInstance(), Frame.Factory));
				p.addLast("frameEncoder", new ProtobufFrameEncoder());
				p.addLast("handler", new ServerHandler());
				return p;
			}
		});

		ChannelFactory clientChannelFactory = new NioClientSocketChannelFactory(executor, executor, props.clientThreads);
		client = new ClientBootstrap(clientChannelFactory);
		client.setOption("connectTimeoutMillis", props.responseTimeOutMs);
		client.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline p = Channels.pipeline();
				p.addLast("frameDecoder", new ProtobufFrameDecoder(Message.getDefaultInstance(), Frame.Factory));
				p.addLast("frameEncoder", new ProtobufFrameEncoder());
				p.addLast("handler", new ClientHandler());
				return p;
			}
		});

		channels = new DefaultChannelGroup("kademlia");
		conversations = new Conversations<Frame>(props.responseTimeOutMs);

		for (int port = portFrom; port <= portTo; port++) {
			try {
				address = new InetSocketAddress(port);
				server.bind(address);
				log.info("Kademlia server started at {}", address);
				notifyStarted();
				break;
			} catch (ChannelException e) {
				if (port < portTo) {
					log.warn("Failed to bind {}, trying next in range ({} - {})",
							new Object[] { port, portFrom, portTo });
				} else {
					log.error("Failed to bind port range ({} - {})", portFrom, portTo);
					channels = null;
					address = null;
					throw e;
				}
			}
		}
	}

	@Override
	protected void doStop() {
		channels.close().awaitUninterruptibly();
		conversations.cancelAll();
		client.releaseExternalResources();
		server.releaseExternalResources();

		log.info("Kademlia server stopped at {}", address);

		channels = null;
		conversations = null;
		address = null;
		executor.shutdownNow();
		executor = null;
		notifyStopped();
	}

	@Override
	public void contact(InetSocketAddress address) {
		contactASync(address);
	}

	// Package private for testing
	ListenableFuture<Frame> contactASync(InetSocketAddress address) {
		ByteString rpcId = Util.generateByteStringId();
		Frame req = new Frame(message(REQ_PING, nodeId, this.address, rpcId).build());
		// TODO Direct errors to GUI
		return sendMessageASync(req, address);
	}

	// Does an iterativeStore in the Kademlia network
	@Override
	public BinaryKey store(ByteBuffer data) throws TimeoutException {
		// Note: if you change this, make sure data is not modified, neither directly, nor by any side-effect
		BinaryKey key = Util.checksum(data);

		ByteString keyBS = ByteString.copyFrom(key.toByteArray());

		List<Contact> contacts = iterativeFindNode(key);

		List<ListenableFuture<Frame>> futures = new ArrayList<ListenableFuture<Frame>>(contacts.size());
		for (final Contact contact : contacts) {
			// TODO Create a new conversation kind for mass messaging, and reuse the frame instance
			ByteString rpcId = Util.generateByteStringId();
			Message message = message(REQ_STORE, nodeId, this.address, rpcId).setKey(keyBS).build();
			Frame frame = new Frame(message, ChannelBuffers.wrappedBuffer(data));
			futures.add(sendMessageASync(frame, contact.address));
		}

		try {
			// Waiting for results and logging errors
			List<Frame> results = Futures.allAsList(futures).get(props.responseTimeOutMs, TimeUnit.MILLISECONDS);
			for (Frame rsp : results) {
				Message msg = rsp.getMessage();
				if (!msg.getType().equals(RSP_SUCCESS)) {
					log.error("Store failed for: {}", msg.getAddress());
				}
			}
		} catch (InterruptedException e) {
			throw new ShutdownException(e);
		} catch (ExecutionException e) {
			log.error("Store FAILED", e.getCause());
		}

		return key;
	}

	// // Does an iterativeStore in the Kademlia network
	// @Override
	// public ListenableFuture<BinaryKey> store(ByteBuffer data) {
	// // Note: if you change this, make sure data is not modified, neither directly, nor by any side-effect
	// final BinaryKey key = Util.checksum(data);
	//
	// ByteString keyBS = ByteString.copyFrom(key.toByteArray());
	// ByteString rpcId = Util.generateByteStringId();
	// Message message = message(REQ_STORE, nodeId, this.address, rpcId).setKey(keyBS).build();
	// final Frame frame = new Frame(message, ChannelBuffers.wrappedBuffer(data));
	//
	// // Finding suitable nodes for the given key
	// ListenableFuture<List<Contact>> contacts = iterativeFindNode(key);
	//
	// // Sending STORE message to the best k
	// ListenableFuture<List<Frame>> stored = Futures.transform(contacts,
	// new AsyncFunction<List<Contact>, List<Frame>>() {
	// @Override
	// public ListenableFuture<List<Frame>> apply(List<Contact> contacts) throws Exception {
	// List<ListenableFuture<Frame>> futures = new ArrayList<ListenableFuture<Frame>>(contacts.size());
	// for (final Contact contact : contacts) {
	// futures.add(sendMessageASync(frame, contact.address));
	// }
	// return Futures.allAsList(futures);
	// }
	// });
	//
	// // Determining results, returning key if any of the STOREs were successful
	// ListenableFuture<BinaryKey> result = Futures.transform(stored, new Function<List<Frame>, BinaryKey>() {
	// @Override
	// public BinaryKey apply(List<Frame> results) {
	// boolean success = false;
	// for (Frame rsp : results) {
	// Message msg = rsp.getMessage();
	// if (!msg.getType().equals(RSP_SUCCESS)) {
	// log.error("Store failed for: {}", msg.getAddress());
	// } else {
	// success = true;
	// }
	// }
	//
	// return success ? key : null;
	// }
	// });
	//
	// return result;
	// }

	@Override
	public ByteBuffer load(BinaryKey key) {
		ChannelBuffer buffer = iterativeFindValue(key);
		// TODO Make sure that this does not copy the contents
		return buffer == null ? null : buffer.toByteBuffer();
	}

	/**
	 * Does an iterativeFindNode for the specified key and returns at most {@link #maxContacts} closest contacts.
	 * 
	 * @param key
	 * @param operation
	 *            the type of the operation to do iteratively - either REQ_FIND_NODE, or REQ_FIND_VALUE
	 */
	private List<Contact> iterativeFindNode(BinaryKey key) {
		final List<Contact> results = new ArrayList<Contact>(props.maxContacts);
		iterativeFind(key, new IterativeFindCallback() {
			@Override
			public Message getRequest(ByteString key) {
				ByteString rpcId = Util.generateByteStringId();
				return message(REQ_FIND_NODE, nodeId, Server.this.address, rpcId).setKey(key).build();
			}

			@Override
			protected boolean onSuccess(Contact contact, Frame response) {
				results.add(contact);
				// We stop if we have k successfully probed contacts
				return results.size() >= props.maxContacts;
			}
		});
		return results;
	}

	/**
	 * Does an iterativeFindNode for the specified key and returns at most {@link #maxContacts} closest contacts.
	 * 
	 * @param key
	 * @return the data chunk for the searched key, or null if not found
	 */
	private ChannelBuffer iterativeFindValue(final BinaryKey key) {
		final ChannelBuffer[] result = new ChannelBuffer[1];
		iterativeFind(key, new IterativeFindCallback() {
			@Override
			public Message getRequest(ByteString key) {
				ByteString rpcId = Util.generateByteStringId();
				return message(REQ_FIND_VALUE, nodeId, Server.this.address, rpcId).setKey(key).build();
			}

			@Override
			protected boolean onSuccess(Contact contact, Frame response) {
				if (!response.hasData()) {
					log.debug("Contact responded SUCCESS but no data to a FIND_VALUE: {}", contact);
					return false;
				}

				ChannelBuffer data = response.getData();

				// TODO Make sure that this does not copy the contents
				ByteBuffer buffer = data.toByteBuffer();
				BinaryKey computedKey = Util.checksum(buffer);

				if (!computedKey.equals(key)) {
					// Either someone was nasty, or there was a transfer error
					log.error("Data chunk cheksum mismatch (" + key + " != " + computedKey + ")");
					return false;
				}

				// We stop if we found the result
				result[0] = data;
				return true;
			}
		});
		return result[0];
	}

	/**
	 * Executes an iterativeFind operation.
	 * <p>
	 * This iteratively queries nodes of the network, and searches for a value or more nodes, depending on the callback
	 * implementation.
	 * 
	 * @param key
	 * @param callback
	 */
	private void iterativeFind(BinaryKey key, IterativeFindCallback callback) {
		Queue<ContactWithDistance> queue = new PriorityQueue<ContactWithDistance>(props.maxContacts,
				Util.ContactDistanceComparator);
		Set<Contact> seen = new HashSet<Contact>(props.maxContacts * 2);

		// Initially the queue is filled using the routing table
		for (Contact contact : routingTable.getClosestTo(key, props.maxParallelism)) {
			queue.add(new ContactWithDistance(contact, Util.distanceOf(key, contact.nodeId)));
		}

		ByteString keyBS = ByteString.copyFrom(key.toByteArray());

		// TODO Time-limited operation (?)
		ContactWithDistance cwd;
		while ((cwd = queue.poll()) != null) {
			Contact contact = cwd.contact;

			if (seen.contains(contact)) {
				continue;
			} else {
				seen.add(contact);
			}

			// This will either be a REQ_FIND_NODE, or a REQ_FIND_VALUE
			// TODO Create a new conversation kind for mass messaging, and reuse the frame instance
			Message message = callback.getRequest(keyBS);
			Frame frame = new Frame(message);
			ListenableFuture<Frame> future = sendMessageASync(frame, contact.address);

			try {
				// TODO Parallel execution
				Frame rsp = future.get(props.responseTimeOutMs, TimeUnit.MILLISECONDS);

				Message msg = rsp.getMessage();
				if (msg.getType() == RSP_SUCCESS) {

					if (callback.onSuccess(contact, rsp)) {
						break;
					}

					// Adding resulting nodes to queue
					for (Node node : msg.getNodesList()) {
						Contact newContact = Util.toContact(node.getNodeId(), node.getAddress(), node.getPort());
						queue.add(new ContactWithDistance(newContact, Util.distanceOf(key, newContact.nodeId)));
					}
				}
			} catch (InterruptedException e) {
				throw new ShutdownException(e);
			} catch (ExecutionException e) {
				log.error("IterativeFind FAILED for: " + contact, e.getCause());
			} catch (TimeoutException e) {
				log.error("IterativeFind TIMED OUT for: " + contact);
			}
		}
	}

	// Callback interface for various iterativeFind implementations
	private abstract static class IterativeFindCallback {

		// The implementation returns the request used for the iterative find here
		public abstract Message getRequest(ByteString key);

		// The implementation returns true if all the results were found
		protected abstract boolean onSuccess(Contact contact, Frame response);
	}

	/**
	 * Sets the specified port range for this server (inclusive). Won't have effect until the server is restarted.
	 * 
	 * @param from
	 * @param to
	 */
	public void setPortRange(int from, int to) {
		this.portFrom = Math.min(from, to);
		this.portTo = Math.max(from, to);
	}

	// The bound server address, or null if the server is not bound, for testing
	InetSocketAddress getAddress() {
		return address;
	}

	// The nodeId of this server, for testing
	ByteString getNodeId() {
		return nodeId;
	}

	// The routing table, for testing
	RoutingTable getRoutingTable() {
		return routingTable;
	}

	private void onMessageReceived(Contact contact, Frame frame) {

		// When a Kademlia node receives any message from another node, it updates the appropriate k-bucket in the
		// routing table for the sender’s node ID
		routingTable.update(contact, updateAction);
	}

	/**
	 * Sends the <code>frame</code> to <code>address</code> asynchronously, and returns a future.
	 * 
	 * @param frame
	 * @param address
	 * @return A future holding the response frame
	 */
	// Package private for testing
	ListenableFuture<Frame> sendMessageASync(final Frame frame, final InetSocketAddress address) {
		SingleFrameConversation conversation = new SingleFrameConversation(client);
		conversations.add(frame.getMessage().getRpcId(), conversation).proceed(frame, address);
		return JdkFutureAdapters.listenInPoolThread(conversation, executor); // TODO get rid of this
	}

	/**
	 * Processes a {@link MessageType#REQ_PING} message.
	 * <p>
	 * Required:
	 * <ul>
	 * </ul>
	 * Optional:
	 * <ul>
	 * </ul>
	 * Results:<br>
	 * A {@link MessageType#RSP_SUCCESS}.
	 * 
	 * @param requestor
	 * @param request
	 * @return
	 */
	private Frame processPing(Contact requestor, Frame request) {
		return new Frame(message(RSP_SUCCESS, this.nodeId, this.address, request.getMessage().getRpcId()).build());
	}

	/**
	 * Processes a {@link MessageType#REQ_STORE} message.
	 * <p>
	 * Required:
	 * <ul>
	 * <li>the binary data chunk to store
	 * <li>the key of the data chunk
	 * </ul>
	 * Optional:
	 * <ul>
	 * </ul>
	 * Results:<br>
	 * A {@link MessageType#RSP_SUCCESS} a {@link MessageType#RSP_IO_ERROR}.
	 * 
	 * @param requestor
	 * @param request
	 * @return
	 */
	private Frame processStore(Contact requestor, Frame request) {
		Message req = request.getMessage();
		BinaryKey key = Util.ensureHasKey(req);
		// TODO 2-phase STORE (first signal if we already have the data, for performance)
		ChannelBuffer data = Util.ensureHasData(request);

		// TODO Make sure that this does not copy the contents
		ByteBuffer buffer = data.toByteBuffer();
		BinaryKey computedKey = Util.checksum(buffer);

		Message message;
		if (computedKey.equals(key)) {
			store.store(key, buffer);
			message = message(RSP_SUCCESS, nodeId, this.address, req.getRpcId()).build();
		} else {
			log.error("Data chunk cheksum mismatch (" + key + " != " + computedKey + ")");
			message = message(RSP_IO_ERROR, nodeId, this.address, req.getRpcId()).build();
		}
		return new Frame(message);
	}

	/**
	 * Processes a {@link MessageType#REQ_FIND_NODE} message.
	 * <p>
	 * Required:
	 * <ul>
	 * <li>the key of the searched data chunk
	 * </ul>
	 * Optional:
	 * <ul>
	 * </ul>
	 * Results:<br>
	 * A {@link MessageType#RSP_SUCCESS} along with the a list of {@link Node}s which are closer to the searched key.
	 * 
	 * @param requestor
	 * @param request
	 * @return
	 */
	private Frame processFindNode(Contact requestor, Frame request) {
		Message req = request.getMessage();
		BinaryKey key = Util.ensureHasKey(req);
		Builder builder = message(RSP_SUCCESS, this.nodeId, this.address, req.getRpcId());

		Collection<Contact> closestContacts = routingTable.getClosestTo(key, props.maxContacts);
		for (Contact c : closestContacts) {

			// The recipient of a FIND_NODE should never return a triple containing the nodeID of the requestor
			if (requestor.equals(c)) {
				continue;
			}

			Node node = Util.toNode(c);
			builder.addNodes(node);
		}
		return new Frame(builder.build());
	}

	/**
	 * Processes a {@link MessageType#REQ_FIND_VALUE} message.
	 * <p>
	 * Required:
	 * <ul>
	 * <li>the key of the requested data chunk
	 * </ul>
	 * Optional:
	 * <ul>
	 * </ul>
	 * Results:<br>
	 * A {@link MessageType#RSP_SUCCESS} along with the requested data chunk if it is stored locally, or calls
	 * {@link #processFindNode(Message, Builder)} to return the k closest nodes.
	 * 
	 * @param requestor
	 * @param request
	 * @return
	 */
	private Frame processFindValue(Contact requestor, Frame request) {
		Message req = request.getMessage();
		BinaryKey key = Util.ensureHasKey(req);
		ByteBuffer dataBuffer = store.load(key);
		if (dataBuffer != null) {
			Message message = message(RSP_SUCCESS, nodeId, this.address, req.getRpcId()).build();
			return new Frame(message, ChannelBuffers.wrappedBuffer(dataBuffer));
		} else {
			return processFindNode(requestor, request);
		}
	}

	/**
	 * Processes a {@link MessageType#RSP_SUCCESS} message.
	 * <p>
	 * Required:
	 * <ul>
	 * </ul>
	 * Optional:
	 * <ul>
	 * <li>a list of {@link Node}s which are closer to the searched key, if the request was a
	 * {@link MessageType#REQ_FIND_NODE} or a {@link MessageType#REQ_FIND_VALUE}
	 * </ul>
	 * Results:<br>
	 * Internal processing, nothing is returned.
	 * 
	 * @param req
	 * @return
	 */
	private void processSuccess(Frame req) {
		// TODO process success result based on rpc id
	}

	/**
	 * Processes a {@link MessageType#RSP_IO_ERROR} message.
	 * <p>
	 * Required:
	 * <ul>
	 * </ul>
	 * Optional:
	 * <ul>
	 * </ul>
	 * Results:<br>
	 * Internal processing, nothing is returned.
	 * 
	 * @param req
	 */
	private void processIOError(Frame req) {
		// TODO process IO error result based on rpc id
	}

	private void unsupportedMessage(Contact contact, Frame request) {
		log.error("Unsupported message from: {} ({})", contact, request);
	}

	// A simple conversation for sending a single message and waiting for the response
	private static class SingleFrameConversation extends NettyConversation<Frame, Frame> {

		private enum State {
			INIT, SENDING_REQ, RSP_RECEIVED;
		}

		public SingleFrameConversation(ClientBootstrap client) {
			super(client, State.INIT);
		}

		@Override
		protected Enum<?> proceed(Enum<?> state, Frame message, InetSocketAddress address) {
			switch ((State) state) {
			case INIT:
				send(message, address);
				return State.SENDING_REQ;
			case SENDING_REQ:
				succeed(message);
				return State.RSP_RECEIVED;
			default:
				throw new IllegalStateException();
			}
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

	// The client handler for protobuf Kademlia messages
	private class ClientHandler extends SimpleChannelUpstreamHandler {

		@Override
		public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			channels.add(e.getChannel());
			super.channelOpen(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			Frame frame = (Frame) e.getMessage();
			Message rsp = frame.getMessage();

			Contact src = Util.toContact(rsp.getNodeId(), rsp.getAddress(), rsp.getPort());
			onMessageReceived(src, frame);

			conversations.get(rsp.getRpcId()).proceed(frame, src.address);
			e.getChannel().close();
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
			log.error("Unexpected communication error", e.getCause());
			e.getChannel().close();
		}
	}

	// The server handler for protobuf Kademlia messages
	private class ServerHandler extends SimpleChannelUpstreamHandler {

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			Frame frame = (Frame) e.getMessage();

			Message req = frame.getMessage();
			Contact src = Util.toContact(req.getNodeId(), req.getAddress(), req.getPort());

			onMessageReceived(src, frame);

			Frame rsp;

			// Processing messages
			switch (req.getType()) {
			case REQ_PING:
				rsp = processPing(src, frame);
				break;
			case REQ_STORE:
				rsp = processStore(src, frame);
				break;
			case REQ_FIND_NODE:
				rsp = processFindNode(src, frame);
				break;
			case REQ_FIND_VALUE:
				rsp = processFindValue(src, frame);
				break;
			case RSP_SUCCESS:
				processSuccess(frame);
				return;
			case RSP_IO_ERROR:
				processIOError(frame);
				return;
			default:
				unsupportedMessage(src, frame);
				return;
			}

			ctx.getChannel().write(rsp);
		}

		@Override
		public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			channels.add(e.getChannel());
			super.channelOpen(ctx, e);
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
			log.error("Unexpected communication error: {}. Closing connection to: {}", e.getCause()
					.getLocalizedMessage(), e.getChannel().getRemoteAddress());
			log.debug("Failure trace", e.getCause());
			e.getChannel().close();
		}
	}
}
