package org.ogreg.sdis.kademlia;

import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_FIND_NODE;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
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
import org.ogreg.sdis.util.ProtobufFrameDecoder;
import org.ogreg.sdis.util.ProtobufFrameEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * The service endpoint listening for incoming requests from the P2P network.
 * 
 * @author gergo
 */
public class Server implements P2PService {

	private static final Logger log = LoggerFactory.getLogger(Server.class);

	/**
	 * The number of milliseconds to wait before a {@link MessageType#REQ_PING} times out.
	 */
	private static final int RESPONSE_TIMEOUT_MS = 10000;

	/**
	 * The number of milliseconds to wait before a connection close attempt times out.
	 */
	private static final int CLOSE_TIMEOUT_MS = 3000;

	/**
	 * The maximum number of contacts stored in a k-bucket (k).
	 */
	private static final int MAX_CONTACTS = 20;

	/**
	 * The degree of parallelism in network calls (alpha).
	 */
	private static final int MAX_PARALLELISM = 3;

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
	private StorageService store;

	/**
	 * The Kademlia routing table used by this node.
	 */
	private RoutingTable routingTable;

	/**
	 * The current Node's node ID.
	 */
	private final ByteString nodeId;

	private ExecutorService serverExecutor;

	private ServerBootstrap server;
	private ClientBootstrap client;

	private ChannelGroup channels = null;
	private InetSocketAddress address = null;

	private ReplaceAction updateAction;

	public Server(StorageService store, BinaryKey nodeId) {
		this.store = store;
		this.nodeId = ByteString.copyFrom(nodeId.toByteArray());
		this.routingTable = new RoutingTableFixedImpl(nodeId, MAX_CONTACTS);
		this.serverExecutor = Executors.newCachedThreadPool();

		// Update action: if the appropriate k-bucket is full, then the recipient pings the k-bucket’s least-recently
		// seen node to decide what to do
		updateAction = new ReplaceAction() {
			@Override
			public Contact replace(Contact leastRecent, Contact newContact) {
				if (sendPingSync(leastRecent.address)) {
					// If the least-recently seen node responds, the new contact is discarded
					return leastRecent;
				} else {
					// If the least-recently seen node fails to respond, the new contact is added
					return newContact;
				}
			}
		};
	}

	/**
	 * Starts listening on {@link #port}.
	 */
	public synchronized void start() {

		if (channels != null) {
			throw new IllegalStateException("Kademlia server already started. Channels: " + channels);
		}

		NioServerSocketChannelFactory serverChannelFactory = new NioServerSocketChannelFactory(serverExecutor,
				serverExecutor);
		server = new ServerBootstrap(serverChannelFactory);
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

		NioClientSocketChannelFactory clientChannelFactory = new NioClientSocketChannelFactory(serverExecutor,
				serverExecutor);
		client = new ClientBootstrap(clientChannelFactory);
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

		for (int port = portFrom; port <= portTo; port++) {
			try {
				address = new InetSocketAddress(port);
				server.bind(address);
				log.info("Kademlia server started at {}", address);
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

	/**
	 * Stops the Kademlia server.
	 */
	public synchronized void stop() {
		if (channels != null) {
			channels.close().awaitUninterruptibly();
			client.releaseExternalResources();
			server.releaseExternalResources();
			log.info("Kademlia server stopped at {}", address);
			channels = null;
			address = null;
		}
	}

	@Override
	public void contact(InetSocketAddress address) {
		// Pinging peer
		try {
			// TODO Async pinging
			Frame req = new Frame(message(REQ_PING, nodeId, this.address).build());
			Frame rsp = sendMessageSync(req, address);
			Message msg = rsp.getMessage();
			if (msg.getType().equals(RSP_SUCCESS)) {
				// TODO This should be done only in the server handler, as we should use async conversation states
				Contact contact = Util.toContact(msg.getNodeId(), msg.getAddress(), msg.getPort());
				onMessageReceived(contact, rsp);
			}
			// TODO Direct errors to GUI
		} catch (TimeoutException e) {
			log.debug("{}", e.getLocalizedMessage());
		} catch (InterruptedException e) {
			log.debug("{} was interrupted", REQ_PING);
		}
	}

	// Does an iterativeStore in the Kademlia network
	@Override
	public void store(ByteBuffer data) {
		// Note: if you change this, make sure data is not modified, neither directly, nor by any side-effect
		BinaryKey key = Util.checksum(data);

		ByteString keyBS = ByteString.copyFrom(key.toByteArray());
		Message message = message(REQ_STORE, nodeId, this.address).setKey(keyBS).build();
		Frame frame = new Frame(message, ChannelBuffers.wrappedBuffer(data));

		List<Contact> contacts = iterativeFindNode(key);
		for (Contact contact : contacts) {
			try {
				Frame rsp = sendMessageSync(frame, contact.address);
				if (!rsp.getMessage().getType().equals(RSP_SUCCESS)) {
					log.error("Store failed for: {}", contact);
				}
			} catch (TimeoutException e) {
				log.debug("Store timed out for: {}", contact);
				continue;
			} catch (InterruptedException e) {
				log.warn("Store was interrupted for: {}", contact);
				break;
			}
		}
	}

	/**
	 * Does an iterativeFindNode for the specified key and returns at most {@link #MAX_CONTACTS} closest contacts.
	 * 
	 * @param key
	 */
	private List<Contact> iterativeFindNode(BinaryKey key) {
		List<Contact> results = new ArrayList<Contact>(MAX_CONTACTS);

		Queue<ContactWithDistance> queue = new PriorityQueue<ContactWithDistance>(MAX_CONTACTS,
				Util.ContactDistanceComparator);
		Set<Contact> seen = new HashSet<Contact>(MAX_CONTACTS * 2);

		// Initially the queue is filled using the routing table
		for (Contact contact : routingTable.getClosestTo(key, MAX_PARALLELISM)) {
			queue.add(new ContactWithDistance(contact, Util.distanceOf(key, contact.nodeId)));
		}

		ByteString keyBS = ByteString.copyFrom(key.toByteArray());
		Message message = message(REQ_FIND_NODE, nodeId, this.address).setKey(keyBS).build();
		Frame frame = new Frame(message);

		// TODO Time-limited operation (?)
		ContactWithDistance cwd;
		while ((cwd = queue.poll()) != null) {
			Contact contact = cwd.contact;

			if (seen.contains(contact)) {
				continue;
			} else {
				seen.add(contact);
			}

			Frame rsp;

			try {
				// TODO Parallel execution
				rsp = sendMessageSync(frame, contact.address);
			} catch (TimeoutException e) {
				log.debug("FindNode timed out for: {}", contact);
				continue;
			} catch (InterruptedException e) {
				log.warn("FindNode was interrupted for: {}", contact);
				break;
			}

			Message msg = rsp.getMessage();
			if (msg.getType().equals(RSP_SUCCESS) && msg.getNodesCount() > 0) {
				results.add(contact);

				// We stop if we have k successfully probed contacts
				if (results.size() >= MAX_CONTACTS) {
					break;
				}

				// Adding resulting nodes to queue
				for (Node node : msg.getNodesList()) {
					Contact newContact = Util.toContact(node.getNodeId(), node.getAddress(), node.getPort());
					queue.add(new ContactWithDistance(newContact, Util.distanceOf(key, newContact.nodeId)));
				}
			}
		}

		return results;
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

	// The bound server address, or null if the server is not bound.
	InetSocketAddress getAddress() {
		return address;
	}

	// The nodeId of this server.
	ByteString getNodeId() {
		return nodeId;
	}

	/**
	 * Sends a {@link MessageType#REQ_PING} to <code>address</code> synchronously.
	 * 
	 * @param address
	 * 
	 * @return true if the remote node responded successfully
	 */
	private boolean sendPingSync(InetSocketAddress address) {
		Message message = message(REQ_PING, nodeId, this.address).build();
		Frame frame = new Frame(message);
		try {
			Frame response = sendMessageSync(frame, address);
			return response.getMessage().getType().equals(RSP_SUCCESS);
		} catch (TimeoutException e) {
			log.debug("{}", e.getLocalizedMessage());
		} catch (InterruptedException e) {
			log.debug("{} was interrupted", REQ_PING);
		}
		return false;
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
	 * 
	 * @return The future for the response received
	 */
	private Future<Frame> sendMessageASync(Frame frame, InetSocketAddress address) {
		ChannelFuture channelFuture = client.connect(address);
		return new MessageFuture(channelFuture, frame, address);
	}

	/**
	 * Sends the <code>message</code> to <code>address</code> synchronously, and returns the response.
	 * 
	 * @param frame
	 * @param address
	 * 
	 * @return The response received
	 * @throws TimeoutException
	 *             if the request has timed out
	 * @throws InterruptedException
	 *             if the request was interrupted
	 */
	// Package private for unit testing
	Frame sendMessageSync(Frame frame, InetSocketAddress address) throws TimeoutException, InterruptedException {
		log.debug("Sending {} from {} to {}", new Object[] { frame.getType(), this.address, address });
		ChannelFuture channelFuture = client.connect(address);
		return new MessageFuture(channelFuture, frame, address).get(RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
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
		return new Frame(message(RSP_SUCCESS, this.nodeId, this.address).build());
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
		BinaryKey key = Util.ensureHasKey(request.getMessage());
		// TODO 2-phase STORE (first signal if we already have the data, for performance)
		ChannelBuffer data = Util.ensureHasData(request);

		// TODO Make sure that this does not copy the contents
		ByteBuffer buffer = data.toByteBuffer();
		BinaryKey computedKey = Util.checksum(buffer);

		Message message;
		if (computedKey.equals(key)) {
			store.store(key, buffer);
			message = message(RSP_SUCCESS, nodeId, this.address).build();
		} else {
			log.error("Data chunk cheksum mismatch (" + key + " != " + computedKey + ")");
			message = message(RSP_IO_ERROR, nodeId, this.address).build();
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
		BinaryKey key = Util.ensureHasKey(request.getMessage());
		Builder builder = message(RSP_SUCCESS, this.nodeId, this.address);

		Collection<Contact> closestContacts = routingTable.getClosestTo(key, MAX_CONTACTS);
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
		BinaryKey key = Util.ensureHasKey(request.getMessage());
		ByteBuffer dataBuffer = store.load(key);
		if (dataBuffer != null) {
			Message message = message(RSP_SUCCESS, nodeId, this.address).build();
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

	// The client handler for protobuf Kademlia messages
	private class ClientHandler extends SimpleChannelUpstreamHandler {

		private final BlockingQueue<Frame> responses = new LinkedBlockingQueue<Frame>();

		private volatile Channel channel;

		/**
		 * Sends a Kademlia message synchronously and returns the response received.
		 * 
		 * @return The response received from the server, or null if there was a timeout
		 * @throws InterruptedException
		 *             if the waiting for the response was interrupted
		 */
		public Frame sendMessage(Frame frame) throws InterruptedException {
			channel.write(frame);
			return responses.poll(Server.RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		}

		@Override
		public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			channels.add(e.getChannel());
			channel = e.getChannel();
			super.channelOpen(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			responses.offer((Frame) e.getMessage());
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

	// Future wrapper for sending messages with ChannelFutures
	private static class MessageFuture implements Future<Frame> {

		private final ChannelFuture future;
		private final Frame frame;
		private final InetSocketAddress address;

		public MessageFuture(ChannelFuture future, Frame frame, InetSocketAddress address) {
			this.future = future;
			this.frame = frame;
			this.address = address;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return future.cancel();
		}

		@Override
		public boolean isCancelled() {
			return future.isCancelled();
		}

		@Override
		public boolean isDone() {
			return future.isDone();
		}

		@Override
		public Frame get() throws InterruptedException, ExecutionException {
			try {
				return get(RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			} catch (TimeoutException e) {
				throw new InterruptedException("Wait interrupted by default timeout: " + RESPONSE_TIMEOUT_MS + "ms");
			}
		}

		@Override
		public Frame get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {

			if (!future.await(timeout, unit)) {
				throw new TimeoutException(frame.getType() + " to " + address + " has timed out in "
						+ RESPONSE_TIMEOUT_MS + " milliseconds");
			}

			Channel channel = future.getChannel();

			try {
				ClientHandler handler = channel.getPipeline().get(ClientHandler.class);
				Frame rsp = handler.sendMessage(frame);
				if (rsp == null) {
					throw new TimeoutException(frame.getType() + " to " + address + " has timed out");
				}
				return rsp;
			} finally {
				channel.close().await(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			}
		}
	}
}
