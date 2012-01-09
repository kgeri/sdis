package org.ogreg.sdis.kademlia;

import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_IO_ERROR;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_SUCCESS;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.ogreg.sdis.BinaryKey;
import org.ogreg.sdis.StorageService;
import org.ogreg.sdis.kademlia.Protocol.Message;
import org.ogreg.sdis.kademlia.Protocol.Message.Builder;
import org.ogreg.sdis.kademlia.Protocol.MessageType;
import org.ogreg.sdis.kademlia.Protocol.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * The service endpoint listening for incoming requests from the P2P network.
 * 
 * @author gergo
 */
public class Server {

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
	 * The port on which the server listens.
	 */
	private int port;

	/**
	 * The storage service used for loading and saving blocks of data.
	 */
	private StorageService store;

	/**
	 * The Kademlia {@link Contact}s known to this node.
	 */
	private Contacts contacts;

	/**
	 * The current Node's node ID.
	 */
	private final ByteString nodeId;

	private ServerBootstrap server;

	private ClientBootstrap client;

	public Server(StorageService store, BinaryKey nodeId) {
		this.store = store;
		this.nodeId = ByteString.copyFrom(nodeId.toByteArray());
		this.contacts = new Contacts(nodeId);

		NioServerSocketChannelFactory serverChannelFactory = new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		server = new ServerBootstrap(serverChannelFactory);
		server.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline p = Channels.pipeline();
				p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
				p.addLast("protobufDecoder", new ProtobufDecoder(Message.getDefaultInstance()));
				p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
				p.addLast("protobufEncoder", new ProtobufEncoder());
				p.addLast("handler", new ServerHandler());
				return p;
			}
		});

		NioClientSocketChannelFactory clientChannelFactory = new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		client = new ClientBootstrap(clientChannelFactory);
		client.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline p = Channels.pipeline();
				p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
				p.addLast("protobufDecoder", new ProtobufDecoder(Message.getDefaultInstance()));
				p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
				p.addLast("protobufEncoder", new ProtobufEncoder());
				p.addLast("handler", new ClientHandler());
				return p;
			}
		});
	}

	/**
	 * Starts listening on {@link #port}.
	 */
	public void start() {
		server.bind(new InetSocketAddress(port));
	}

	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Sends a {@link MessageType#REQ_PING} to <code>address</code>.
	 * 
	 * @param address
	 * 
	 * @return true if the remote node responded successfully
	 */
	public boolean sendPing(InetSocketAddress address) {
		Message message = newMessageBuilder().setType(MessageType.REQ_PING).build();
		try {
			Message response = sendMessage(message, address);
			return response.getType().equals(RSP_SUCCESS);
		} catch (TimeoutException e) {
			log.debug("{}", e.getLocalizedMessage());
		} catch (InterruptedException e) {
			log.debug("{} was interrupted", MessageType.REQ_PING);
		}
		return false;
	}

	// Creates an initialized message builder with no type set
	private Builder newMessageBuilder() {
		ByteString rpcId = Util.generateByteStringId();
		return Message.newBuilder().setNodeId(nodeId).setRpcId(rpcId);
	}

	private void onMessageReceived(Message req, InetSocketAddress fromAddress) {
		// When a Kademlia node receives any message from another node, it updates the appropriate k-bucket for the
		// sender’s node ID
		Contact contact = Util.toContact(req.getNodeId(), fromAddress);

		Contact leastRecent = contacts.update(contact);

		// If the appropriate k-bucket is full, then the recipient pings the k-bucket’s least-recently seen node to
		// decide what to do
		if (leastRecent != null) {
			if (sendPing(leastRecent.address)) {
				// If the least-recently seen node responds, it is moved to the tail of the list, and the new
				// sender’s contact is discarded
				contacts.update(leastRecent);
			} else {
				// If the least-recently seen node fails to respond, it is evicted from the k-bucket and the new
				// sender inserted at the tail
				contacts.add(contact);
			}
		}
	}

	/**
	 * Sends the <code>message</code> to <code>address</code> synchronously, and returns the response.
	 * 
	 * @param message
	 * @param address
	 * 
	 * @return The response received
	 * @throws TimeoutException
	 *             if the request has timed out
	 * @throws InterruptedException
	 *             if the request was interrupted
	 */
	private Message sendMessage(Message message, InetSocketAddress address) throws TimeoutException,
			InterruptedException {

		ChannelFuture future = client.connect(address);

		if (!future.await(RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
			throw new TimeoutException(message.getType() + " to " + address + " has timed out in "
					+ RESPONSE_TIMEOUT_MS + " milliseconds");
		}

		Channel channel = future.getChannel();

		try {
			ClientHandler handler = channel.getPipeline().get(ClientHandler.class);
			return handler.sendMessage(message);
		} finally {
			channel.close().await(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		}
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
	 * @param req
	 * @param builder
	 */
	private void processPing(Message req, Builder builder) {
		builder.setType(RSP_SUCCESS);
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
	 * @param req
	 * @param builder
	 */
	private void processStore(Message req, Builder builder) {
		BinaryKey key = Util.ensureHasKey(req);
		ByteString data = Util.ensureHasData(req);

		BinaryKey computedKey = Util.checksum(data);

		if (computedKey.equals(key)) {
			store.store(key, data.asReadOnlyByteBuffer());
			builder.setType(RSP_SUCCESS);
		} else {
			log.error("Data chunk cheksum mismatch (" + key + " != " + computedKey + ")");
			builder.setType(RSP_IO_ERROR);
		}
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
	 * @param req
	 * @param builder
	 */
	private void processFindNode(Message req, Builder builder) {
		BinaryKey key = Util.ensureHasKey(req);
		List<Contact> closestContacts = contacts.getClosestTo(key);
		for (Contact contact : closestContacts) {
			Node node = Util.toNode(contact);
			builder.addNodes(node);
		}
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
	 * @param req
	 * @param builder
	 */
	private void processFindValue(Message req, Builder builder) {
		BinaryKey key = Util.ensureHasKey(req);
		ByteBuffer dataBuffer = store.load(key);
		if (dataBuffer != null) {
			builder.setData(ByteString.copyFrom(dataBuffer));
		} else {
			processFindNode(req, builder);
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
	 * @param builder
	 */
	private void processSuccess(Message req, Builder builder) {
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
	 * @param builder
	 */
	private void processIOError(Message req, Builder builder) {
		// TODO process IO error result based on rpc id
	}

	private void unsupportedMessage(MessageEvent event, Message request) {
		log.error("Unsupported message from: {} ({})", event.getRemoteAddress(), request);
	}

	// The client handler for protobuf Kademlia messages
	private class ClientHandler extends SimpleChannelUpstreamHandler {

		private final BlockingQueue<Message> responses = new LinkedBlockingQueue<Message>();

		private volatile Channel channel;

		/**
		 * Sends a Kademlia message synchronously and returns the response received.
		 * 
		 * @return The response received from the server, or null if there was a timeout
		 * @throws InterruptedException
		 *             if the waiting for the response was interrupted
		 */
		public Message sendMessage(Message message) throws InterruptedException {
			channel.write(message);
			return responses.poll(Server.RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		}

		@Override
		public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			channel = e.getChannel();
			super.channelOpen(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			responses.offer((Message) e.getMessage());
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
			log.error("Unexpected communication error", e);
			e.getChannel().close();
		}
	}

	// The server handler for protobuf Kademlia messages
	private class ServerHandler extends SimpleChannelUpstreamHandler {

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			Message req = (Message) e.getMessage();

			onMessageReceived(req, (InetSocketAddress) e.getRemoteAddress());

			Builder builder = newMessageBuilder();

			// Processing messages
			switch (req.getType()) {
			case REQ_PING:
				processPing(req, builder);
				break;
			case REQ_STORE:
				processStore(req, builder);
				break;
			case REQ_FIND_NODE:
				processFindNode(req, builder);
				break;
			case REQ_FIND_VALUE:
				processFindValue(req, builder);
				break;
			case RSP_SUCCESS:
				processSuccess(req, builder);
				break;
			case RSP_IO_ERROR:
				processIOError(req, builder);
				break;
			default:
				unsupportedMessage(e, req);
				return;
			}

			ctx.getChannel().write(builder.build());
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
			log.error("Unexpected communication error", e);
			e.getChannel().close();
		}
	}
}
