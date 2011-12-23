package org.ogreg.sdis;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.ogreg.sdis.messages.Kademlia;
import org.ogreg.sdis.messages.Kademlia.Message;
import org.ogreg.sdis.messages.Kademlia.Message.Builder;
import static org.ogreg.sdis.messages.Kademlia.MessageType.*;
import org.ogreg.sdis.messages.Kademlia.Node;
import org.ogreg.sdis.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * The service endpoint listening for incoming requests from the P2P network.
 * 
 * @author gergo
 */
public class KademliaServer {

	private static final Logger log = LoggerFactory.getLogger(KademliaServer.class);

	/**
	 * The port on which the server listens.
	 */
	private int port;

	/**
	 * The storage service used for loading and saving blocks of data.
	 */
	private StorageService store;

	/**
	 * Starts listening on {@link #port}.
	 */
	public void start() {
		ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
		bootstrap.setPipelineFactory(new KademliaPipelineFactory());
		bootstrap.bind(new InetSocketAddress(port));
	}

	public void setPort(int port) {
		this.port = port;
	}

	private void processPing(Message req, Builder builder) {
		builder.setType(RSP_SUCCESS);
	}

	private void processStore(Message req, Builder builder) {
		BinaryKey key = getKey(req);
		ByteString data = getData(req);
		// TODO check key
		// TODO check data
		store.store(key, data.asReadOnlyByteBuffer());
	}

	private void processFindNode(Message req, Builder builder) {
		BinaryKey key = getKey(req);
		// TODO get closest nodes
		builder.addNodes((Node) null);
	}

	private void processFindValue(Message req, Builder builder) {
		BinaryKey key = getKey(req);
		ByteBuffer dataBuffer = store.load(key);
		if (dataBuffer != null) {
			builder.setData(ByteString.copyFrom(dataBuffer));
		} else {
			// TODO get closest nodes
			builder.addNodes((Node) null);
		}
	}

	private void processSuccess(Message req, Builder builder) {
		// TODO process success result based on rpc id
	}

	private void processIOError(Message req, Builder builder) {
		// TODO process IO error result based on rpc id
	}

	private void unsupportedMessage(MessageEvent event, Message request) {
		log.error("Unsupported message from: {} ({})", event.getRemoteAddress(), request);
	}

	private BinaryKey getKey(Message msg) {
		// TODO assert has key
		return new BinaryKey(msg.getKey().toByteArray());
	}

	private ByteString getData(Message msg) {
		// TODO assert has data
		return msg.getData();
	}

	// A pipeline factory for handling Kademlia protobuf messages
	private class KademliaPipelineFactory implements ChannelPipelineFactory {

		@Override
		public ChannelPipeline getPipeline() throws Exception {
			ChannelPipeline p = Channels.pipeline();
			p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
			p.addLast("protobufDecoder", new ProtobufDecoder(Kademlia.Message.getDefaultInstance()));
			p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
			p.addLast("protobufEncoder", new ProtobufEncoder());
			p.addLast("handler", new KademliaHandler());
			return p;
		}
	}

	// The handler for protobuf Kademlia messages
	private class KademliaHandler extends SimpleChannelUpstreamHandler {

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			Message req = (Message) e.getMessage();

			ByteString nodeId = null;
			ByteString rpcId = KademliaUtil.generateId();

			Builder builder = Message.newBuilder().setNodeId(nodeId).setRpcId(rpcId);

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
	}
}
