package org.ogreg.sdis.kademlia;

import org.jboss.netty.buffer.ChannelBuffer;
import org.ogreg.sdis.kademlia.Protocol.Message;
import org.ogreg.sdis.kademlia.Protocol.MessageType;
import org.ogreg.sdis.util.ProtobufFrame;
import org.ogreg.sdis.util.ProtobufFrameFactory;

/**
 * A Kademlia message frame.
 * 
 * @author gergo
 */
public class Frame extends ProtobufFrame<Message> {

	public static final ProtobufFrameFactory<Frame, Message> Factory = new ProtobufFrameFactory<Frame, Protocol.Message>() {
		@Override
		public Frame newInstance(Message message, ChannelBuffer data) {
			return new Frame(message, data);
		}

		@Override
		public Frame newInstance(Message message) {
			return new Frame(message);
		}
	};

	public Frame(Message message) {
		super(message);
	}

	public Frame(Message message, ChannelBuffer data) {
		super(message, data);
	}

	public MessageType getType() {
		return message.getType();
	}
}
