package org.ogreg.sdis.util;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.CompositeChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

/**
 * An encoder that prepends two Google Protocol Buffers <a
 * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html#varints">Base 128 Varints</a> integer length
 * fields, then encodes the message with protocol buffers, and optionally adds raw data. For example:
 * 
 * <pre>
 * BEFORE DECODE                    AFTER DECODE (602 bytes)
 *                             +--------+--------+---------------+--------------+
 *  new Frame() -------------->| Length | Length | Protobuf Data | Message Data |
 *                             | 0xAC02 | 0xAC02 |  (300 bytes)  |  (300 bytes) |
 *                             +--------+--------+---------------+--------------+
 * </pre>
 * 
 * *
 * 
 * @see com.google.protobuf.CodedOutputStream
 */
public class ProtobufFrameEncoder extends OneToOneEncoder {

	@Override
	protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
		if (!(msg instanceof ProtobufFrame)) {
			return msg;
		}

		@SuppressWarnings("unchecked")
		ProtobufFrame<MessageLite> frame = (ProtobufFrame<MessageLite>) msg;

		List<ChannelBuffer> buffers = new ArrayList<ChannelBuffer>(4);

		// Writing message
		byte[] messageBody = frame.getMessage().toByteArray();

		// Calculating lengths
		int mesgLength = messageBody.length;
		int dataLength = frame.hasData() ? frame.getData().readableBytes() : 0;
		int headLength = CodedOutputStream.computeRawVarint32Size(mesgLength)
				+ CodedOutputStream.computeRawVarint32Size(dataLength);

		// Writing lengths to header
		byte[] headerBody = new byte[headLength];
		CodedOutputStream os = CodedOutputStream.newInstance(headerBody);
		os.writeRawVarint32(mesgLength);
		os.writeRawVarint32(dataLength);
		os.flush();

		// Collecting buffers: lengths + message [+ data]
		buffers.add(new BigEndianHeapChannelBuffer(headerBody));
		buffers.add(new BigEndianHeapChannelBuffer(messageBody));
		if (frame.hasData()) {
			buffers.add(frame.getData());
		}

		return new CompositeChannelBuffer(ByteOrder.BIG_ENDIAN, buffers);
	}
}