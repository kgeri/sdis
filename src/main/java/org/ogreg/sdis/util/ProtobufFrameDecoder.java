package org.ogreg.sdis.util;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.CorruptedFrameException;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.MessageLite;

/**
 * A decoder that splits the received {@link ChannelBuffer}s dynamically by the value of two Google Protocol Buffers <a
 * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html#varints">Base 128 Varints</a> integer length
 * fields. It then returns a protocol buffers message, and an optional raw data buffer wrapped in a {@link Frame} . For
 * example:
 * 
 * <pre>
 * BEFORE DECODE (604 bytes)                               AFTER DECODE
 * +--------+--------+---------------+--------------+
 * | Length | Length | Protobuf Data | Message Data |-----> new Frame()
 * | 0xAC02 | 0xAC02 |  (300 bytes)  |  (300 bytes) |
 * +--------+--------+---------------+--------------+
 * </pre>
 * 
 * @see com.google.protobuf.CodedInputStream
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ProtobufFrameDecoder extends FrameDecoder {

	private final MessageLite prototype;

	private final ProtobufFrameFactory frameFactory;

	public ProtobufFrameDecoder(MessageLite prototype, ProtobufFrameFactory frameFactory) {
		this.frameFactory = frameFactory;
		this.prototype = prototype.getDefaultInstanceForType();
	}

	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
		buffer.markReaderIndex();

		if (buffer.readableBytes() < 9) {
			// Not enough data to decode header
			// This assumes that the 2 varint fields always fit in 9 bytes (OK)
			// This also assumes that the message is always at least 7 bytes (OK for us now)
			return null;
		}

		// Decoding header
		byte[] head = new byte[9];
		buffer.readBytes(head);
		CodedInputStream cis = CodedInputStream.newInstance(head);
		int mesgLength = ensureNotNegative(cis.readRawVarint32());
		int dataLength = ensureNotNegative(cis.readRawVarint32());

		// Resetting buffer to end-of-header
		int headLength = cis.getTotalBytesRead();
		buffer.readerIndex(headLength);

		if (buffer.readableBytes() < mesgLength + dataLength) {
			// Not enough data to decode frame
			buffer.resetReaderIndex();
			return null;
		}

		// Parse protobuf message
		MessageLite message;
		if (buffer.hasArray()) {
			message = prototype.newBuilderForType()
					.mergeFrom(buffer.array(), buffer.arrayOffset() + headLength, mesgLength).build();
		} else {
			message = prototype.newBuilderForType().mergeFrom(new ChannelBufferInputStream(buffer, mesgLength)).build();
		}

		// Get optional data buffer
		if (dataLength > 0) {
			// Marking buffer as all read, creating slice for data
			buffer.readerIndex(headLength + mesgLength + dataLength);
			return frameFactory.newInstance(message, buffer.slice(headLength + mesgLength, dataLength));
		} else {
			// Marking buffer as all read
			buffer.readerIndex(headLength + mesgLength);
			return frameFactory.newInstance(message);
		}
	}

	private int ensureNotNegative(int length) throws CorruptedFrameException {
		if (length >= 0) {
			return length;
		}
		throw new CorruptedFrameException("negative length: " + length);
	}
}