/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.spdy;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.FullHttpMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.HashMap;
import java.util.Map;

import static io.netty.handler.codec.spdy.SpdyHeaders.HttpNames.HOST;
import static io.netty.handler.codec.spdy.SpdyHeaders.HttpNames.METHOD;
import static io.netty.handler.codec.spdy.SpdyHeaders.HttpNames.PATH;
import static io.netty.handler.codec.spdy.SpdyHeaders.HttpNames.SCHEME;
import static io.netty.handler.codec.spdy.SpdyHeaders.HttpNames.STATUS;
import static io.netty.handler.codec.spdy.SpdyHeaders.HttpNames.VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SpdyHttpDecoderTest {

    // Client-initiated (odd) stream id, so SYN_STREAM frames are decoded as HTTP requests.
    private static final int STREAM_ID = 1;
    private static final int MAX_CONTENT_LENGTH = 1024;

    private final Map<Integer, FullHttpMessage> messageMap = new HashMap<Integer, FullHttpMessage>();

    private static final class TestSpdyHttpDecoder extends SpdyHttpDecoder {
        TestSpdyHttpDecoder(Map<Integer, FullHttpMessage> messageMap) {
            super(SpdyVersion.SPDY_3_1, MAX_CONTENT_LENGTH, messageMap);
        }
    }

    private static SpdySynStreamFrame newSynStreamFrame(int streamId) {
        SpdySynStreamFrame frame = new DefaultSpdySynStreamFrame(streamId, 0, (byte) 0);
        frame.headers()
                .set(METHOD, "POST")
                .set(PATH, "/upload")
                .set(VERSION, "HTTP/1.1")
                .set(HOST, "example.com")
                .set(SCHEME, "https");
        // Not the last frame: the request body follows, so the message is kept in the map.
        frame.setLast(false);
        return frame;
    }

    private static SpdySynReplyFrame newSynReplyFrame(int streamId) {
        SpdySynReplyFrame frame = new DefaultSpdySynReplyFrame(streamId);
        frame.headers()
                .set(STATUS, "200")
                .set(VERSION, "HTTP/1.1");
        frame.setLast(false);
        return frame;
    }

    @Test
    public void testDuplicateSynStreamReleasesPreviousMessage() {
        EmbeddedChannel channel = new EmbeddedChannel(new TestSpdyHttpDecoder(messageMap));

        assertFalse(channel.writeInbound(newSynStreamFrame(STREAM_ID)));
        FullHttpMessage first = messageMap.get(STREAM_ID);
        assertNotNull(first);

        // A second SYN_STREAM for the same stream must not leak the pending message it replaces.
        assertFalse(channel.writeInbound(newSynStreamFrame(STREAM_ID)));
        FullHttpMessage second = messageMap.get(STREAM_ID);
        assertNotNull(second);
        assertNotSame(first, second);
        assertEquals(0, first.refCnt());
        assertEquals(1, second.refCnt());

        assertFalse(channel.finish());
        assertEquals(0, second.refCnt());
        assertTrue(messageMap.isEmpty());
    }

    @Test
    public void testDuplicateSynReplyReleasesPreviousMessage() {
        EmbeddedChannel channel = new EmbeddedChannel(new TestSpdyHttpDecoder(messageMap));

        assertFalse(channel.writeInbound(newSynReplyFrame(STREAM_ID)));
        FullHttpMessage first = messageMap.get(STREAM_ID);
        assertNotNull(first);

        assertFalse(channel.writeInbound(newSynReplyFrame(STREAM_ID)));
        FullHttpMessage second = messageMap.get(STREAM_ID);
        assertNotNull(second);
        assertNotSame(first, second);
        assertEquals(0, first.refCnt());
        assertEquals(1, second.refCnt());

        assertFalse(channel.finish());
        assertEquals(0, second.refCnt());
        assertTrue(messageMap.isEmpty());
    }

    @Test
    public void testRstStreamReleasesPendingMessage() {
        EmbeddedChannel channel = new EmbeddedChannel(new TestSpdyHttpDecoder(messageMap));

        assertFalse(channel.writeInbound(newSynStreamFrame(STREAM_ID)));
        FullHttpMessage pending = messageMap.get(STREAM_ID);
        assertNotNull(pending);

        assertFalse(channel.writeInbound(new DefaultSpdyRstStreamFrame(STREAM_ID, SpdyStreamStatus.CANCEL)));
        assertTrue(messageMap.isEmpty());
        assertEquals(0, pending.refCnt());

        assertFalse(channel.finish());
    }

    @Test
    public void testTooLongContentReleasesPendingMessage() {
        final EmbeddedChannel channel = new EmbeddedChannel(new TestSpdyHttpDecoder(messageMap));

        assertFalse(channel.writeInbound(newSynStreamFrame(STREAM_ID)));
        FullHttpMessage pending = messageMap.get(STREAM_ID);
        assertNotNull(pending);

        final SpdyDataFrame dataFrame =
                new DefaultSpdyDataFrame(STREAM_ID, Unpooled.wrappedBuffer(new byte[MAX_CONTENT_LENGTH + 1]));
        assertThrows(TooLongFrameException.class, new Executable() {
            @Override
            public void execute() {
                channel.writeInbound(dataFrame);
            }
        });
        assertTrue(messageMap.isEmpty());
        assertEquals(0, pending.refCnt());
        assertEquals(0, dataFrame.refCnt());

        assertFalse(channel.finish());
    }

    @Test
    public void testHandlerRemovedReleasesPendingMessage() {
        SpdyHttpDecoder decoder = new TestSpdyHttpDecoder(messageMap);
        EmbeddedChannel channel = new EmbeddedChannel(decoder);

        assertFalse(channel.writeInbound(newSynStreamFrame(STREAM_ID)));
        FullHttpMessage pending = messageMap.get(STREAM_ID);
        assertNotNull(pending);

        // Removing the decoder while the channel is still active must not leak the pending message.
        channel.pipeline().remove(decoder);
        assertTrue(messageMap.isEmpty());
        assertEquals(0, pending.refCnt());

        assertFalse(channel.finish());
    }

    @Test
    public void testChannelInactiveReleasesPendingMessage() {
        EmbeddedChannel channel = new EmbeddedChannel(new TestSpdyHttpDecoder(messageMap));

        assertFalse(channel.writeInbound(newSynStreamFrame(STREAM_ID)));
        FullHttpMessage pending = messageMap.get(STREAM_ID);
        assertNotNull(pending);

        channel.pipeline().fireChannelInactive();
        assertTrue(messageMap.isEmpty());
        assertEquals(0, pending.refCnt());

        // Closing the channel triggers channelInactive and handlerRemoved again; the release must be idempotent.
        assertFalse(channel.finish());
        assertEquals(0, pending.refCnt());
    }
}
