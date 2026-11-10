/*
 * Copyright 2019 The Netty Project
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
package io.netty.handler.codec.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.Brotli;
import io.netty.handler.codec.compression.CompressionOptions;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.compression.Zstd;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HttpContentDecompressorTest {

    private static final int MAX_ALLOCATION = 64 * 1024;
    private static final int BOMB_CHUNK_SIZE = 16 * 1024;
    // 512 KiB of zeros once decompressed, eight times the configured maxAllocation.
    private static final int BOMB_CHUNKS = 32;

    // See https://github.com/netty/netty/issues/8915.
    @Test
    public void testInvokeReadWhenNotProduceMessage() {
        final AtomicInteger readCalled = new AtomicInteger();
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void read(ChannelHandlerContext ctx) {
                readCalled.incrementAndGet();
                ctx.read();
            }
        }, new HttpContentDecompressor(), new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                ctx.fireChannelRead(msg);
                ctx.read();
            }
        });

        channel.config().setAutoRead(false);

        readCalled.set(0);
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.CONTENT_ENCODING, "gzip");
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json;charset=UTF-8");
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);

        assertTrue(channel.writeInbound(response));

        // we triggered read explicitly
        assertEquals(1, readCalled.get());

        assertTrue(channel.readInbound() instanceof HttpResponse);

        assertFalse(channel.writeInbound(new DefaultHttpContent(Unpooled.EMPTY_BUFFER)));

        // read was triggered by the HttpContentDecompressor itself as it did not produce any message to the next
        // inbound handler.
        assertEquals(2, readCalled.get());
        assertFalse(channel.finishAndReleaseAll());
    }

    static String[] encodings() {
        List<String> encodings = new ArrayList<String>();
        encodings.add("gzip");
        encodings.add("deflate");
        if (Brotli.isAvailable()) {
            encodings.add("br");
        }
        if (Zstd.isAvailable()) {
            encodings.add("zstd");
        }
        encodings.add("snappy");
        return encodings.toArray(new String[0]);
    }

    @ParameterizedTest
    @MethodSource("encodings")
    public void testZipBomb(String encoding) {
        int chunkSize = 1024 * 1024;
        int numberOfChunks = 64;
        int memoryLimit = chunkSize * 128;

        EmbeddedChannel compressionChannel = new EmbeddedChannel(new HttpContentCompressor());
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        req.headers().set(HttpHeaderNames.ACCEPT_ENCODING, encoding);
        compressionChannel.writeInbound(req);

        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        compressionChannel.writeOutbound(response);

        for (int i = 0; i < numberOfChunks; i++) {
            ByteBuf buffer = compressionChannel.alloc().buffer(chunkSize);
            buffer.writeZero(chunkSize);
            compressionChannel.writeOutbound(new DefaultHttpContent(buffer));
        }
        compressionChannel.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT);
        compressionChannel.finish();
        compressionChannel.releaseInbound();

        ByteBuf compressed = compressionChannel.alloc().buffer();
        HttpMessage message = null;
        while (true) {
            HttpObject obj = compressionChannel.readOutbound();
            if (obj == null) {
                break;
            }
            if (obj instanceof HttpMessage) {
                message = (HttpMessage) obj;
            }
            if (obj instanceof HttpContent) {
                HttpContent content = (HttpContent) obj;
                compressed.writeBytes(content.content());
                content.release();
            }
        }

        PooledByteBufAllocator allocator = new PooledByteBufAllocator(false);

        ZipBombIncomingHandler incomingHandler = new ZipBombIncomingHandler(memoryLimit);
        EmbeddedChannel decompressChannel = new EmbeddedChannel(new HttpContentDecompressor(false), incomingHandler);
        decompressChannel.config().setAllocator(allocator);
        decompressChannel.writeInbound(message);
        decompressChannel.writeInbound(new DefaultLastHttpContent(compressed));

        assertEquals((long) chunkSize * numberOfChunks, incomingHandler.total);
    }

    static String[] maxAllocationEncodings() {
        List<String> encodings = new ArrayList<String>();
        encodings.add("gzip");
        encodings.add("deflate");
        if (Brotli.isAvailable()) {
            encodings.add("br");
        }
        encodings.add("snappy");
        return encodings.toArray(new String[0]);
    }

    // See CVE-2026-42587: a small compressed body must not decompress past the configured maxAllocation.
    @ParameterizedTest
    @MethodSource("maxAllocationEncodings")
    public void testDecompressionBombRejectedWithMaxAllocation(String encoding) {
        final ByteBuf compressed = concat(compressZeros(encoding, BOMB_CHUNK_SIZE, BOMB_CHUNKS));
        assertTrue(compressed.readableBytes() < MAX_ALLOCATION);

        ContentCountingHandler counter = new ContentCountingHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(new HttpContentDecompressor(MAX_ALLOCATION), counter);
        channel.writeInbound(newCompressedResponse(encoding));
        assertThrows(DecompressionException.class, new Executable() {
            @Override
            public void execute() {
                channel.writeInbound(new DefaultLastHttpContent(compressed));
            }
        });

        assertTrue(counter.contentBytes <= MAX_ALLOCATION,
                "decompressed " + counter.contentBytes + " bytes, limit is " + MAX_ALLOCATION);
        assertFalse(channel.finishAndReleaseAll());
    }

    // Every chunk stays below maxAllocation on its own, the limit must hold for the whole message and
    // must be reported only once, not once per chunk of the remaining bomb.
    @ParameterizedTest
    @MethodSource("maxAllocationEncodings")
    public void testDecompressionBombAcrossChunksRejectedOnce(String encoding) {
        List<ByteBuf> chunks = compressZeros(encoding, BOMB_CHUNK_SIZE, BOMB_CHUNKS);

        ContentCountingHandler counter = new ContentCountingHandler();
        EmbeddedChannel channel = new EmbeddedChannel(new HttpContentDecompressor(MAX_ALLOCATION), counter);
        channel.writeInbound(newCompressedResponse(encoding));
        int failures = 0;
        for (ByteBuf chunk : chunks) {
            try {
                channel.writeInbound(new DefaultHttpContent(chunk));
            } catch (DecompressionException expected) {
                failures++;
            }
        }
        channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);

        assertEquals(1, failures);
        assertTrue(counter.contentBytes <= MAX_ALLOCATION,
                "decompressed " + counter.contentBytes + " bytes, limit is " + MAX_ALLOCATION);
        assertTrue(counter.lastContentReceived);
        assertFalse(channel.finishAndReleaseAll());
    }

    @ParameterizedTest
    @MethodSource("maxAllocationEncodings")
    public void testDecompressionWithinMaxAllocation(String encoding) {
        int numberOfChunks = 3;
        List<ByteBuf> chunks = compressZeros(encoding, BOMB_CHUNK_SIZE, numberOfChunks);

        ContentCountingHandler counter = new ContentCountingHandler();
        EmbeddedChannel channel = new EmbeddedChannel(new HttpContentDecompressor(MAX_ALLOCATION), counter);
        channel.writeInbound(newCompressedResponse(encoding));
        for (ByteBuf chunk : chunks) {
            channel.writeInbound(new DefaultHttpContent(chunk));
        }
        channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);

        assertEquals((long) BOMB_CHUNK_SIZE * numberOfChunks, counter.contentBytes);
        assertTrue(counter.lastContentReceived);
        assertFalse(channel.finishAndReleaseAll());
    }

    @ParameterizedTest
    @MethodSource("maxAllocationEncodings")
    public void testDecompressionUnlimitedByDefault(String encoding) {
        HttpContentDecompressor[] decompressors = {
                new HttpContentDecompressor(), new HttpContentDecompressor(0), new HttpContentDecompressor(true, 0)
        };
        for (HttpContentDecompressor decompressor : decompressors) {
            ByteBuf compressed = concat(compressZeros(encoding, BOMB_CHUNK_SIZE, BOMB_CHUNKS));

            ContentCountingHandler counter = new ContentCountingHandler();
            EmbeddedChannel channel = new EmbeddedChannel(decompressor, counter);
            channel.writeInbound(newCompressedResponse(encoding));
            channel.writeInbound(new DefaultLastHttpContent(compressed));

            assertEquals((long) BOMB_CHUNK_SIZE * BOMB_CHUNKS, counter.contentBytes);
            assertTrue(counter.lastContentReceived);
            assertFalse(channel.finishAndReleaseAll());
        }
    }

    @Test
    public void testNegativeMaxAllocationRejected() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                new HttpContentDecompressor(-1);
            }
        });
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                new HttpContentDecompressor(true, -1);
            }
        });
    }

    private static HttpResponse newCompressedResponse(String encoding) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.CONTENT_ENCODING, encoding);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        return response;
    }

    /**
     * Compresses {@code numberOfChunks} chunks of {@code chunkSize} zeros with the given encoding and returns the
     * compressed form of every chunk, followed by the trailer the encoder emits when the message ends.
     */
    private static List<ByteBuf> compressZeros(String encoding, int chunkSize, int numberOfChunks) {
        // Passing no CompressionOptions enables every available encoding, including br and snappy.
        EmbeddedChannel compressionChannel = new EmbeddedChannel(
                new HttpContentCompressor((CompressionOptions[]) null));
        DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        req.headers().set(HttpHeaderNames.ACCEPT_ENCODING, encoding);
        compressionChannel.writeInbound(req);

        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        compressionChannel.writeOutbound(response);
        HttpResponse encodedResponse = compressionChannel.readOutbound();
        assertEquals(encoding, encodedResponse.headers().get(HttpHeaderNames.CONTENT_ENCODING));

        List<ByteBuf> chunks = new ArrayList<ByteBuf>();
        for (int i = 0; i < numberOfChunks; i++) {
            ByteBuf buffer = compressionChannel.alloc().buffer(chunkSize);
            buffer.writeZero(chunkSize);
            compressionChannel.writeOutbound(new DefaultHttpContent(buffer));
            chunks.add(readCompressedContent(compressionChannel));
        }
        compressionChannel.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT);
        chunks.add(readCompressedContent(compressionChannel));
        compressionChannel.finishAndReleaseAll();
        return chunks;
    }

    private static ByteBuf readCompressedContent(EmbeddedChannel channel) {
        ByteBuf compressed = Unpooled.buffer();
        for (;;) {
            Object obj = channel.readOutbound();
            if (obj == null) {
                return compressed;
            }
            if (obj instanceof HttpContent) {
                compressed.writeBytes(((HttpContent) obj).content());
            }
            ReferenceCountUtil.release(obj);
        }
    }

    private static ByteBuf concat(List<ByteBuf> buffers) {
        ByteBuf result = Unpooled.buffer();
        for (ByteBuf buffer : buffers) {
            result.writeBytes(buffer);
            buffer.release();
        }
        return result;
    }

    private static final class ZipBombIncomingHandler extends ChannelInboundHandlerAdapter {
        final int memoryLimit;
        long total;

        ZipBombIncomingHandler(int memoryLimit) {
            this.memoryLimit = memoryLimit;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            PooledByteBufAllocator allocator = (PooledByteBufAllocator) ctx.alloc();
            assertTrue(allocator.metric().usedHeapMemory() < memoryLimit);
            assertTrue(allocator.metric().usedDirectMemory() < memoryLimit);

            if (msg instanceof HttpContent) {
                HttpContent buf = (HttpContent) msg;
                total += buf.content().readableBytes();
                buf.release();
            }
        }
    }

    private static final class ContentCountingHandler extends ChannelInboundHandlerAdapter {
        long contentBytes;
        boolean lastContentReceived;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpContent) {
                contentBytes += ((HttpContent) msg).content().readableBytes();
                if (msg instanceof LastHttpContent) {
                    lastContentReceived = true;
                }
            }
            ReferenceCountUtil.release(msg);
        }
    }
}
