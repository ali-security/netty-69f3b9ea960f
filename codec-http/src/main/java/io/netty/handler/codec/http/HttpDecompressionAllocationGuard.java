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
package io.netty.handler.codec.http;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.util.ReferenceCountUtil;

/**
 * Enforces a cumulative ceiling on the total number of decompressed bytes a
 * content decoder may emit for a single HTTP message.
 * <p>
 * Some compression codecs used by {@link HttpContentDecompressor} (notably
 * Brotli) do not honor the configured {@code maxAllocation} natively: their
 * decoders stream decompressed output with no total-size bound, so a small
 * compressed body can expand without limit (a "decompression bomb"). This is
 * the weakness described by CVE-2026-42587.
 * <p>
 * This handler is appended to the decompression {@link io.netty.channel.embedded.EmbeddedChannel}
 * immediately after such a decoder. It sums the readable bytes of every
 * decompressed buffer and, once the running total exceeds {@code maxAllocation},
 * throws a {@link DecompressionException} — the same reject-on-overflow behavior
 * that {@link io.netty.handler.codec.compression.ZlibDecoder} already applies to
 * {@code gzip} / {@code deflate}. Because the zlib decoders only bound a single
 * decompression buffer, the guard is appended after them as well so that the
 * limit also holds across all the chunks of a message.
 * <p>
 * Once the limit has been exceeded the exception is raised only once; every
 * decompressed buffer produced afterwards for the same message is released and
 * dropped silently, mirroring how {@code ZlibDecoder} discards input after its
 * buffer is exhausted. This prevents the remainder of the bomb from raising one
 * exception (and log entry) per decompressed buffer.
 * <p>
 * A {@code maxAllocation} of {@code 0} means "no limit", matching the existing
 * {@link HttpContentDecompressor} contract, so the guard is a no-op unless a
 * limit was explicitly configured.
 */
final class HttpDecompressionAllocationGuard extends ChannelInboundHandlerAdapter {

    private final int maxAllocation;
    private long decompressedBytes;
    private boolean exceeded;

    HttpDecompressionAllocationGuard(int maxAllocation) {
        this.maxAllocation = maxAllocation;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (maxAllocation > 0 && msg instanceof ByteBuf) {
            if (exceeded) {
                // The limit was already reported, discard everything that follows.
                ReferenceCountUtil.release(msg);
                return;
            }
            decompressedBytes += ((ByteBuf) msg).readableBytes();
            if (decompressedBytes > maxAllocation) {
                exceeded = true;
                ReferenceCountUtil.release(msg);
                throw new DecompressionException(
                        "Decompression buffer has exceeded maximum size: " + maxAllocation + " bytes");
            }
        }
        ctx.fireChannelRead(msg);
    }
}
