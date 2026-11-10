/*
 * Copyright 2012 The Netty Project
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

import static io.netty.handler.codec.http.HttpHeaderValues.BR;
import static io.netty.handler.codec.http.HttpHeaderValues.DEFLATE;
import static io.netty.handler.codec.http.HttpHeaderValues.GZIP;
import static io.netty.handler.codec.http.HttpHeaderValues.X_DEFLATE;
import static io.netty.handler.codec.http.HttpHeaderValues.X_GZIP;
import static io.netty.handler.codec.http.HttpHeaderValues.SNAPPY;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.Brotli;
import io.netty.handler.codec.compression.BrotliDecoder;
import io.netty.handler.codec.compression.JZlibDecoder;
import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibDecoder;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;

/**
 * Decompresses an {@link HttpMessage} and an {@link HttpContent} compressed in
 * {@code gzip} or {@code deflate} encoding.  For more information on how this
 * handler modifies the message, please refer to {@link HttpContentDecoder}.
 */
public class HttpContentDecompressor extends HttpContentDecoder {

    // Same switch ZlibCodecFactory uses to pick between the JDK and the JZlib based decoder.
    private static final boolean NO_JDK_ZLIB_DECODER = SystemPropertyUtil.getBoolean("io.netty.noJdkZlibDecoder",
            PlatformDependent.javaVersion() < 7);

    private final boolean strict;
    private final int maxAllocation;

    /**
     * Create a new {@link HttpContentDecompressor} in non-strict mode.
     */
    public HttpContentDecompressor() {
        this(false);
    }

    /**
     * Create a new {@link HttpContentDecompressor}.
     *
     * @param strict    if {@code true} use strict handling of deflate if used, otherwise handle it in a
     *                  more lenient fashion.
     */
    public HttpContentDecompressor(boolean strict) {
        this(strict, 0);
    }

    /**
     * Create a new {@link HttpContentDecompressor} in non-strict mode.
     *
     * @param maxAllocation Maximum size of the decompression buffer. Must be &gt;= 0.
     *                      If zero, maximum size is not limited. The limit also caps the total number of
     *                      decompressed bytes of a single message; content exceeding it is rejected with a
     *                      {@link io.netty.handler.codec.compression.DecompressionException}.
     */
    public HttpContentDecompressor(int maxAllocation) {
        this(false, maxAllocation);
    }

    /**
     * Create a new {@link HttpContentDecompressor}.
     *
     * @param strict    if {@code true} use strict handling of deflate if used, otherwise handle it in a
     *                  more lenient fashion.
     * @param maxAllocation Maximum size of the decompression buffer. Must be &gt;= 0.
     *                      If zero, maximum size is not limited. The limit also caps the total number of
     *                      decompressed bytes of a single message; content exceeding it is rejected with a
     *                      {@link io.netty.handler.codec.compression.DecompressionException}.
     */
    public HttpContentDecompressor(boolean strict, int maxAllocation) {
        if (maxAllocation < 0) {
            throw new IllegalArgumentException("maxAllocation: " + maxAllocation + " (expected >= 0)");
        }
        this.strict = strict;
        this.maxAllocation = maxAllocation;
    }

    @Override
    protected EmbeddedChannel newContentDecoder(String contentEncoding) throws Exception {
        if (GZIP.contentEqualsIgnoreCase(contentEncoding) ||
            X_GZIP.contentEqualsIgnoreCase(contentEncoding)) {
            return new EmbeddedChannel(ctx.channel().id(), ctx.channel().metadata().hasDisconnect(),
                    ctx.channel().config(), newZlibDecoder(ZlibWrapper.GZIP),
                    new HttpDecompressionAllocationGuard(maxAllocation));
        }
        if (DEFLATE.contentEqualsIgnoreCase(contentEncoding) ||
            X_DEFLATE.contentEqualsIgnoreCase(contentEncoding)) {
            final ZlibWrapper wrapper = strict ? ZlibWrapper.ZLIB : ZlibWrapper.ZLIB_OR_NONE;
            // To be strict, 'deflate' means ZLIB, but some servers were not implemented correctly.
            return new EmbeddedChannel(ctx.channel().id(), ctx.channel().metadata().hasDisconnect(),
                    ctx.channel().config(), newZlibDecoder(wrapper),
                    new HttpDecompressionAllocationGuard(maxAllocation));
        }
        // Brotli/Snappy decoders do not enforce the configured maxAllocation natively, so a
        // decompression bomb can bypass the limit (CVE-2026-42587). Append a guard that caps the
        // cumulative decompressed output, mirroring the reject-on-overflow behavior of the zlib path.
        if (Brotli.isAvailable() && BR.contentEqualsIgnoreCase(contentEncoding)) {
            return new EmbeddedChannel(ctx.channel().id(), ctx.channel().metadata().hasDisconnect(),
              ctx.channel().config(), new BrotliDecoder(),
              new HttpDecompressionAllocationGuard(maxAllocation));
        }

        if (SNAPPY.contentEqualsIgnoreCase(contentEncoding)) {
            return new EmbeddedChannel(ctx.channel().id(), ctx.channel().metadata().hasDisconnect(),
                    ctx.channel().config(), new SnappyFrameDecoder(),
                    new HttpDecompressionAllocationGuard(maxAllocation));
        }

        // 'identity' or unsupported
        return null;
    }

    private ZlibDecoder newZlibDecoder(ZlibWrapper wrapper) {
        if (maxAllocation == 0) {
            return ZlibCodecFactory.newZlibDecoder(wrapper);
        }
        // The zlib decoders only bound a single decompression buffer by maxAllocation, the
        // HttpDecompressionAllocationGuard that follows them caps the cumulative output (CVE-2026-42587).
        if (PlatformDependent.javaVersion() < 7 || NO_JDK_ZLIB_DECODER) {
            return new JZlibDecoder(wrapper, maxAllocation);
        }
        return new JdkZlibDecoder(wrapper, true, maxAllocation);
    }
}
