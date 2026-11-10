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

import io.netty.util.AsciiString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;

import static io.netty.handler.codec.http.HttpHeadersTestUtils.of;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultHttpRequestTest {
    public static Collection<String> invalidUris() {
        return Arrays.asList(
                "http://localhost/\r\n",
                "/r\r\n?q=1",
                "http://localhost/\r\n?q=1",
                "/r\r\n/?q=1",
                "http://localhost/\r\n/?q=1",
                "/r\r\n",
                "http://localhost/ HTTP/1.1\r\n\r\nPOST /p HTTP/1.1\r\n\r\n",
                "/r HTTP/1.1\r\n\r\nPOST /p HTTP/1.1\r\n\r\n",
                "/ path",
                "/path ",
                " /path",
                "http://localhost/ ",
                " http://localhost/",
                "http://local host/"
        );
    }

    public static Collection<String> invalidMethods() {
        return Arrays.asList(
                "GET ",
                " GET",
                "G ET",
                " GET ",
                "GET\r",
                "GET\n",
                "GET\r\n",
                "GE\rT",
                "GE\nT",
                "GE\r\nT",
                "\rGET",
                "\nGET",
                "\r\nGET",
                " \r\nGET",
                "\r \nGET",
                "\r\n GET",
                "\r\nGET ",
                "\nGET ",
                "\rGET ",
                "\r GET",
                " \rGET",
                "\nGET ",
                "\n GET",
                " \nGET",
                "GET \n",
                "GET \r",
                " GET\r",
                " GET\r",
                "GET \n",
                " GET\n",
                " GET\n",
                "GE\nT ",
                "GE\rT ",
                " GE\rT",
                " GE\rT",
                "GE\nT ",
                " GE\nT",
                " GE\nT"
        );
    }

    public static Collection<String> validUris() {
        return Arrays.asList(
                "/",
                "/path",
                "/path?query=value",
                "http://localhost/",
                "http://localhost/path",
                "*"
        );
    }

    public static Collection<String> validMethods() {
        return Arrays.asList(
                "GET",
                "POST",
                "PUT",
                "HEAD",
                "DELETE",
                "OPTIONS",
                "CONNECT",
                "TRACE",
                "PATCH",
                "QUERY"
        );
    }

    @ParameterizedTest
    @MethodSource("invalidUris")
    void constructorMustRejectIllegalUrisByDefault(final String uri) {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
            }
        });
    }

    @ParameterizedTest
    @MethodSource("invalidUris")
    void setUriMustRejectIllegalUrisByDefault(final String uri) {
        final DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                request.setUri(uri);
            }
        });
    }

    @ParameterizedTest
    @MethodSource("validUris")
    void constructorMustAcceptValidUris(String uri) {
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    }

    @ParameterizedTest
    @MethodSource("validUris")
    void setUriMustAcceptValidUris(String uri) {
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/").setUri(uri);
    }

    @ParameterizedTest
    @MethodSource("invalidMethods")
    void constructorMustRejectIllegalHttpMethodByDefault(final String method) {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new DefaultHttpRequest(HttpVersion.HTTP_1_0, new HttpMethod("GET") {
                    @Override
                    public AsciiString asciiName() {
                        return new AsciiString(method);
                    }
                }, "/");
            }
        });
    }

    @ParameterizedTest
    @MethodSource("invalidMethods")
    void setMethodMustRejectIllegalHttpMethodByDefault(final String method) {
        final DefaultHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                request.setMethod(new HttpMethod("GET") {
                    @Override
                    public AsciiString asciiName() {
                        return new AsciiString(method);
                    }
                });
            }
        });
    }

    @ParameterizedTest
    @MethodSource("validMethods")
    void constructorMustAcceptAllHttpMethods(final String method) {
        new DefaultHttpRequest(HttpVersion.HTTP_1_0, new HttpMethod("GET") {
            @Override
            public AsciiString asciiName() {
                return new AsciiString(method);
            }
        }, "/");

        new DefaultHttpRequest(HttpVersion.HTTP_1_0, new HttpMethod(method), "/");
    }

    @ParameterizedTest
    @MethodSource("validMethods")
    void setMethodMustAcceptAllHttpMethods(final String method) {
        DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");

        request.setMethod(new HttpMethod("GET") {
            @Override
            public AsciiString asciiName() {
                return new AsciiString(method);
            }
        });

        request.setMethod(new HttpMethod(method));
    }

    @Test
    public void testHeaderRemoval() {
        HttpMessage m = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        HttpHeaders h = m.headers();

        // Insert sample keys.
        for (int i = 0; i < 1000; i ++) {
            h.set(of(String.valueOf(i)), AsciiString.EMPTY_STRING);
        }

        // Remove in reversed order.
        for (int i = 999; i >= 0; i --) {
            h.remove(of(String.valueOf(i)));
        }

        // Check if random access returns nothing.
        for (int i = 0; i < 1000; i ++) {
            assertNull(h.get(of(String.valueOf(i))));
        }

        // Check if sequential access returns nothing.
        assertTrue(h.isEmpty());
    }
}
