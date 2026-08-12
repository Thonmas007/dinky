/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.dinky.controller;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import org.dinky.security.FlinkWebLinkAuthenticator;
import org.dinky.security.FlinkWebLinkAuthenticator.AuthorizationResult;

import cn.dev33.satoken.stp.StpUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.core.util.URLUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import cn.hutool.http.Method;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@Controller
@Api(tags = "Flink Proxy API", hidden = true)
@RequestMapping(FlinkProxyController.API)
@RequiredArgsConstructor
public class FlinkProxyController {
    public static final String API = "/api/flink/";

    private static final Set<String> HANDOFF_QUERY_KEYS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            FlinkWebLinkAuthenticator.USER_PARAM,
            FlinkWebLinkAuthenticator.TENANT_PARAM,
            FlinkWebLinkAuthenticator.EXPIRES_PARAM,
            FlinkWebLinkAuthenticator.SIGNATURE_PARAM)));

    private final FlinkWebLinkAuthenticator linkAuthenticator;

    @RequestMapping("/**")
    @ApiOperation("Flink Proxy API")
    public void proxyUba(HttpServletRequest request, HttpServletResponse resp)
            throws URISyntaxException, IOException {
        // String url = URLDecoder.decode(request.getRequestURL().toString(), "UTF-8");
        URI uri = new URI(request.getRequestURI());
        String path = uri.getPath();
        if (!StrUtil.contains(path, API)) {
            return;
        }
        path = path.replace(API, "");
        if (StrUtil.isBlank(path)) {
            return;
        }
        String address = extractAddress(path);
        // BlueGame 签发的免登录会话仅用于查看 WebUI；写请求仍要求完整 Dinky 登录态。
        if (!isReadOnlyMethod(request.getMethod()) && !StpUtil.isLogin()) {
            resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Signed Flink WebUI sessions are read-only");
            return;
        }
        AuthorizationResult authorization = StrUtil.isBlank(address)
                ? AuthorizationResult.DENIED
                : linkAuthenticator.authorize(request, resp, address);
        if (authorization == AuthorizationResult.DENIED) {
            resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Flink WebUI link is invalid or expired");
            return;
        }
        // 换取浏览器会话的签名参数只供 Dinky 验证，绝不能继续转发给 Flink JobManager。
        String query = removeHandoffQuery(request.getQueryString());
        if (authorization == AuthorizationResult.HANDOFF_ESTABLISHED) {
            // 先落受限 Cookie 再跳转到干净地址，避免一次性签名页面被浏览器缓存或出现资源加载白屏。
            resp.setStatus(HttpServletResponse.SC_FOUND);
            resp.setHeader("Location", buildCleanRedirect(request.getRequestURI(), query));
            return;
        }
        if (StrUtil.isNotBlank(query)) {
            path = HttpUtil.urlWithForm(path, URLUtil.decode(query), StandardCharsets.UTF_8, true);
        }
        HttpRequest httpRequest = HttpUtil.createRequest(Method.valueOf(request.getMethod()), path);
        try (HttpResponse httpResponse = httpRequest.execute()) {
            writeToHttpServletResponse(httpResponse, resp);
        }
    }

    private boolean isReadOnlyMethod(String method) {
        return "GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method);
    }

    private String extractAddress(String path) {
        int slashIndex = path.indexOf('/');
        String encodedAddress = slashIndex < 0 ? path : path.substring(0, slashIndex);
        return URLUtil.decode(encodedAddress);
    }

    private String removeHandoffQuery(String query) {
        if (StrUtil.isBlank(query)) {
            return query;
        }
        return Arrays.stream(query.split("&"))
                .filter(item -> {
                    int separator = item.indexOf('=');
                    String key = separator < 0 ? item : item.substring(0, separator);
                    return !HANDOFF_QUERY_KEYS.contains(URLUtil.decode(key));
                })
                .collect(Collectors.joining("&"));
    }

    private String buildCleanRedirect(String requestUri, String query) {
        return StrUtil.isBlank(query) ? requestUri : requestUri + "?" + query;
    }

    @SneakyThrows
    public void writeToHttpServletResponse(HttpResponse httpResponse, HttpServletResponse resp) {
        if (httpResponse.body() != null) {
            httpResponse.headers().forEach((k, v) -> {
                if (StrUtil.isNotBlank(k)) {
                    resp.addHeader(k, v.get(0));
                }
            });
            httpResponse.writeBody(resp.getOutputStream(), true, null);
        }
    }
}
