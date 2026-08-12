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

import cn.hutool.core.util.StrUtil;
import cn.hutool.core.util.URLUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import cn.hutool.http.Method;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.SneakyThrows;

@Controller
@Api(tags = "Flink Proxy API", hidden = true)
@RequestMapping(FlinkProxyController.API)
public class FlinkProxyController {
    public static final String API = "/api/flink/";

    private static final Set<String> HANDOFF_QUERY_KEYS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "_bg_uid", "_bg_tenant", "_bg_exp", "_bg_sig")));

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
        // 免登录代理只承载 WebUI 查看请求，禁止通过该入口调用 Flink 写接口。
        if (!isReadOnlyMethod(request.getMethod())) {
            resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Flink WebUI proxy is read-only");
            return;
        }
        // 兼容已签发的旧链接，但签名参数不再参与鉴权，也绝不能继续转发给 Flink JobManager。
        String query = removeHandoffQuery(request.getQueryString());
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
