/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */
package org.dinky.security;

import org.dinky.data.model.SysToken;
import org.dinky.mapper.TokenMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Component;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;

import cn.dev33.satoken.stp.StpUtil;
import cn.hutool.core.util.StrUtil;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

/** 为外部平台签发的短时链接建立受限 WebUI 会话，避免公开 Dinky Token 或放开任意内网代理。 */
@Component
@RequiredArgsConstructor
public class FlinkWebLinkAuthenticator {
    public static final String USER_PARAM = "_bg_uid";
    public static final String TENANT_PARAM = "_bg_tenant";
    public static final String EXPIRES_PARAM = "_bg_exp";
    public static final String SIGNATURE_PARAM = "_bg_sig";

    private static final long LINK_CLOCK_SKEW_SECONDS = 10;
    private static final long LINK_MAX_FUTURE_SECONDS = 120;
    private static final long SESSION_SECONDS = 10 * 60 * 60;
    private static final String HMAC_ALGORITHM = "HmacSHA256";

    private final TokenMapper tokenMapper;

    /** 已登录 Dinky 用户沿用原会话；BlueGame 跳转则以一分钟签名换取仅限该 JobManager 的 HttpOnly 会话。 */
    public AuthorizationResult authorize(HttpServletRequest request, HttpServletResponse response, String address) {
        if (StpUtil.isLogin()) {
            return AuthorizationResult.AUTHORIZED;
        }
        if (verifySessionCookie(request, address)) {
            return AuthorizationResult.AUTHORIZED;
        }
        SignedIdentity identity = verifyLink(request, address);
        if (identity == null) {
            return AuthorizationResult.DENIED;
        }
        writeSessionCookie(request, response, address, identity);
        return AuthorizationResult.HANDOFF_ESTABLISHED;
    }

    private SignedIdentity verifyLink(HttpServletRequest request, String address) {
        long userId = parsePositiveLong(request.getParameter(USER_PARAM));
        long tenantId = parsePositiveLong(request.getParameter(TENANT_PARAM));
        long expiresAt = parsePositiveLong(request.getParameter(EXPIRES_PARAM));
        String signature = request.getParameter(SIGNATURE_PARAM);
        long now = Instant.now().getEpochSecond();
        if (userId <= 0
                || tenantId <= 0
                || expiresAt < now - LINK_CLOCK_SKEW_SECONDS
                || expiresAt > now + LINK_MAX_FUTURE_SECONDS
                || StrUtil.isBlank(signature)) {
            return null;
        }
        String canonical = String.format("v1\n%d\n%d\n%s\n%d", userId, tenantId, address, expiresAt);
        SysToken token = findMatchingToken(userId, tenantId, canonical, signature);
        return token == null ? null : new SignedIdentity(userId, tenantId, token.getTokenValue());
    }

    private boolean verifySessionCookie(HttpServletRequest request, String address) {
        Cookie[] cookies = request.getCookies();
        if (cookies == null) {
            return false;
        }
        String cookieName = sessionCookieName(address);
        for (Cookie cookie : cookies) {
            if (!cookieName.equals(cookie.getName())) {
                continue;
            }
            String[] parts = StrUtil.splitToArray(cookie.getValue(), '.');
            if (parts.length != 2) {
                return false;
            }
            String payload;
            try {
                payload = new String(Base64.getUrlDecoder().decode(parts[0]), StandardCharsets.UTF_8);
            } catch (IllegalArgumentException ignored) {
                return false;
            }
            String[] fields = payload.split("\\n", -1);
            if (fields.length != 5 || !"v1-session".equals(fields[0]) || !address.equals(fields[3])) {
                return false;
            }
            long userId = parsePositiveLong(fields[1]);
            long tenantId = parsePositiveLong(fields[2]);
            long expiresAt = parsePositiveLong(fields[4]);
            if (expiresAt < Instant.now().getEpochSecond()) {
                return false;
            }
            return findMatchingToken(userId, tenantId, payload, parts[1]) != null;
        }
        return false;
    }

    private void writeSessionCookie(
            HttpServletRequest request, HttpServletResponse response, String address, SignedIdentity identity) {
        long expiresAt = Instant.now().getEpochSecond() + SESSION_SECONDS;
        String payload = String.format(
                "v1-session\n%d\n%d\n%s\n%d", identity.userId, identity.tenantId, address, expiresAt);
        String value = Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.UTF_8))
                + "."
                + sign(identity.token, payload);
        String header = sessionCookieName(address)
                + "="
                + value
                + "; Path=/api/flink/; Max-Age="
                + SESSION_SECONDS
                // 外部平台首次打开属于跨站顶层导航，Lax 可保证 302 后的干净地址携带该受限会话。
                + "; HttpOnly; SameSite=Lax"
                + (request.isSecure() ? "; Secure" : "");
        response.addHeader("Set-Cookie", header);
    }

    /** 区分已有会话与刚完成签名交接，后者必须重定向以清理地址栏中的一次性签名。 */
    public enum AuthorizationResult {
        AUTHORIZED,
        HANDOFF_ESTABLISHED,
        DENIED
    }

    /** 只使用仍有效且属于同一租户服务身份的 Token 验签，Token 删除或过期后浏览器会话立即失效。 */
    private SysToken findMatchingToken(long userId, long tenantId, String canonical, String signature) {
        List<SysToken> tokens = tokenMapper.selectList(new LambdaQueryWrapper<SysToken>()
                .eq(SysToken::getUserId, userId)
                .eq(SysToken::getTenantId, tenantId));
        Date now = new Date();
        for (SysToken token : tokens) {
            if (StrUtil.isBlank(token.getTokenValue()) || !isActive(token, now)) {
                continue;
            }
            if (secureEquals(sign(token.getTokenValue(), canonical), signature)) {
                return token;
            }
        }
        return null;
    }

    private boolean isActive(SysToken token, Date now) {
        if (token.getExpireStartTime() != null && token.getExpireStartTime().after(now)) {
            return false;
        }
        return Integer.valueOf(1).equals(token.getExpireType())
                || (token.getExpireEndTime() != null && token.getExpireEndTime().after(now));
    }

    private String sessionCookieName(String address) {
        byte[] digest = sha256(address);
        return "dinky-flink-web-" + toHex(digest).substring(0, 16);
    }

    @SneakyThrows
    private String sign(String token, String canonical) {
        Mac mac = Mac.getInstance(HMAC_ALGORITHM);
        mac.init(new SecretKeySpec(token.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM));
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(mac.doFinal(canonical.getBytes(StandardCharsets.UTF_8)));
    }

    @SneakyThrows
    private byte[] sha256(String value) {
        return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private boolean secureEquals(String expected, String actual) {
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), StrUtil.nullToEmpty(actual).getBytes(StandardCharsets.UTF_8));
    }

    private long parsePositiveLong(String value) {
        try {
            return Long.parseLong(StrUtil.trim(value));
        } catch (RuntimeException ignored) {
            return -1;
        }
    }

    private String toHex(byte[] value) {
        StringBuilder builder = new StringBuilder(value.length * 2);
        for (byte item : value) {
            builder.append(String.format("%02x", item));
        }
        return builder.toString();
    }

    private static final class SignedIdentity {
        private final long userId;
        private final long tenantId;
        private final String token;

        private SignedIdentity(long userId, long tenantId, String token) {
            this.userId = userId;
            this.tenantId = tenantId;
            this.token = token;
        }
    }
}
