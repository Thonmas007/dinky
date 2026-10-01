/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to you under the Apache License, Version 2.0
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
package org.dinky.service.impl;

import org.dinky.data.dto.KubernetesCommandDTO;
import org.dinky.data.dto.KubernetesClusterOptionDTO;
import org.dinky.data.dto.KubernetesCommandResultDTO;
import org.dinky.data.enums.GatewayType;
import org.dinky.gateway.config.K8sConfig;
import org.dinky.gateway.model.FlinkClusterConfig;
import org.dinky.service.ClusterConfigurationService;
import org.dinky.service.KubernetesCommandService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;

/** 集群查询命令执行器，确保用户输入不会经过 shell 解释。 */
@Service
@RequiredArgsConstructor
public class KubernetesCommandServiceImpl implements KubernetesCommandService {

    private static final long TIMEOUT_SECONDS = 30;
    private static final int MAX_OUTPUT_LENGTH = 1024 * 1024;
    private static final Pattern TOKEN_PATTERN =
            Pattern.compile("[\\p{Alnum}._:/=@%+,-]+|\\\"[^\\\"]*\\\"|'[^']*'");
    private static final Set<String> READ_ONLY_ACTIONS =
            new HashSet<>(Arrays.asList("get", "describe", "logs", "top", "version", "cluster-info"));
    private static final Set<String> FORBIDDEN_FLAGS =
            new HashSet<>(Arrays.asList(
                    "--watch",
                    "-w",
                    "--watch-only",
                    "-f",
                    "--follow",
                    "--request-timeout=0",
                    "--kubeconfig",
                    "--server",
                    "--token",
                    "--username",
                    "--password",
                    "--certificate-authority",
                    "--client-certificate",
                    "--client-key",
                    "--insecure-skip-tls-verify"));
    private static final Set<String> FORBIDDEN_FLAG_PREFIXES =
            new HashSet<>(Arrays.asList(
                    "--kubeconfig=",
                    "--server=",
                    "--token=",
                    "--username=",
                    "--password=",
                    "--certificate-authority=",
                    "--client-certificate=",
                    "--client-key=",
                    "--insecure-skip-tls-verify="));

    private final ClusterConfigurationService clusterConfigurationService;

    /** 返回 Kubernetes 配置摘要，供查询页选择实际执行命令的目标集群。 */
    @Override
    public List<KubernetesClusterOptionDTO> listConfigurations() {
        return clusterConfigurationService.listAllClusterConfig().stream()
                .filter(configuration -> isKubernetesType(configuration.getType()))
                .map(configuration -> new KubernetesClusterOptionDTO(
                        configuration.getId(),
                        configuration.getName(),
                        configuration.getType(),
                        configuration.getEnabled(),
                        configuration.getIsAvailable()))
                .collect(java.util.stream.Collectors.toList());
    }

    /** 执行已通过白名单校验的查询命令，并统一处理超时、退出码和输出上限。 */
    @Override
    public KubernetesCommandResultDTO execute(KubernetesCommandDTO request) {
        final List<String> args;
        Path kubeConfigFile = null;
        try {
            args = parseAndValidate(request == null ? null : request.getCommand());
            if (request == null || request.getClusterConfigurationId() == null) {
                throw new IllegalArgumentException("请选择 Kubernetes 集群配置");
            }
            kubeConfigFile = createKubeConfigFile(request.getClusterConfigurationId());
            args.add(1, "--kubeconfig");
            args.add(2, kubeConfigFile.toString());
        } catch (IllegalArgumentException e) {
            return new KubernetesCommandResultDTO(e.getMessage(), -1, false);
        } catch (IOException e) {
            return new KubernetesCommandResultDTO("无法读取 Kubernetes 集群配置：" + e.getMessage(), -1, false);
        }

        Process process = null;
        StringBuilder output = new StringBuilder();
        try {
            process = new ProcessBuilder(args).redirectErrorStream(true).start();
            Process runningProcess = process;
            Thread outputReader =
                    new Thread(() -> appendOutput(runningProcess, output), "kubectl-output-reader");
            outputReader.setDaemon(true);
            outputReader.start();
            boolean completed = process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!completed) {
                process.destroyForcibly();
                outputReader.join(1000);
                return new KubernetesCommandResultDTO(append(output.toString(),
                        "命令执行超时（超过 " + TIMEOUT_SECONDS + " 秒）"), -1, true);
            }
            outputReader.join(1000);
            return new KubernetesCommandResultDTO(output.toString(), process.exitValue(), false);
        } catch (IOException e) {
            return new KubernetesCommandResultDTO("无法执行 kubectl：" + e.getMessage(), -1, false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (process != null) {
                process.destroyForcibly();
            }
            return new KubernetesCommandResultDTO("命令执行被中断", -1, false);
        } finally {
            deleteKubeConfigFile(kubeConfigFile);
        }
    }

    /** 从 Dinky 已保存的集群配置生成短生命周期文件，避免把 kubeconfig 复制到镜像或返回前端。 */
    private Path createKubeConfigFile(Integer clusterConfigurationId) throws IOException {
        FlinkClusterConfig clusterConfig =
                clusterConfigurationService.getAndCheckEnableFlinkClusterCfg(clusterConfigurationId);
        if (clusterConfig == null
                || clusterConfig.getType() == null
                || !isKubernetesType(clusterConfig.getType().getLongValue())) {
            throw new IllegalArgumentException("所选集群配置不是 Kubernetes 类型");
        }
        K8sConfig kubernetesConfig = clusterConfig.getKubernetesConfig();
        if (kubernetesConfig == null || StringUtils.isBlank(kubernetesConfig.getKubeConfig())) {
            throw new IllegalArgumentException("所选集群配置没有保存 KubeConfig");
        }
        Path kubeConfigFile = Files.createTempFile("dinky-kubeconfig-", ".yaml");
        try {
            Files.write(kubeConfigFile, kubernetesConfig.getKubeConfig().getBytes(StandardCharsets.UTF_8));
            try {
                Files.setPosixFilePermissions(
                        kubeConfigFile,
                        java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
            } catch (UnsupportedOperationException ignored) {
                // Windows 等不支持 POSIX 权限的文件系统仍由临时目录权限提供隔离。
            }
            return kubeConfigFile;
        } catch (IOException e) {
            deleteKubeConfigFile(kubeConfigFile);
            throw e;
        }
    }

    /** 删除包含认证信息的临时文件，避免 kubeconfig 长时间残留在服务磁盘。 */
    private void deleteKubeConfigFile(Path kubeConfigFile) {
        if (kubeConfigFile != null) {
            try {
                Files.deleteIfExists(kubeConfigFile);
            } catch (IOException ignored) {
                // 命令结果已经生成，清理失败不应覆盖原始查询结果。
            }
        }
    }

    /** 仅允许复用 Dinky 已支持的三类 Kubernetes 提交配置。 */
    private boolean isKubernetesType(String type) {
        GatewayType gatewayType = GatewayType.get(type);
        return gatewayType == GatewayType.KUBERNETES_SESSION
                || gatewayType == GatewayType.KUBERNETES_APPLICATION
                || gatewayType == GatewayType.KUBERNETES_APPLICATION_OPERATOR;
    }

    /** 将输入拆成参数并只放行不会改变集群状态的 kubectl 子命令。 */
    static List<String> parseAndValidate(String command) {
        if (command == null || command.trim().isEmpty()) {
            throw new IllegalArgumentException("请输入 kubectl 命令");
        }
        List<String> tokens = new ArrayList<>();
        Matcher matcher = TOKEN_PATTERN.matcher(command.trim());
        int end = 0;
        while (matcher.find()) {
            if (!command.substring(end, matcher.start()).trim().isEmpty()) {
                throw new IllegalArgumentException("命令包含不支持的字符或 shell 操作符");
            }
            String token = matcher.group();
            if ((token.startsWith("\"") && token.endsWith("\""))
                    || (token.startsWith("'") && token.endsWith("'"))) {
                token = token.substring(1, token.length() - 1);
            }
            tokens.add(token);
            end = matcher.end();
        }
        if (!command.substring(end).trim().isEmpty()
                || tokens.size() < 2
                || !"kubectl".equals(tokens.get(0))) {
            throw new IllegalArgumentException("仅支持以 kubectl 开头的命令");
        }
        if (!READ_ONLY_ACTIONS.contains(tokens.get(1))) {
            throw new IllegalArgumentException(
                    "仅支持 get、describe、logs、top、version、cluster-info 查询命令");
        }
        for (String token : tokens) {
            if (isForbiddenFlag(token)) {
                throw new IllegalArgumentException("不允许覆盖集群连接参数或使用持续监听参数");
            }
        }
        return tokens;
    }

    /** 拦截空格和等号两种形式的凭证/连接覆盖参数，确保命令复用选定集群配置。 */
    private static boolean isForbiddenFlag(String token) {
        return FORBIDDEN_FLAGS.contains(token)
                || token.startsWith("--watch=")
                || FORBIDDEN_FLAG_PREFIXES.stream().anyMatch(token::startsWith);
    }

    /** 持续消费进程输出，即使达到返回上限也不能让 kubectl 阻塞在管道写入。 */
    private void appendOutput(Process process, StringBuilder output) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            boolean truncated = false;
            while ((line = reader.readLine()) != null) {
                // 达到返回上限后仍继续消费管道，避免 kubectl 因 stdout 缓冲区满而无法退出。
                if (output.length() < MAX_OUTPUT_LENGTH) {
                    if (output.length() > 0) {
                        output.append('\n');
                    }
                    output.append(line);
                }
                if (output.length() >= MAX_OUTPUT_LENGTH && !truncated) {
                    output.setLength(MAX_OUTPUT_LENGTH);
                    output.append("\n输出已截断");
                    truncated = true;
                }
            }
        } catch (IOException e) {
            if (output.length() > 0) {
                output.append('\n');
            }
            output.append("读取命令输出失败：").append(e.getMessage());
        }
    }

    /** 为超时结果追加原因，同时保留命令已经输出的诊断信息。 */
    private String append(String output, String suffix) {
        return output == null || output.isEmpty() ? suffix : output + "\n" + suffix;
    }
}
