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
package org.dinky.controller;

import org.dinky.data.constant.PermissionConstants;
import org.dinky.data.dto.KubernetesCommandDTO;
import org.dinky.data.dto.KubernetesClusterOptionDTO;
import org.dinky.data.dto.KubernetesCommandResultDTO;
import org.dinky.data.result.Result;
import org.dinky.service.KubernetesCommandService;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cn.dev33.satoken.annotation.SaCheckLogin;
import cn.dev33.satoken.annotation.SaCheckPermission;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;

/** 集群查询页面接口，仅提供受限的只读 kubectl 查询能力。 */
@RestController
@Api(tags = "Kubernetes Command Controller")
@RequestMapping("/api/kubernetes")
@SaCheckLogin
@RequiredArgsConstructor
public class KubernetesCommandController {

    private final KubernetesCommandService kubernetesCommandService;

    /** 返回可复用的 Kubernetes 集群配置，不暴露配置中的认证内容。 */
    @GetMapping("/configurations")
    @ApiOperation("List Kubernetes configurations")
    @SaCheckPermission(PermissionConstants.REGISTRATION_CLUSTER_KUBERNETES_COMMAND)
    public Result<List<KubernetesClusterOptionDTO>> listConfigurations() {
        return Result.succeed(kubernetesCommandService.listConfigurations());
    }

    /** 接收页面查询请求并交给服务层执行，接口本身不拼接或解释 shell 命令。 */
    @PostMapping("/execute")
    @ApiOperation("Execute read-only Kubernetes command")
    @SaCheckPermission(PermissionConstants.REGISTRATION_CLUSTER_KUBERNETES_COMMAND)
    public Result<KubernetesCommandResultDTO> execute(@RequestBody KubernetesCommandDTO command) {
        return Result.succeed(kubernetesCommandService.execute(command));
    }
}
