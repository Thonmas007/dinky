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
package org.dinky.data.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** 查询页可选择的 Kubernetes 集群配置摘要，不向浏览器返回 kubeconfig 内容。 */
@Getter
@AllArgsConstructor
@ApiModel(value = "KubernetesClusterOptionDTO", description = "Kubernetes cluster option")
public class KubernetesClusterOptionDTO {

    @ApiModelProperty("集群配置 ID")
    private final Integer id;

    @ApiModelProperty("集群配置名称")
    private final String name;

    @ApiModelProperty("集群类型")
    private final String type;

    @ApiModelProperty("是否启用")
    private final Boolean enabled;

    @ApiModelProperty("连接是否可用")
    private final Boolean available;
}
