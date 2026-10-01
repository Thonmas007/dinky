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
import lombok.Getter;
import lombok.Setter;

/** 集群查询页面提交的只读 kubectl 命令。 */
@Getter
@Setter
@ApiModel(value = "KubernetesCommandDTO", description = "Kubernetes read-only command request")
public class KubernetesCommandDTO {

    @ApiModelProperty(value = "集群配置 ID", required = true, example = "1")
    private Integer clusterConfigurationId;

    @ApiModelProperty(value = "kubectl command", required = true, example = "kubectl get pods -n flink-dev")
    private String command;
}
