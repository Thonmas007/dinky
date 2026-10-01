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

/** 集群查询命令的标准化执行结果，保留退出码供页面区分命令失败。 */
@Getter
@AllArgsConstructor
@ApiModel(value = "KubernetesCommandResultDTO", description = "Kubernetes command result")
public class KubernetesCommandResultDTO {

    @ApiModelProperty("标准输出和错误输出")
    private final String output;

    @ApiModelProperty("进程退出码")
    private final int exitCode;

    @ApiModelProperty("是否因超时被终止")
    private final boolean timedOut;
}
