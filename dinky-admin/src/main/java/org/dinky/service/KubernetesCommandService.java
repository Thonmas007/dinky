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
package org.dinky.service;

import org.dinky.data.dto.KubernetesCommandDTO;
import org.dinky.data.dto.KubernetesClusterOptionDTO;
import org.dinky.data.dto.KubernetesCommandResultDTO;

import java.util.List;

/** 为集群查询页面执行受限的只读 Kubernetes 命令。 */
public interface KubernetesCommandService {

    /** 返回已配置的 Kubernetes 集群摘要，避免前端重复维护 kubeconfig。 */
    List<KubernetesClusterOptionDTO> listConfigurations();

    /** 使用用户选中的 Dinky 集群配置执行受限的只读查询。 */
    KubernetesCommandResultDTO execute(KubernetesCommandDTO command);
}
