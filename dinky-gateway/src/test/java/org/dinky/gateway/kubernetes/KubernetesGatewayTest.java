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

package org.dinky.gateway.kubernetes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;

import java.util.Collections;

import org.junit.Test;

public class KubernetesGatewayTest {

    /** 验证连接探测只读取命名空间资源，不再调用新版 Kubernetes 不兼容的版本接口。 */
    @Test
    public void testVerifyKubernetesAccess() {
        FlinkKubeClient client = mock(FlinkKubeClient.class);
        when(client.getPodsWithLabels(Collections.emptyMap())).thenReturn(Collections.emptyList());

        KubernetesApplicationGateway gateway = new KubernetesApplicationGateway();
        int visiblePodCount = gateway.verifyKubernetesAccess(client);

        assertThat(visiblePodCount).isZero();
        verify(client).getPodsWithLabels(Collections.emptyMap());
    }
}
