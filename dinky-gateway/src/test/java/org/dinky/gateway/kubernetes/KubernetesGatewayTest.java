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

import org.dinky.data.enums.JobStatus;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

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

    /** 验证主动轮询沿用 Dinky 状态枚举，避免页面出现无法识别的 Flink 原生状态。 */
    @Test
    public void testQueryJobStatus() throws Exception {
        String jobId = "0123456789abcdef0123456789abcdef";
        ClusterClient<String> clusterClient = mock(ClusterClient.class);
        when(clusterClient.getJobStatus(JobID.fromHexString(jobId)))
                .thenReturn(CompletableFuture.completedFuture(org.apache.flink.api.common.JobStatus.RUNNING));

        KubernetesApplicationGateway gateway = new KubernetesApplicationGateway();
        assertThat(gateway.queryJobStatus(clusterClient, jobId)).isEqualTo(JobStatus.RUNNING);
    }

    /** 验证自动注册标识可还原为 Kubernetes Deployment 名称，保证轮询连接到正确集群。 */
    @Test
    public void testResolveClusterId() {
        String jobId = "0123456789abcdef0123456789abcdef";

        assertThat(KubernetesApplicationGateway.resolveClusterId("demo-job" + jobId, jobId))
                .isEqualTo("demo-job");
        assertThat(KubernetesApplicationGateway.resolveClusterId("demo-job", jobId))
                .isEqualTo("demo-job");
    }

    /** 验证状态确认使用当前 Service ClusterIP，避免同名任务重建后命中旧 DNS 缓存。 */
    @Test
    public void testBuildRestQueryUrl() {
        String webUrl = "http://demo-job-rest.flink-dev:8081";

        assertThat(KubernetesApplicationGateway.buildRestQueryUrl("172.20.10.8", webUrl))
                .isEqualTo("http://172.20.10.8:8081");
        assertThat(KubernetesApplicationGateway.buildRestQueryUrl("None", webUrl))
                .isEqualTo(webUrl);
        assertThat(KubernetesApplicationGateway.buildRestQueryUrl(null, webUrl)).isEqualTo(webUrl);
    }
}
