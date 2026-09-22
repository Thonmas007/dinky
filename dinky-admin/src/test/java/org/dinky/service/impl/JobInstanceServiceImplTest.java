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

package org.dinky.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.dinky.data.dto.JobDataDto;
import org.dinky.data.model.ClusterInstance;
import org.dinky.data.model.ext.JobInfoDetail;
import org.dinky.data.model.job.History;
import org.dinky.data.model.job.JobInstance;
import org.dinky.data.model.mapping.ClusterInstanceMapping;

import java.util.Arrays;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class JobInstanceServiceImplTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** 驱逐后 overview 可能同时包含历史终态和新运行态，只能选择启动时间最新的活跃同名作业。 */
    @Test
    void shouldFindLatestActiveJobWithSameName() throws Exception {
        JsonNode finished = job("old-finished", "datagen-task-v12", "FINISHED", 100L);
        JsonNode olderRunning = job("older-running", "datagen-task-v12", "RUNNING", 200L);
        JsonNode latestRunning = job("latest-running", "datagen-task-v12", "RUNNING", 300L);
        JsonNode differentName = job("different", "other-task", "RUNNING", 400L);

        Optional<JsonNode> result = JobInstanceServiceImpl.findDiscoverableJob(
                Arrays.asList(finished, olderRunning, latestRunning, differentName), "datagen-task-v12");

        assertTrue(result.isPresent());
        assertEquals("latest-running", result.get().path("jid").asText());
    }

    /** 不允许把失败、取消等终态作业重新关联到仍需恢复的 Dinky 实例。 */
    @Test
    void shouldIgnoreTerminalJobs() throws Exception {
        Optional<JsonNode> result = JobInstanceServiceImpl.findDiscoverableJob(
                Arrays.asList(
                        job("failed", "datagen-task-v12", "FAILED", 200L),
                        job("canceled", "datagen-task-v12", "CANCELED", 300L)),
                "datagen-task-v12");

        assertFalse(result.isPresent());
    }

    /** SQL 显式设置 JobGraph 名称时，Application 集群中的唯一活跃作业仍应能恢复关联。 */
    @Test
    void shouldUseSoleActiveJobWhenJobGraphNameDiffers() throws Exception {
        Optional<JsonNode> result = JobInstanceServiceImpl.findDiscoverableJob(
                Arrays.asList(
                        job("old-finished", "pf-oneid-agent-relation", "FINISHED", 100L),
                        job("new-running", "MysqlToClickHouseOneId", "RUNNING", 200L)),
                "pf-oneid-agent-relation");

        assertTrue(result.isPresent());
        assertEquals("new-running", result.get().path("jid").asText());
    }

    /** 无同名结果且存在多个活跃作业时无法安全判断归属，必须拒绝自动关联。 */
    @Test
    void shouldRejectAmbiguousActiveJobsWithDifferentNames() throws Exception {
        Optional<JsonNode> result = JobInstanceServiceImpl.findDiscoverableJob(
                Arrays.asList(
                        job("first-running", "first-job", "RUNNING", 100L),
                        job("second-running", "second-job", "RUNNING", 200L)),
                "pf-oneid-agent-relation");

        assertFalse(result.isPresent());
    }

    /** 当前集群实例丢失时，历史提交模式仍能证明该实例支持主动发现。 */
    @Test
    void shouldAllowDiscoveryByHistoryTypeWhenClusterInstanceMissing() {
        JobInfoDetail jobInfoDetail = new JobInfoDetail(1);
        History history = new History();
        history.setType("kubernetes-application");
        jobInfoDetail.setHistory(history);

        assertTrue(JobInstanceServiceImpl.isDiscoverableKubernetesApplication(jobInfoDetail));
    }

    /** JobManager 地址优先取当前实例，缺失时回退到历史快照，最后再使用 History 记录。 */
    @Test
    void shouldResolveDiscoverHostFromHistorySnapshot() {
        JobInfoDetail jobInfoDetail = new JobInfoDetail(1);
        ClusterInstance clusterInstance = new ClusterInstance();
        clusterInstance.setJobManagerHost("");
        jobInfoDetail.setClusterInstance(clusterInstance);
        ClusterInstanceMapping clusterSnapshot = new ClusterInstanceMapping();
        clusterSnapshot.setJobManagerHost("attribute-user-id-01-rest.flink-dev:8081");
        jobInfoDetail.setJobDataDto(JobDataDto.builder().cluster(clusterSnapshot).build());
        History history = new History();
        history.setJobManagerAddress("history-address:8081");
        jobInfoDetail.setHistory(history);

        assertEquals(
                "attribute-user-id-01-rest.flink-dev:8081",
                JobInstanceServiceImpl.getDiscoverJobManagerHost(jobInfoDetail));
    }

    /** 注册集群被清理后，历史地址可补成运行时实例，供发现后的强制刷新继续访问 Flink REST。 */
    @Test
    void shouldBuildRuntimeClusterInstanceFromHistoryWhenClusterInstanceMissing() {
        JobInfoDetail jobInfoDetail = new JobInfoDetail(166);
        JobInstance jobInstance = new JobInstance();
        jobInstance.setName("derived-event-realtime-engine");
        jobInstance.setTaskId(31);
        jobInfoDetail.setInstance(jobInstance);
        History history = new History();
        history.setType("kubernetes-application");
        history.setJobName("derived-event-realtime-engine");
        history.setJobManagerAddress("derived-event-realtime-engine-rest.flink-dev:8081");
        history.setClusterConfigurationId(1);
        jobInfoDetail.setHistory(history);

        ClusterInstance clusterInstance = JobInstanceServiceImpl.buildDiscoverClusterInstance(jobInfoDetail);

        assertEquals("derived-event-realtime-engine", clusterInstance.getName());
        assertEquals("kubernetes-application", clusterInstance.getType());
        assertEquals("derived-event-realtime-engine-rest.flink-dev:8081", clusterInstance.getJobManagerHost());
        assertTrue(clusterInstance.isAutoRegisters());
        assertEquals(31, clusterInstance.getTaskId());
        assertEquals(1, clusterInstance.getClusterConfigurationId());
    }

    /** 缩容重建导致 ClusterIP 变化时，使用稳定的作业名和 namespace 定位 REST Service。 */
    @Test
    void shouldBuildKubernetesRestServiceAddress() {
        assertEquals(
                "attribute-user-id-01-rest.flink-dev:8081",
                JobInstanceServiceImpl.buildKubernetesRestServiceAddress("attribute-user-id-01", "flink-dev"));
    }

    /** 缺少作业名或 namespace 时不能猜测 Service，继续沿用未发现结果。 */
    @Test
    void shouldNotBuildKubernetesRestServiceAddressWithoutRequiredName() {
        assertNull(JobInstanceServiceImpl.buildKubernetesRestServiceAddress("", "flink-dev"));
        assertNull(JobInstanceServiceImpl.buildKubernetesRestServiceAddress("attribute-user-id-01", null));
    }

    private JsonNode job(String jid, String name, String state, long startTime) throws Exception {
        return OBJECT_MAPPER.readTree(String.format(
                "{\"jid\":\"%s\",\"name\":\"%s\",\"state\":\"%s\",\"start-time\":%d}",
                jid,
                name,
                state,
                startTime));
    }
}
