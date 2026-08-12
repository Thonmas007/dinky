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

import java.util.Arrays;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    private JsonNode job(String jid, String name, String state, long startTime) throws Exception {
        return OBJECT_MAPPER.readTree(String.format(
                "{\"jid\":\"%s\",\"name\":\"%s\",\"state\":\"%s\",\"start-time\":%d}",
                jid,
                name,
                state,
                startTime));
    }
}
