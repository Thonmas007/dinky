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

package org.dinky.job;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.dinky.controller.JobInstanceController;
import org.dinky.data.dto.JobDataDto;
import org.dinky.data.flink.job.FlinkJobDetailInfo;
import org.dinky.data.model.ext.JobInfoDetail;
import org.dinky.data.model.job.JobInstance;
import org.dinky.service.JobInstanceService;

import org.junit.jupiter.api.Test;

class JobDiscoveryControllerTest {

    /** 历史页面应指向最新实例，不能把所有权拒绝误报成 Flink 不存活。 */
    @Test
    void shouldRejectHistoricalPageBeforeDiscovery() {
        JobInstanceService service = mock(JobInstanceService.class);
        JobInstance requested = new JobInstance();
        requested.setId(41);
        requested.setTaskId(11);
        JobInstance latest = new JobInstance();
        latest.setId(51);
        when(service.getById(41)).thenReturn(requested);
        when(service.getJobInstanceByTaskId(11)).thenReturn(latest);
        assertFalse(new JobInstanceController(service).discoverJobId(41).isSuccess());
        verify(service, never()).discoverJobId(41);
    }

    // REST 发现成功但刷新仍为旧终态时，不得宣称已恢复监控。
    @Test
    void shouldRejectCanceledRefreshAfterDiscovery() {
        assertFalse(discoverWithStatus("CANCELED"));
    }

    @Test
    void shouldRejectReconnectingRefreshAfterDiscovery() {
        assertFalse(discoverWithStatus("RECONNECTING"));
    }

    @Test
    void shouldRejectUnknownRefreshAfterDiscovery() {
        assertFalse(discoverWithStatus("UNKNOWN"));
    }

    // 发现与详情查询间发生生命周期变化时，以详情的最终状态为准。
    @Test
    void shouldRejectStoppingOrFailingLiveJob() {
        assertFalse(discoverWithStatus("CANCELLING"));
        assertFalse(discoverWithStatus("FAILING"));
        assertFalse(discoverWithStatus("SUSPENDED"));
    }

    @Test
    void shouldConfirmLiveRunningRefresh() {
        assertTrue(discoverWithStatus("RUNNING"));
    }

    @Test
    void shouldRejectMismatchedLiveStatus() {
        assertFalse(discoverWithStatus("RUNNING", "CANCELED", "test-job", false));
    }

    @Test
    void shouldRejectMismatchedLiveJobId() {
        assertFalse(discoverWithStatus("RUNNING", "RUNNING", "other-job", false));
    }

    @Test
    void shouldRejectFailedLiveRefresh() {
        assertFalse(discoverWithStatus("RUNNING", "RUNNING", "test-job", true));
    }

    // 独立模拟发现与实时刷新，覆盖接口消息必须以最终监控结果为准的约束。
    private boolean discoverWithStatus(String status) {
        return discoverWithStatus(status, status, "test-job", false);
    }

    // 并发刷新可能保留新实例和旧详情，接口不得把不一致的快照作为恢复成功证据。
    private boolean discoverWithStatus(String status, String liveStatus, String liveJobId, boolean error) {
        JobInstanceService service = mock(JobInstanceService.class);
        JobInstance instance = new JobInstance();
        instance.setId(55);
        instance.setTaskId(12);
        instance.setJid("test-job");
        instance.setStatus(status);
        JobInfoDetail detail = new JobInfoDetail(55);
        detail.setInstance(instance);
        FlinkJobDetailInfo liveJob = new FlinkJobDetailInfo();
        liveJob.setJid(liveJobId);
        liveJob.setState(liveStatus);
        detail.setJobDataDto(JobDataDto.builder().job(liveJob).error(error).build());
        when(service.getById(55)).thenReturn(instance);
        when(service.getJobInstanceByTaskId(12)).thenReturn(instance);
        when(service.discoverJobId(55)).thenReturn(detail);
        when(service.refreshJobInfoDetail(55, 12, true)).thenReturn(detail);
        return new JobInstanceController(service).discoverJobId(55).isSuccess();
    }
}
