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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.dinky.context.SpringContextUtils;
import org.dinky.context.TenantContextHolder;
import org.dinky.data.dto.TaskDTO;
import org.dinky.data.model.ClusterInstance;
import org.dinky.data.model.ext.JobInfoDetail;
import org.dinky.data.model.job.JobInstance;
import org.dinky.service.JobInstanceService;
import org.dinky.service.MonitorService;
import org.dinky.service.TaskService;

import org.apache.ibatis.builder.MapperBuilderAssistant;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;

/** 校验监控只处理最新提交，并验证错绑恢复失败不会进入无限自动发现。 */
class FlinkMonitorOwnershipTest {
    private static ApplicationContext previousContext;
    private static GenericApplicationContext context;
    private static final JobInstanceService instanceService = mock(JobInstanceService.class);
    private static final TaskService taskService = mock(TaskService.class);

    @BeforeAll
    static void initializeServices() {
        TableInfoHelper.initTableInfo(
                new MapperBuilderAssistant(new MybatisConfiguration(), "test"), JobInstance.class);
        previousContext = SpringContextUtils.applicationContext;
        context = new GenericApplicationContext();
        context.registerBean("jobInstanceServiceImpl", JobInstanceService.class, () -> instanceService);
        context.registerBean("taskServiceImpl", TaskService.class, () -> taskService);
        context.registerBean("monitorServiceImpl", MonitorService.class, () -> mock(MonitorService.class));
        context.refresh();
        SpringContextUtils.applicationContext = context;
        // 先加载静态依赖，其他监控测试随后恢复自己的 Spring 上下文。
        new FlinkJobTask();
    }

    @AfterAll
    static void restoreServices() {
        SpringContextUtils.applicationContext = previousContext;
        context.close();
    }

    @BeforeEach
    void resetServices() {
        reset(instanceService, taskService);
        TenantContextHolder.clear();
    }

    @AfterEach
    void clearTenant() {
        TenantContextHolder.clear();
    }

    // 模拟定时器提前创建无租户线程，查询必须先绑定实例租户，不能误判为历史监控。
    @Test
    void shouldBindTenantBeforeOwnershipQueryOnUninitializedWorker() {
        assertTenantBeforeOwnershipQuery(null);
    }

    // 同一工作线程轮询不同租户时，不能继承上一任务的租户并查不到当前实例。
    @Test
    void shouldReplacePreviousTenantBeforeOwnershipQuery() {
        assertTenantBeforeOwnershipQuery(2);
    }

    private void assertTenantBeforeOwnershipQuery(Integer previousTenant) {
        FlinkJobTask monitor = monitor(51);
        TenantContextHolder.set(previousTenant);
        when(instanceService.getJobInstanceByTaskId(11))
                .thenAnswer(invocation -> Integer.valueOf(1).equals(TenantContextHolder.get())
                        ? monitor.getJobInfoDetail().getInstance()
                        : null);
        when(taskService.getTaskInfoById(11)).thenReturn(boundTask("SCANNING"));
        when(instanceService.updateIfCurrentJobId(any(), any(), any())).thenReturn(true);
        assertFalse(monitor.dealTask());
        verify(instanceService).discoverJobId(51);
    }

    @Test
    void shouldDiscardHistoricalMonitorEvenIfTaskBindingPointsToIt() {
        FlinkJobTask monitor = monitor(41);
        JobInstance latest = new JobInstance();
        latest.setId(51);
        when(instanceService.getJobInstanceByTaskId(11)).thenReturn(latest);
        assertTrue(monitor.dealTask());
        verify(instanceService, never()).discoverJobId(41);
    }

    @Test
    void shouldNotRecoverStaleBindingAfterManualStop() {
        FlinkJobTask monitor = monitor(51);
        when(instanceService.getJobInstanceByTaskId(11))
                .thenReturn(monitor.getJobInfoDetail().getInstance());
        when(taskService.getTaskInfoById(11)).thenReturn(boundTask("CANCELED"));
        assertTrue(monitor.dealTask());
        verify(instanceService, never()).discoverJobId(51);
    }

    // 停止发生在重扫等待期间时，UNKNOWN/RECONNECTING 不能再等满下一轮才退出。
    @Test
    void shouldStopWaitingRecoveryImmediatelyAfterManualStop() {
        for (String status : new String[] {"UNKNOWN", "RECONNECTING"}) {
            FlinkJobTask monitor = monitor(51);
            monitor.getJobInfoDetail().getInstance().setStatus(status);
            monitor.setLastMonitorScanTime(System.currentTimeMillis());
            when(instanceService.getJobInstanceByTaskId(11))
                    .thenReturn(monitor.getJobInfoDetail().getInstance());
            when(taskService.getTaskInfoById(11)).thenReturn(boundTask("CANCELED"));
            assertTrue(monitor.dealTask());
        }
        verify(instanceService, never()).discoverJobId(51);
    }

    @Test
    void shouldStopAfterBoundedRecoveryAttemptsForActuallyCanceledJob() {
        FlinkJobTask monitor = monitor(51);
        when(instanceService.getJobInstanceByTaskId(11))
                .thenReturn(monitor.getJobInfoDetail().getInstance());
        when(taskService.getTaskInfoById(11)).thenReturn(boundTask("SCANNING"));
        when(instanceService.updateIfCurrentJobId(any(), any(), any())).thenReturn(true);
        when(instanceService.update(any())).thenReturn(true);
        assertFalse(monitor.dealTask());
        // 推进重扫时钟而不真实等待，验证十次延迟重试后退出队列。
        for (int retry = 1; retry <= 10; retry++) {
            monitor.setPreDealTime(0L);
            monitor.setLastMonitorScanTime(System.currentTimeMillis() - 31_000L);
            if (retry < 10) {
                assertFalse(monitor.dealTask());
            } else {
                assertTrue(monitor.dealTask());
            }
        }
        verify(instanceService, times(11)).discoverJobId(51);
    }

    private TaskDTO boundTask(String scanStatus) {
        TaskDTO task = new TaskDTO();
        task.setId(11);
        task.setJobInstanceId(41);
        task.setMonitorScanStatus(scanStatus);
        return task;
    }

    private FlinkJobTask monitor(int id) {
        JobInstance instance = new JobInstance();
        instance.setId(id);
        instance.setTaskId(11);
        instance.setTenantId(1);
        instance.setJid("same-job");
        instance.setStatus("CANCELED");
        ClusterInstance cluster = new ClusterInstance();
        cluster.setType("kubernetes-application");
        JobInfoDetail detail = new JobInfoDetail(id);
        detail.setInstance(instance);
        detail.setClusterInstance(cluster);
        FlinkJobTask monitor = new FlinkJobTask();
        monitor.setJobInfoDetail(detail);
        return monitor;
    }
}
