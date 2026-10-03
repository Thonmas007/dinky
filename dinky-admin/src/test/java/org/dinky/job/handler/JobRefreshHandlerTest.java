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

package org.dinky.job.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.dinky.configure.ApplicationMonitorProperties;
import org.dinky.context.SpringContextUtils;
import org.dinky.data.dto.JobDataDto;
import org.dinky.data.model.SystemConfiguration;
import org.dinky.init.FlinkHistoryServer;
import org.dinky.service.ClusterInstanceService;
import org.dinky.service.HistoryService;
import org.dinky.service.JobHistoryService;
import org.dinky.service.JobInstanceService;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

import com.sun.net.httpserver.HttpServer;

class JobRefreshHandlerTest {
    private static final String JOB_ID = "000000007e89cb440000000000000000";
    private HttpServer server;
    private String state;
    private boolean detailUnavailable;
    private Boolean historyEnabled;
    private Integer historyPort;
    private static ApplicationContext previousContext;
    private static GenericApplicationContext testContext;

    /** 隔离监控器的静态服务依赖，只通过本地 HTTP 验证实际 REST 路由与错误处理。 */
    @BeforeAll
    static void initializeServices() {
        previousContext = SpringContextUtils.applicationContext;
        GenericApplicationContext context = new GenericApplicationContext();
        testContext = context;
        context.registerBean("jobInstanceServiceImpl", JobInstanceService.class, () -> mock(JobInstanceService.class));
        context.registerBean("jobHistoryServiceImpl", JobHistoryService.class, () -> mock(JobHistoryService.class));
        context.registerBean(
                "clusterInstanceServiceImpl", ClusterInstanceService.class, () -> mock(ClusterInstanceService.class));
        context.registerBean("historyServiceImpl", HistoryService.class, () -> mock(HistoryService.class));
        context.registerBean(ApplicationMonitorProperties.class, ApplicationMonitorProperties::new);
        context.refresh();
        SpringContextUtils.applicationContext = context;
    }

    @AfterAll
    static void restoreServices() {
        SpringContextUtils.applicationContext = previousContext;
        testContext.close();
    }

    @BeforeEach
    void startJobManager() throws Exception {
        historyEnabled =
                SystemConfiguration.getInstances().getUseFlinkHistoryServer().getValue();
        historyPort =
                SystemConfiguration.getInstances().getFlinkHistoryServerPort().getValue();
        SystemConfiguration.getInstances().getUseFlinkHistoryServer().setValue(true);
        // 旧实现会错误访问这个不可用的 HistoryServer，测试必须仍然成功读取当前 JobManager。
        SystemConfiguration.getInstances().getFlinkHistoryServerPort().setValue(1);
        FlinkHistoryServer.HISTORY_JOBID_SET.add(JOB_ID);
        state = "RUNNING";
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String response = "{\"errors\":[\"optional endpoint unavailable\"]}";
            if (exchange.getRequestURI().getPath().equals("/jobs/" + JOB_ID) && !detailUnavailable) {
                response = "{\"jid\":\"" + JOB_ID + "\",\"state\":\"" + state
                        + "\",\"start-time\":1791030938054,\"end-time\":-1,\"duration\":1000,"
                        + "\"vertices\":[],\"plan\":{\"nodes\":[]}}";
            }
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void cleanUp() {
        if (server != null) {
            server.stop(0);
        }
        FlinkHistoryServer.HISTORY_JOBID_SET.remove(JOB_ID);
        SystemConfiguration.getInstances().getUseFlinkHistoryServer().setValue(historyEnabled);
        SystemConfiguration.getInstances().getFlinkHistoryServerPort().setValue(historyPort);
    }

    /** 同一 JID 已有历史取消记录时，本次实时状态仍以 JobManager 为准。 */
    @Test
    void shouldReadLiveJobDespiteArchivedJobIdAndOptionalErrors() {
        JobDataDto data = readJob();
        assertFalse(data.isError());
        assertEquals("RUNNING", data.getJob().getState());
        assertEquals(1791030938054L, data.getJob().getStartTime());
    }

    /** 当前详情不可用不能回退到同 JID 的旧终态，应让上层进入重连。 */
    @Test
    void shouldReportUnavailableCurrentJobRatherThanUseArchive() {
        detailUnavailable = true;
        assertTrue(readJob().isError());
    }

    /** 当前 JobManager 明确确认取消时仍保留真实终态，不强制改成运行。 */
    @Test
    void shouldKeepConfirmedCurrentCancellation() {
        state = "CANCELED";
        JobDataDto data = readJob();
        assertFalse(data.isError());
        assertEquals("CANCELED", data.getJob().getState());
    }

    private JobDataDto readJob() {
        return JobRefreshHandler.getJobData(
                56, "127.0.0.1:" + server.getAddress().getPort(), JOB_ID);
    }
}
