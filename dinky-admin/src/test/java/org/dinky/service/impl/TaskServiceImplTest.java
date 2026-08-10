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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.dinky.data.dto.TaskDTO;
import org.dinky.data.enums.JobLifeCycle;
import org.dinky.data.model.Task;
import org.dinky.data.model.job.JobInstance;
import org.dinky.service.AlertGroupService;
import org.dinky.service.ClusterConfigurationService;
import org.dinky.service.ClusterInstanceService;
import org.dinky.service.DataBaseService;
import org.dinky.service.FragmentVariableService;
import org.dinky.service.JobInstanceService;
import org.dinky.service.SavepointsService;
import org.dinky.service.TaskVersionService;
import org.dinky.service.UDFTemplateService;
import org.dinky.service.UserService;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.ApplicationContext;

import com.alibaba.druid.pool.DruidDataSource;

class TaskServiceImplTest {

    /** 发布生命周期切换不能触发作业探测，否则历史实例会被重新加入运行监控队列。 */
    @Test
    void shouldNotRefreshJobStatusWhenChangingTaskLifeCycle() throws Exception {
        JobInstanceService jobInstanceService = mock(JobInstanceService.class);
        TaskServiceImpl taskService = spy(new TaskServiceImpl(
                mock(SavepointsService.class),
                mock(ClusterInstanceService.class),
                mock(ClusterConfigurationService.class),
                mock(DataBaseService.class),
                jobInstanceService,
                mock(AlertGroupService.class),
                mock(TaskVersionService.class),
                mock(FragmentVariableService.class),
                mock(UDFTemplateService.class),
                mock(DataSourceProperties.class),
                mock(UserService.class),
                mock(ApplicationContext.class),
                mock(DruidDataSource.class)));

        TaskDTO task = new TaskDTO();
        task.setId(2);
        task.setJobInstanceId(20);
        JobInstance jobInstance = new JobInstance();
        jobInstance.setId(20);
        jobInstance.setTaskId(2);
        jobInstance.setName("task-2");

        doReturn(task).when(taskService).getTaskInfoById(2);
        doReturn(true).when(taskService).saveOrUpdate(any(Task.class));
        when(jobInstanceService.getById(20)).thenReturn(jobInstance);
        when(jobInstanceService.updateById(jobInstance)).thenReturn(true);

        assertTrue(taskService.changeTaskLifeRecyle(2, JobLifeCycle.DEVELOP));
        verify(jobInstanceService).updateById(jobInstance);
        verify(jobInstanceService, never()).refreshJobInfoDetail(any(), any(), anyBoolean());
    }
}
