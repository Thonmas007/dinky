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

import org.dinky.api.FlinkAPI;
import org.dinky.assertion.Asserts;
import org.dinky.context.TenantContextHolder;
import org.dinky.daemon.pool.FlinkJobThreadPool;
import org.dinky.daemon.task.DaemonTask;
import org.dinky.daemon.task.DaemonTaskConfig;
import org.dinky.data.dto.ClusterConfigurationDTO;
import org.dinky.data.dto.JobDataDto;
import org.dinky.data.enums.GatewayType;
import org.dinky.data.enums.JobStatus;
import org.dinky.data.enums.Status;
import org.dinky.data.enums.TaskMonitorScanStatus;
import org.dinky.data.model.ClusterConfiguration;
import org.dinky.data.model.ClusterInstance;
import org.dinky.data.model.Task;
import org.dinky.data.model.ext.JobInfoDetail;
import org.dinky.data.model.home.JobInstanceCount;
import org.dinky.data.model.home.JobInstanceStatus;
import org.dinky.data.model.home.JobModelOverview;
import org.dinky.data.model.job.History;
import org.dinky.data.model.job.JobHistory;
import org.dinky.data.model.job.JobInstance;
import org.dinky.data.model.mapping.ClusterConfigurationMapping;
import org.dinky.data.model.mapping.ClusterInstanceMapping;
import org.dinky.data.result.ProTableResult;
import org.dinky.data.vo.task.JobInstanceVo;
import org.dinky.explainer.lineage.LineageBuilder;
import org.dinky.explainer.lineage.LineageResult;
import org.dinky.job.FlinkJobTask;
import org.dinky.mapper.JobInstanceMapper;
import org.dinky.mapper.TaskMapper;
import org.dinky.mybatis.service.impl.SuperServiceImpl;
import org.dinky.mybatis.util.ProTableUtil;
import org.dinky.service.ClusterConfigurationService;
import org.dinky.service.ClusterInstanceService;
import org.dinky.service.HistoryService;
import org.dinky.service.JobHistoryService;
import org.dinky.service.JobInstanceService;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import cn.hutool.core.util.StrUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * JobInstanceServiceImpl
 *
 * @since 2022/2/2 13:52
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class JobInstanceServiceImpl extends SuperServiceImpl<JobInstanceMapper, JobInstance>
        implements JobInstanceService {

    private static final Set<JobStatus> DISCOVERABLE_JOB_STATUSES = Collections.unmodifiableSet(EnumSet.of(
            JobStatus.INITIALIZING, JobStatus.CREATED, JobStatus.RUNNING, JobStatus.RESTARTING, JobStatus.RECONCILING));
    private static final String KUBERNETES_NAMESPACE_KEY = "kubernetes.namespace";
    private static final int FLINK_REST_PORT = 8081;
    private static final int FAILED_APPLICATION_CLEANUP_MAX_ATTEMPTS = 3;
    private static final String FAILED_APPLICATION_CLEANUP_PENDING = "PENDING";
    private static final String FAILED_APPLICATION_CLEANUP_RUNNING = "RUNNING";
    private static final String FAILED_APPLICATION_CLEANUP_SUCCEEDED = "SUCCEEDED";
    private static final String FAILED_APPLICATION_CLEANUP_SKIPPED = "SKIPPED";
    private static final String FAILED_APPLICATION_CLEANUP_EXHAUSTED = "EXHAUSTED";

    private final HistoryService historyService;
    private final ClusterInstanceService clusterInstanceService;
    private final ClusterConfigurationService clusterConfigurationService;
    private final JobHistoryService jobHistoryService;
    private final TaskMapper taskMapper;

    /**
     * 作业已明确 FAILED 时登记一小时后的回收计划；计划绑定 JobInstance，防止同名重提任务被旧计划误删。
     */
    @Override
    public void scheduleFailedKubernetesApplicationCleanup(JobInstance jobInstance) {
        if (!JobStatus.FAILED.getValue().equals(jobInstance.getStatus())) {
            return;
        }
        LocalDateTime cleanupAfter = Optional.ofNullable(jobInstance.getFailedCleanupAfter())
                .orElseGet(() -> LocalDateTime.now().plusHours(1));
        // 失败状态可能被多个刷新线程重复感知；只允许首次登记，避免反复延长日志保留期或重置已失败次数。
        TenantContextHolder.ignoreTenant();
        lambdaUpdate()
                .eq(JobInstance::getId, jobInstance.getId())
                .eq(JobInstance::getStatus, JobStatus.FAILED.getValue())
                .isNull(JobInstance::getFailedCleanupStatus)
                .set(JobInstance::getFailedCleanupStatus, FAILED_APPLICATION_CLEANUP_PENDING)
                .set(JobInstance::getFailedCleanupAfter, cleanupAfter)
                .set(JobInstance::getFailedCleanupAttempts, 0)
                .update();
    }

    @Override
    public List<JobInstance> listDueFailedKubernetesApplicationCleanup(LocalDateTime now, int limit) {
        TenantContextHolder.ignoreTenant();
        return lambdaQuery()
                .eq(JobInstance::getStatus, JobStatus.FAILED.getValue())
                .eq(JobInstance::getFailedCleanupStatus, FAILED_APPLICATION_CLEANUP_PENDING)
                .le(JobInstance::getFailedCleanupAfter, now)
                .lt(JobInstance::getFailedCleanupAttempts, FAILED_APPLICATION_CLEANUP_MAX_ATTEMPTS)
                .orderByAsc(JobInstance::getFailedCleanupAfter)
                .last("limit " + limit)
                .list();
    }

    @Override
    public void recoverInterruptedFailedKubernetesApplicationCleanup() {
        TenantContextHolder.ignoreTenant();
        lambdaUpdate()
                .eq(JobInstance::getStatus, JobStatus.FAILED.getValue())
                .eq(JobInstance::getFailedCleanupStatus, FAILED_APPLICATION_CLEANUP_RUNNING)
                .set(JobInstance::getFailedCleanupStatus, FAILED_APPLICATION_CLEANUP_PENDING)
                .update();
    }

    @Override
    public boolean claimFailedKubernetesApplicationCleanup(Integer jobInstanceId) {
        TenantContextHolder.ignoreTenant();
        return lambdaUpdate()
                .eq(JobInstance::getId, jobInstanceId)
                .eq(JobInstance::getStatus, JobStatus.FAILED.getValue())
                .eq(JobInstance::getFailedCleanupStatus, FAILED_APPLICATION_CLEANUP_PENDING)
                .lt(JobInstance::getFailedCleanupAttempts, FAILED_APPLICATION_CLEANUP_MAX_ATTEMPTS)
                .set(JobInstance::getFailedCleanupStatus, FAILED_APPLICATION_CLEANUP_RUNNING)
                .setSql("failed_cleanup_attempts = COALESCE(failed_cleanup_attempts, 0) + 1")
                .update();
    }

    @Override
    public void finishFailedKubernetesApplicationCleanup(Integer jobInstanceId, boolean success, boolean skipped) {
        TenantContextHolder.ignoreTenant();
        JobInstance jobInstance = getByIdWithoutTenant(jobInstanceId);
        boolean exhausted = !success
                && !skipped
                && jobInstance != null
                && Objects.requireNonNullElse(jobInstance.getFailedCleanupAttempts(), 0)
                        >= FAILED_APPLICATION_CLEANUP_MAX_ATTEMPTS;
        String cleanupStatus = skipped
                ? FAILED_APPLICATION_CLEANUP_SKIPPED
                : success
                        ? FAILED_APPLICATION_CLEANUP_SUCCEEDED
                        : exhausted ? FAILED_APPLICATION_CLEANUP_EXHAUSTED : FAILED_APPLICATION_CLEANUP_PENDING;
        lambdaUpdate()
                .eq(JobInstance::getId, jobInstanceId)
                .eq(JobInstance::getFailedCleanupStatus, FAILED_APPLICATION_CLEANUP_RUNNING)
                .set(JobInstance::getFailedCleanupStatus, cleanupStatus)
                .update();
        if (exhausted) {
            log.error(
                    "Failed Kubernetes Application cleanup is exhausted after {} attempts, job instance {} requires manual handling",
                    FAILED_APPLICATION_CLEANUP_MAX_ATTEMPTS,
                    jobInstanceId);
        }
    }

    @Override
    public JobInstance getByIdWithoutTenant(Integer id) {
        return baseMapper.getByIdWithoutTenant(id);
    }

    @Override
    public JobInstanceStatus getStatusCount() {
        List<JobInstanceCount> jobInstanceCounts;
        jobInstanceCounts = baseMapper.countStatus();
        JobModelOverview modelOverview = baseMapper.getJobStreamingOrBatchModelOverview();
        JobInstanceStatus jobInstanceStatus = new JobInstanceStatus();
        jobInstanceStatus.setModelOverview(modelOverview);
        int total = 0;
        for (JobInstanceCount item : jobInstanceCounts) {
            Integer counts = Asserts.isNull(item.getCounts()) ? 0 : item.getCounts();
            total += counts;
            switch (JobStatus.get(item.getStatus())) {
                case INITIALIZING:
                    jobInstanceStatus.setInitializing(counts);
                    break;
                case RUNNING:
                    jobInstanceStatus.setRunning(counts);
                    break;
                case FINISHED:
                    jobInstanceStatus.setFinished(counts);
                    break;
                case FAILED:
                case FAILING:
                    jobInstanceStatus.setFailed(counts);
                    break;
                case CANCELED:
                    jobInstanceStatus.setCanceled(counts);
                    break;
                case RESTARTING:
                    jobInstanceStatus.setRestarting(counts);
                    break;
                case CREATED:
                    jobInstanceStatus.setCreated(counts);
                    break;
                case CANCELLING:
                    jobInstanceStatus.setCancelling(counts);
                    break;
                case SUSPENDED:
                    jobInstanceStatus.setSuspended(counts);
                    break;
                case RECONCILING:
                    jobInstanceStatus.setReconciling(counts);
                    break;
                case UNKNOWN:
                    jobInstanceStatus.setUnknown(counts);
                    break;
                default:
            }
        }
        jobInstanceStatus.setAll(total);
        return jobInstanceStatus;
    }

    @Override
    public List<JobInstance> listJobInstanceActive() {
        return baseMapper.listJobInstanceActive();
    }

    @Override
    public JobInfoDetail getJobInfoDetail(Integer id) {
        if (Asserts.isNull(TenantContextHolder.get())) {
            initTenantByJobInstanceId(id);
        }
        return getJobInfoDetailInfo(getById(id));
    }

    @Override
    public JobInfoDetail getJobInfoDetailInfo(JobInstance jobInstance) {
        Asserts.checkNull(jobInstance, Status.JOB_INSTANCE_NOT_EXIST.getMessage());

        JobInfoDetail jobInfoDetail = new JobInfoDetail(jobInstance.getId());

        jobInfoDetail.setInstance(jobInstance);

        ClusterInstance clusterInstance = clusterInstanceService.getById(jobInstance.getClusterId());
        jobInfoDetail.setClusterInstance(clusterInstance);

        History history = historyService.getById(jobInstance.getHistoryId());
        if (history != null) {
            history.setConfigJson(history.getConfigJson());
            jobInfoDetail.setHistory(history);
            if (Asserts.isNotNull(history.getClusterConfigurationId())) {
                ClusterConfiguration clusterConfig =
                        clusterConfigurationService.getClusterConfigById(history.getClusterConfigurationId());
                if (clusterConfig != null) {
                    jobInfoDetail.setClusterConfiguration(ClusterConfigurationDTO.fromBean(clusterConfig));
                }
            }
        }

        JobDataDto jobDataDto = jobHistoryService.getJobHistoryDto(jobInstance.getId());
        if (jobDataDto == null) {
            JobHistory jobHistory = JobHistory.builder()
                    .id(jobInstance.getId())
                    .clusterJson(ClusterInstanceMapping.getClusterInstanceMapping(clusterInstance))
                    .clusterConfigurationJson(
                            Asserts.isNotNull(jobInfoDetail.getClusterConfiguration())
                                    ? ClusterConfigurationMapping.getClusterConfigurationMapping(jobInfoDetail
                                            .getClusterConfiguration()
                                            .toBean())
                                    : null)
                    .build();
            jobHistoryService.save(jobHistory);
            jobDataDto = JobDataDto.fromJobHistory(jobHistory);
        }
        jobInfoDetail.setJobDataDto(jobDataDto);
        completeMissingKubernetesApplicationClusterInstance(jobInfoDetail);

        return jobInfoDetail;
    }

    @Override
    public JobInfoDetail refreshJobInfoDetail(Integer jobInstanceId, Integer taskId, boolean isForce) {
        DaemonTaskConfig daemonTaskConfig = DaemonTaskConfig.build(FlinkJobTask.TYPE, jobInstanceId, taskId);
        DaemonTask daemonTask = FlinkJobThreadPool.getInstance().getByTaskConfig(daemonTaskConfig);

        if (daemonTask != null && !isForce) {
            return ((FlinkJobTask) daemonTask).getJobInfoDetail();
        } else if (isForce) {
            FlinkJobThreadPool.getInstance().removeByTaskConfig(daemonTaskConfig);
            daemonTask = DaemonTask.build(daemonTaskConfig);
            daemonTask.dealTask();
            JobInfoDetail jobInfoDetail = ((FlinkJobTask) daemonTask).getJobInfoDetail();
            if (!JobStatus.isDone(jobInfoDetail.getInstance().getStatus())) {
                FlinkJobThreadPool.getInstance().execute(daemonTask);
            }
            return jobInfoDetail;
        } else {
            return getJobInfoDetail(jobInstanceId);
        }
    }

    /**
     * 监控刷新使用旧 JID 作为乐观并发条件；主动发现替换 JID 后，迟到的旧结果必须被丢弃。
     */
    @Override
    public boolean updateIfCurrentJobId(JobInstance jobInstance, String expectedJobId) {
        return updateIfCurrentJobId(jobInstance, expectedJobId, null);
    }

    /** 同一作业也使用刷新版本约束写入，避免迟到请求覆盖主动恢复结果。 */
    @Override
    public boolean updateIfCurrentJobId(
            JobInstance jobInstance, String expectedJobId, LocalDateTime expectedUpdateTime) {
        if (jobInstance == null || jobInstance.getId() == null) {
            return false;
        }
        LambdaUpdateWrapper<JobInstance> updateWrapper =
                new LambdaUpdateWrapper<JobInstance>().eq(JobInstance::getId, jobInstance.getId());
        if (expectedUpdateTime != null) {
            updateWrapper.eq(JobInstance::getUpdateTime, expectedUpdateTime);
        }
        updateWrapper.and(wrapper -> {
            if (StrUtil.isBlank(expectedJobId)) {
                wrapper.isNull(JobInstance::getJid).or().eq(JobInstance::getJid, "");
            } else {
                wrapper.eq(JobInstance::getJid, expectedJobId);
            }
        });
        return update(jobInstance, updateWrapper);
    }

    /**
     * 节点驱逐可能让同一个 Kubernetes Application 以新 JID 恢复；这里从 overview 中选择最新活跃作业，
     * 并以条件更新保证后台扫描和人工点击并发执行时不会互相覆盖已经恢复的关联。
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public JobInfoDetail discoverJobId(Integer jobInstanceId) {
        JobInfoDetail jobInfoDetail = getJobInfoDetail(jobInstanceId);
        JobInstance jobInstance = jobInfoDetail.getInstance();
        // 旧版本重发现可能把任务绑定抢回历史实例；提交序号才是当前实例的依据，不能用损坏的绑定拒绝修复。
        Task currentTask = taskMapper.selectById(jobInstance.getTaskId());
        JobInstance latestSubmission = getJobInstanceByTaskId(jobInstance.getTaskId());
        if (currentTask == null
                || latestSubmission == null
                || !Objects.equals(latestSubmission.getId(), jobInstanceId)) {
            log.warn("Skip Job ID discovery for superseded job instance {}", jobInstanceId);
            return null;
        }
        if (!isDiscoverableKubernetesApplication(jobInfoDetail)) {
            log.warn("Job ID discovery only supports Kubernetes Application, job instance {}", jobInstanceId);
            return null;
        }

        String jobManagerHost = getDiscoverJobManagerHost(jobInfoDetail);
        Optional<JsonNode> activeJob = Optional.empty();
        if (StrUtil.isNotBlank(jobManagerHost)) {
            activeJob = queryDiscoverableJob(jobManagerHost, jobInstance, jobInstanceId);
        } else {
            log.warn("JobManager REST address is empty, cannot discover Job ID for instance {}", jobInstanceId);
        }

        if (!activeJob.isPresent()) {
            String serviceAddress =
                    buildKubernetesRestServiceAddress(jobInstance.getName(), getKubernetesNamespace(jobInfoDetail));
            if (StrUtil.isNotBlank(serviceAddress) && !StrUtil.equals(jobManagerHost, serviceAddress)) {
                // dev 缩容重建可能改变 Service ClusterIP，旧地址失效时按稳定的作业名和 namespace 再查一次。
                log.info(
                        "Retry Flink job discovery for instance {} through Kubernetes service {}",
                        jobInstanceId,
                        serviceAddress);
                activeJob = queryDiscoverableJob(serviceAddress, jobInstance, jobInstanceId);
                if (activeJob.isPresent()) {
                    refreshJobManagerAddress(jobInfoDetail, serviceAddress);
                }
            }
        }
        if (!activeJob.isPresent()) {
            log.warn(
                    "No uniquely discoverable active Flink job was found for expected name {}, job instance {}",
                    jobInstance.getName(),
                    jobInstanceId);
            return null;
        }

        JsonNode flinkJob = activeJob.get();
        String oldJobId = jobInstance.getJid();
        String newJobId = flinkJob.path("jid").asText();
        String newStatus = flinkJob.path("state").asText();

        LambdaUpdateWrapper<JobInstance> updateWrapper = new LambdaUpdateWrapper<JobInstance>()
                .eq(JobInstance::getId, jobInstanceId)
                .set(JobInstance::getJid, newJobId)
                .set(JobInstance::getStatus, newStatus)
                .set(JobInstance::getUpdateTime, LocalDateTime.now())
                .set(JobInstance::getFinishTime, null)
                .set(JobInstance::getError, null);
        // 并发发现只允许旧 JID 或同一个新 JID 写入，避免迟到的扫描覆盖另一轮已经建立的新关联。
        updateWrapper.and(wrapper -> {
            if (StrUtil.isBlank(oldJobId)) {
                wrapper.isNull(JobInstance::getJid)
                        .or()
                        .eq(JobInstance::getJid, "")
                        .or()
                        .eq(JobInstance::getJid, newJobId);
            } else {
                wrapper.eq(JobInstance::getJid, oldJobId).or().eq(JobInstance::getJid, newJobId);
            }
        });
        int updatedRows = baseMapper.update(null, updateWrapper);
        if (updatedRows == 0) {
            JobInstance latestInstance = getById(jobInstanceId);
            if (latestInstance == null || !StrUtil.equals(newJobId, latestInstance.getJid())) {
                log.warn("Job ID discovery result {} was superseded for job instance {}", newJobId, jobInstanceId);
                return null;
            }
        }

        // REST 查询期间的新提交或人工停止必须胜出；数据库原子校验最新提交和旧绑定后才允许修复。
        if (taskMapper.recoverLatestJobInstance(
                        jobInstance.getTaskId(),
                        jobInstanceId,
                        currentTask.getJobInstanceId(),
                        currentTask.getMonitorScanStatus(),
                        TaskMonitorScanStatus.SUCCESS.getValue())
                == 0) {
            throw new IllegalStateException("Job instance was superseded during discovery");
        }
        log.info(
                "Discovered and relinked Flink Job ID for instance {}: {} -> {}, status {}",
                jobInstanceId,
                oldJobId,
                newJobId,
                newStatus);
        return getJobInfoDetail(jobInstanceId);
    }

    /** 查询指定 REST 地址并选择可安全关联的活跃作业，网络异常时保留旧关联供后续重试。 */
    private Optional<JsonNode> queryDiscoverableJob(
            String jobManagerHost, JobInstance jobInstance, Integer jobInstanceId) {
        try {
            List<JsonNode> flinkJobs = FlinkAPI.build(jobManagerHost).listJobs();
            return findDiscoverableJob(flinkJobs, jobInstance.getName());
        } catch (Exception e) {
            log.warn(
                    "Query Flink job overview from {} failed for instance {}: {}",
                    jobManagerHost,
                    jobInstanceId,
                    e.toString());
            return Optional.empty();
        }
    }

    /** 失败页可能丢失当前集群实例，只要历史提交模式属于 K8s Application，仍允许按作业名主动恢复 JID。 */
    static boolean isDiscoverableKubernetesApplication(JobInfoDetail jobInfoDetail) {
        if (jobInfoDetail == null) {
            return false;
        }
        return Stream.of(
                        Optional.ofNullable(jobInfoDetail.getClusterInstance()).map(ClusterInstance::getType),
                        Optional.ofNullable(jobInfoDetail.getHistory()).map(History::getType),
                        Optional.ofNullable(jobInfoDetail.getJobDataDto())
                                .map(JobDataDto::getCluster)
                                .map(ClusterInstanceMapping::getType),
                        Optional.ofNullable(jobInfoDetail.getJobDataDto())
                                .map(JobDataDto::getClusterConfiguration)
                                .map(ClusterConfigurationMapping::getType),
                        Optional.ofNullable(jobInfoDetail.getClusterConfiguration())
                                .map(ClusterConfigurationDTO::getType))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .anyMatch(type -> GatewayType.get(type).isKubernetesApplicationMode());
    }

    /** 优先使用当前集群地址，集群实例被清理时再回退到历史快照和提交历史中的 REST 地址。 */
    static String getDiscoverJobManagerHost(JobInfoDetail jobInfoDetail) {
        String currentHost = Optional.ofNullable(jobInfoDetail)
                .map(JobInfoDetail::getClusterInstance)
                .map(ClusterInstance::getJobManagerHost)
                .orElse(null);
        if (StrUtil.isNotBlank(currentHost)) {
            return currentHost;
        }
        String historySnapshotHost = Optional.ofNullable(jobInfoDetail)
                .map(JobInfoDetail::getJobDataDto)
                .map(JobDataDto::getCluster)
                .map(ClusterInstanceMapping::getJobManagerHost)
                .orElse(null);
        if (StrUtil.isNotBlank(historySnapshotHost)) {
            return historySnapshotHost;
        }
        return Optional.ofNullable(jobInfoDetail)
                .map(JobInfoDetail::getHistory)
                .map(History::getJobManagerAddress)
                .orElse(null);
    }

    /** clusterId 指向的注册记录丢失时，从历史快照补一个运行时实例，避免强制刷新立刻把恢复结果写回 UNKNOWN。 */
    private static void completeMissingKubernetesApplicationClusterInstance(JobInfoDetail jobInfoDetail) {
        if (jobInfoDetail == null || jobInfoDetail.getClusterInstance() != null) {
            return;
        }
        Optional.ofNullable(buildDiscoverClusterInstance(jobInfoDetail)).ifPresent(jobInfoDetail::setClusterInstance);
    }

    /** 该实例只用于当前详情刷新和 Flink REST 代理，不入库，防止恢复旧作业时污染注册中心。 */
    static ClusterInstance buildDiscoverClusterInstance(JobInfoDetail jobInfoDetail) {
        if (!isDiscoverableKubernetesApplication(jobInfoDetail)) {
            return null;
        }

        String jobName = getDiscoverJobName(jobInfoDetail);
        String type = getDiscoverClusterType(jobInfoDetail);
        String jobManagerHost = getDiscoverJobManagerHost(jobInfoDetail);
        if (StrUtil.isBlank(jobManagerHost)) {
            jobManagerHost = buildKubernetesRestServiceAddress(jobName, getKubernetesNamespace(jobInfoDetail));
        }
        if (StrUtil.hasBlank(jobName, type, jobManagerHost)) {
            return null;
        }

        ClusterInstance clusterInstance = new ClusterInstance();
        clusterInstance.setName(jobName);
        clusterInstance.setAlias(jobName);
        clusterInstance.setType(type);
        clusterInstance.setHosts(jobManagerHost);
        clusterInstance.setJobManagerHost(jobManagerHost);
        clusterInstance.setEnabled(true);
        clusterInstance.setStatus(1);
        clusterInstance.setAutoRegisters(true);
        clusterInstance.setTaskId(getDiscoverTaskId(jobInfoDetail));
        clusterInstance.setClusterConfigurationId(getDiscoverClusterConfigurationId(jobInfoDetail));
        return clusterInstance;
    }

    private static String getDiscoverClusterType(JobInfoDetail jobInfoDetail) {
        return Stream.of(
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getClusterInstance)
                                .map(ClusterInstance::getType),
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getHistory)
                                .map(History::getType),
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getJobDataDto)
                                .map(JobDataDto::getCluster)
                                .map(ClusterInstanceMapping::getType),
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getJobDataDto)
                                .map(JobDataDto::getClusterConfiguration)
                                .map(ClusterConfigurationMapping::getType),
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getClusterConfiguration)
                                .map(ClusterConfigurationDTO::getType))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(GatewayType::get)
                .filter(GatewayType::isKubernetesApplicationMode)
                .map(GatewayType::getLongValue)
                .findFirst()
                .orElse(null);
    }

    private static String getDiscoverJobName(JobInfoDetail jobInfoDetail) {
        return Stream.of(
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getHistory)
                                .map(History::getJobName),
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getInstance)
                                .map(JobInstance::getName),
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getJobDataDto)
                                .map(JobDataDto::getCluster)
                                .map(ClusterInstanceMapping::getName))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(StrUtil::isNotBlank)
                .findFirst()
                .orElse(null);
    }

    private static Integer getDiscoverTaskId(JobInfoDetail jobInfoDetail) {
        return Stream.of(
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getInstance)
                                .map(JobInstance::getTaskId),
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getHistory)
                                .map(History::getTaskId),
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getJobDataDto)
                                .map(JobDataDto::getCluster)
                                .map(ClusterInstanceMapping::getTaskId))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElse(null);
    }

    private static Integer getDiscoverClusterConfigurationId(JobInfoDetail jobInfoDetail) {
        return Stream.of(
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getHistory)
                                .map(History::getClusterConfigurationId),
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getJobDataDto)
                                .map(JobDataDto::getCluster)
                                .map(ClusterInstanceMapping::getClusterConfigurationId),
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getJobDataDto)
                                .map(JobDataDto::getClusterConfiguration)
                                .map(ClusterConfigurationMapping::getId),
                        Optional.ofNullable(jobInfoDetail)
                                .map(JobInfoDetail::getClusterConfiguration)
                                .map(ClusterConfigurationDTO::getId))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElse(null);
    }

    /** 历史提交配置保留了作业实际 namespace，避免集群配置后续调整影响旧实例恢复。 */
    private static String getKubernetesNamespace(JobInfoDetail jobInfoDetail) {
        String namespace = Optional.ofNullable(jobInfoDetail.getJobDataDto())
                .map(JobDataDto::getClusterConfiguration)
                .map(ClusterConfigurationMapping::getConfigJson)
                .map(config -> config.getKubernetesConfig())
                .map(config -> config.getConfiguration())
                .map(config -> config.get(KUBERNETES_NAMESPACE_KEY))
                .orElse(null);
        if (StrUtil.isNotBlank(namespace)) {
            return namespace;
        }
        return Optional.ofNullable(jobInfoDetail.getClusterConfiguration())
                .map(ClusterConfigurationDTO::getConfig)
                .map(config -> config.getKubernetesConfig())
                .map(config -> config.getConfiguration())
                .map(config -> config.get(KUBERNETES_NAMESPACE_KEY))
                .orElse(null);
    }

    /** Kubernetes Application 的 REST Service 名称稳定，可在 ClusterIP 变化后作为一次性兜底地址。 */
    static String buildKubernetesRestServiceAddress(String jobName, String namespace) {
        if (StrUtil.hasBlank(jobName, namespace)) {
            return null;
        }
        return StrUtil.format("{}-rest.{}:{}", jobName, namespace, FLINK_REST_PORT);
    }

    /** 兜底发现成功后刷新持久化地址，确保监控、日志和 WebUI 不再访问旧 ClusterIP。 */
    private void refreshJobManagerAddress(JobInfoDetail jobInfoDetail, String jobManagerHost) {
        ClusterInstance clusterInstance = jobInfoDetail.getClusterInstance();
        if (clusterInstance != null && clusterInstance.getId() != null) {
            clusterInstance.setHosts(jobManagerHost);
            clusterInstance.setJobManagerHost(jobManagerHost);
            clusterInstanceService.updateById(clusterInstance);
        }

        History history = jobInfoDetail.getHistory();
        if (history != null) {
            history.setJobManagerAddress(jobManagerHost);
            historyService.updateById(history);
        }
    }

    /**
     * SQL 可通过 pipeline.name 等配置覆盖 Flink JobGraph 名称，使其与 Dinky 实例名不同。
     * Kubernetes Application 集群通常只承载一个活跃作业，因此无同名结果时仅允许唯一活跃作业兜底；
     * 若存在多个候选则拒绝猜测，避免把实例关联到错误作业。
     */
    static Optional<JsonNode> findDiscoverableJob(List<JsonNode> jobs, String jobName) {
        if (jobs == null || StrUtil.isBlank(jobName)) {
            return Optional.empty();
        }
        List<JsonNode> activeJobs = jobs.stream()
                .filter(Objects::nonNull)
                .filter(job -> DISCOVERABLE_JOB_STATUSES.contains(
                        JobStatus.get(job.path("state").asText())))
                .filter(job -> StrUtil.isNotBlank(job.path("jid").asText()))
                .collect(Collectors.toList());
        Optional<JsonNode> sameNameJob = activeJobs.stream()
                .filter(job -> StrUtil.equals(jobName, job.path("name").asText()))
                .max(Comparator.comparingLong(
                        (JsonNode job) -> job.path("start-time").asLong(0L)));
        if (sameNameJob.isPresent()) {
            return sameNameJob;
        }
        return activeJobs.size() == 1 ? Optional.of(activeJobs.get(0)) : Optional.empty();
    }

    @Override
    public boolean hookJobDone(String jobId, Integer taskId) {
        LambdaQueryWrapper<JobInstance> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper
                .eq(JobInstance::getJid, jobId)
                .eq(JobInstance::getTaskId, taskId)
                .orderByDesc(JobInstance::getCreateTime)
                .last("limit 1");
        JobInstance instance = baseMapper.selectOne(queryWrapper);
        if (instance == null) {
            // Not having a corresponding jobinstance means that this may not have succeeded in running,
            // returning true to prevent retry.
            return true;
        }

        DaemonTaskConfig config = DaemonTaskConfig.build(FlinkJobTask.TYPE, instance.getId(), instance.getTaskId());
        // 结束回调不抢占已有监控；同 JID 重用或回调迟到时仍由当前 JM 判定状态。
        if (FlinkJobThreadPool.getInstance().getByTaskConfig(config) != null) {
            return true;
        }
        DaemonTask daemonTask = DaemonTask.build(config);

        boolean isDone = daemonTask.dealTask();
        // If the task is not completed, it is re-queued
        if (!isDone) {
            FlinkJobThreadPool.getInstance().execute(daemonTask);
        }
        return isDone;
    }

    @Override
    public boolean hookJobDoneByHistory(String jobId) {
        LambdaQueryWrapper<JobInstance> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper
                .eq(JobInstance::getJid, jobId)
                .orderByDesc(JobInstance::getCreateTime)
                .last("limit 1");
        JobInstance instance = baseMapper.selectOne(queryWrapper);

        if (instance == null
                || !StrUtil.equalsAny(
                        instance.getStatus(), JobStatus.RECONNECTING.getValue(), JobStatus.UNKNOWN.getValue())) {
            // Not having a corresponding jobinstance means that this may not have succeeded in running,
            // returning true to prevent retry.
            return true;
        }

        DaemonTaskConfig config = DaemonTaskConfig.build(FlinkJobTask.TYPE, instance.getId(), instance.getTaskId());
        // 归档通知可能属于同 JID 的旧运行，不能移除当前实时监控；已有监控自行核对当前 JM。
        if (FlinkJobThreadPool.getInstance().getByTaskConfig(config) != null) {
            return true;
        }
        DaemonTask daemonTask = DaemonTask.build(config);
        boolean isDone = daemonTask.dealTask();
        if (!isDone) {
            FlinkJobThreadPool.getInstance().execute(daemonTask);
        }
        return isDone;
    }

    @Override
    public void refreshJobByTaskIds(Integer... taskIds) {
        for (Integer taskId : taskIds) {
            JobInstance instance = getJobInstanceByTaskId(taskId);
            DaemonTaskConfig daemonTaskConfig =
                    DaemonTaskConfig.build(FlinkJobTask.TYPE, instance.getId(), instance.getTaskId());
            FlinkJobThreadPool.getInstance().removeByTaskConfig(daemonTaskConfig);
            FlinkJobThreadPool.getInstance().execute(DaemonTask.build(daemonTaskConfig));
            refreshJobInfoDetail(instance.getId(), instance.getTaskId(), false);
        }
    }

    @Override
    public LineageResult getLineage(Integer id) {
        History history = getJobInfoDetail(id).getHistory();
        return LineageBuilder.getColumnLineageByLogicalPlan(history.getStatement(), history.getConfigJson());
    }

    @Override
    public JobInstance getJobInstanceByTaskId(Integer id) {
        return baseMapper.getJobInstanceByTaskId(id);
    }

    /** HTTP 补偿和 WebSocket 复用同一状态来源，避免队列保留重扫任务时误亮火苗。 */
    @Override
    public Set<Integer> getRunningTaskIds() {
        return new java.util.HashSet<>(baseMapper.listRunningTaskIds());
    }

    @Override
    public ProTableResult<JobInstanceVo> listJobInstances(JsonNode para) {
        int current = para.has("current") ? para.get("current").asInt() : 1;
        int pageSize = para.has("pageSize") ? para.get("pageSize").asInt() : 10;
        QueryWrapper<JobInstanceVo> queryWrapper = new QueryWrapper<>();
        ProTableUtil.autoQueryDefalut(para, queryWrapper);
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> param = mapper.convertValue(para, Map.class);
        Page<JobInstanceVo> page = new Page<>(current, pageSize);
        List<JobInstanceVo> list = baseMapper.selectForProTable(page, queryWrapper, param);
        return ProTableResult.<JobInstanceVo>builder()
                .success(true)
                .data(list)
                .total(page.getTotal())
                .current(current)
                .pageSize(pageSize)
                .build();
    }

    @Override
    public void initTenantByJobInstanceId(Integer id) {
        Integer tenantId = baseMapper.getTenantByJobInstanceId(id);
        Asserts.checkNull(tenantId, Status.JOB_INSTANCE_NOT_EXIST.getMessage());
        TenantContextHolder.set(tenantId);
    }
}
