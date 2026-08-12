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

import static org.dinky.gateway.kubernetes.utils.DinkyKubernetsConstants.DINKY_K8S_INGRESS_DOMAIN_KEY;
import static org.dinky.gateway.kubernetes.utils.DinkyKubernetsConstants.DINKY_K8S_INGRESS_ENABLED_KEY;

import org.dinky.assertion.Asserts;
import org.dinky.context.FlinkUdfPathContextHolder;
import org.dinky.data.enums.GatewayType;
import org.dinky.data.model.SystemConfiguration;
import org.dinky.executor.ClusterDescriptorAdapterImpl;
import org.dinky.gateway.config.AppConfig;
import org.dinky.gateway.exception.GatewayException;
import org.dinky.gateway.kubernetes.ingress.DinkyKubernetesIngress;
import org.dinky.gateway.kubernetes.utils.IgnoreNullRepresenter;
import org.dinky.gateway.kubernetes.utils.K8sClientHelper;
import org.dinky.gateway.model.ingress.JobDetails;
import org.dinky.gateway.model.ingress.JobOverviewInfo;
import org.dinky.gateway.result.GatewayResult;
import org.dinky.gateway.result.KubernetesResult;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.utils.Constants;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;

import org.yaml.snakeyaml.Yaml;

import com.alibaba.fastjson2.JSONObject;
import com.google.common.util.concurrent.Striped;

import cn.hutool.core.date.SystemClock;
import cn.hutool.core.text.StrFormatter;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpStatus;
import cn.hutool.http.HttpUtil;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import lombok.extern.slf4j.Slf4j;

/**
 * KubernetesApplicationGateway
 */
@Slf4j
public class KubernetesApplicationGateway extends KubernetesGateway {

    private static final int EXISTING_CLUSTER_DELETE_TIMEOUT_SECONDS = 60;
    private static final Striped<Lock> SUBMIT_LOCKS = Striped.lazyWeakLock(128);
    /** 固定使用 Kubernetes 当前标准的 apps/v1 Deployment，避免旧客户端回退到 extensions API。 */
    private static final ResourceDefinitionContext APPS_V1_DEPLOYMENT = new ResourceDefinitionContext.Builder()
            .withGroup("apps")
            .withVersion("v1")
            .withPlural("deployments")
            .withKind("Deployment")
            .withNamespaced(true)
            .build();

    /**
     * @return The type of the Kubernetes gateway, which is GatewayType.KUBERNETES_APPLICATION.
     */
    @Override
    public GatewayType getType() {
        return GatewayType.KUBERNETES_APPLICATION;
    }

    /** 从自动注册集群标识中移除 Job ID，恢复 Kubernetes Deployment 的真实名称。 */
    public static String resolveClusterId(String registeredClusterId, String jobId) {
        if (Asserts.isAllNotNullString(registeredClusterId, jobId) && registeredClusterId.endsWith(jobId)) {
            return registeredClusterId.substring(0, registeredClusterId.length() - jobId.length());
        }
        return registeredClusterId;
    }

    /** 通过 Kubernetes 集群描述器重新连接 Application 集群，供 Dinky 服务端主动补查作业状态。 */
    @Override
    public org.dinky.data.enums.JobStatus getJobStatusById(String id) {
        initConfig();
        addConfigParas(
                KubernetesConfigOptions.CLUSTER_ID, config.getClusterConfig().getAppId());
        KubernetesClusterClientFactory clusterClientFactory = new KubernetesClusterClientFactory();
        String clusterId = clusterClientFactory.getClusterId(configuration);
        if (Asserts.isNullString(clusterId)) {
            throw new GatewayException("No Kubernetes cluster id was specified for active polling.");
        }

        try (KubernetesClusterDescriptor clusterDescriptor =
                        clusterClientFactory.createClusterDescriptor(configuration);
                ClusterClient<String> clusterClient =
                        clusterDescriptor.retrieve(clusterId).getClusterClient()) {
            return queryJobStatus(clusterClient, id);
        } catch (Exception e) {
            throw new GatewayException("Active polling Kubernetes application status failed.", e);
        } finally {
            close();
        }
    }

    /** 将 Flink 原生状态转换为 Dinky 状态，避免主动轮询链路产生另一套状态语义。 */
    protected org.dinky.data.enums.JobStatus queryJobStatus(ClusterClient<String> clusterClient, String jobId)
            throws Exception {
        JobStatus status =
                clusterClient.getJobStatus(JobID.fromHexString(jobId)).get(15, TimeUnit.SECONDS);
        return org.dinky.data.enums.JobStatus.get(status.name());
    }

    /**
     * Submits a jar file to the Kubernetes gateway.
     *
     * @throws RuntimeException if an error occurs during submission.
     */
    @Override
    public GatewayResult submitJar(FlinkUdfPathContextHolder udfPathContextHolder) {
        init();
        String namespace = configuration.getString(KubernetesConfigOptions.NAMESPACE);
        String clusterId = configuration.getString(KubernetesConfigOptions.CLUSTER_ID);
        // 同一任务的并发提交必须串行化，否则后到请求可能删除前一个请求刚创建的集群。
        Lock submitLock = SUBMIT_LOCKS.get(namespace + "/" + clusterId);
        submitLock.lock();
        try (KubernetesClient kubernetesClient = getK8sClientHelper().getKubernetesClient()) {
            tryCleanupExistingApplicationBeforeSubmit(kubernetesClient, namespace, clusterId);
            logger.info("Start submit k8s application.");

            ClusterClientProvider<String> clusterClient =
                    deployApplication(getK8sClientHelper().getClient());

            Deployment deployment = getK8sClientHelper().createDinkyResource();

            KubernetesResult kubernetesResult;

            String ingressDomain = checkUseIngress();
            // if ingress is enabled and ingress domain is not empty, create an ingress service
            if (StringUtils.isNotEmpty(ingressDomain)) {
                K8sClientHelper k8sClientHelper = getK8sClientHelper();
                long ingressStart = SystemClock.now();
                DinkyKubernetesIngress ingress = new DinkyKubernetesIngress(k8sClientHelper);
                ingress.configureIngress(
                        k8sClientHelper.getConfiguration().getString(KubernetesConfigOptions.CLUSTER_ID),
                        ingressDomain,
                        k8sClientHelper.getConfiguration().getString(KubernetesConfigOptions.NAMESPACE));
                log.info("Create dinky ingress service success, cost time:{} ms", SystemClock.now() - ingressStart);
                kubernetesResult = waitForJmAndJobStartByIngress(kubernetesClient, deployment, clusterClient);
            } else {
                kubernetesResult = waitForJmAndJobStart(kubernetesClient, deployment, clusterClient);
            }
            kubernetesResult.success();
            return kubernetesResult;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            submitLock.unlock();
            close();
        }
    }

    /**
     * 提交前清理属于防止同名资源冲突的增强措施；检查权限不足或清理失败时记录完整异常，
     * 但不能替代正式提交结果，更不能因此提前终止任务提交。
     */
    protected void tryCleanupExistingApplicationBeforeSubmit(
            KubernetesClient kubernetesClient, String namespace, String clusterId) {
        try {
            cleanupExistingApplication(kubernetesClient, namespace, clusterId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(
                    "Interrupted while checking or cleaning existing Kubernetes application {}/{}; continue submission",
                    namespace,
                    clusterId,
                    e);
        } catch (Exception e) {
            logger.error(
                    "Failed to check or clean existing Kubernetes application {}/{}; continue submission",
                    namespace,
                    clusterId,
                    e);
        }
    }

    /** 二次提交复用原有停止链路清理同名 Flink Application，避免预检查失败导致清理动作无法执行。 */
    protected void cleanupExistingApplication(KubernetesClient kubernetesClient, String namespace, String clusterId)
            throws InterruptedException {
        logger.warn(
                "Try to clean Kubernetes application {} in namespace {} before resubmit",
                clusterId,
                namespace);
        // 该 Flink 原生清理操作与已有停止功能使用同一链路，目标不存在时也可安全重复执行。
        getK8sClientHelper().getClient().stopAndCleanupCluster(clusterId);

        for (int retry = 0; retry < EXISTING_CLUSTER_DELETE_TIMEOUT_SECONDS; retry++) {
            if (!hasExistingApplicationResources(kubernetesClient, namespace, clusterId)) {
                logger.info("Existing Kubernetes application {} has been deleted", clusterId);
                return;
            }
            Thread.sleep(1000);
        }
        throw new GatewayException(StrFormatter.format(
                "Delete existing Kubernetes application {} timed out after {} seconds",
                clusterId,
                EXISTING_CLUSTER_DELETE_TIMEOUT_SECONDS));
    }

    /** 供提交失败后的人工兜底操作使用，仅在同名 Kubernetes Application 仍存在时执行清理。 */
    public boolean cleanupExistingApplication() {
        return cleanupExistingApplicationIf(() -> true);
    }

    /**
     * 延迟回收需要在与提交共用的锁内再次确认归属，避免旧 FAILED 实例在重提期间删除同名的新 Application。
     */
    public boolean cleanupExistingApplicationIf(BooleanSupplier cleanupGuard) {
        init();
        String namespace = configuration.getString(KubernetesConfigOptions.NAMESPACE);
        String clusterId = configuration.getString(KubernetesConfigOptions.CLUSTER_ID);
        Lock submitLock = SUBMIT_LOCKS.get(namespace + "/" + clusterId);
        submitLock.lock();
        try (KubernetesClient kubernetesClient = getK8sClientHelper().getKubernetesClient()) {
            if (!cleanupGuard.getAsBoolean()) {
                logger.info(
                        "Skip cleaning Kubernetes application {}/{} because the failed instance is no longer current",
                        namespace,
                        clusterId);
                return false;
            }
            boolean exists = hasExistingApplicationResources(kubernetesClient, namespace, clusterId);
            if (exists) {
                cleanupExistingApplication(kubernetesClient, namespace, clusterId);
            }
            return exists;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GatewayException("Interrupted while cleaning existing Kubernetes application", e);
        } finally {
            submitLock.unlock();
            close();
        }
    }

    /** Deployment 与内外部 Service 均消失后才能重建，避免残留资源影响新任务。 */
    protected boolean hasExistingApplicationResources(
            KubernetesClient kubernetesClient, String namespace, String clusterId) {
        // 显式声明 group/version，规避 Fabric8 旧版本通过资源模型推断时误用 extensions API。
        GenericKubernetesResource deployment = kubernetesClient
                .genericKubernetesResources(APPS_V1_DEPLOYMENT)
                .inNamespace(namespace)
                .withName(clusterId)
                .get();
        Service restService = kubernetesClient
                .services()
                .inNamespace(namespace)
                .withName(clusterId + "-rest")
                .get();
        Service internalService = kubernetesClient
                .services()
                .inNamespace(namespace)
                .withName(clusterId)
                .get();
        return Objects.nonNull(deployment) || Objects.nonNull(restService) || Objects.nonNull(internalService);
    }

    /**
     * Checks the status of a Pod in Kubernetes.
     *
     * @param pod The Pod to check the status of.
     * @return True if the Pod is ready, false otherwise.
     * @throws GatewayException if the Pod has restarted or terminated.
     */
    public boolean checkPodStatus(Pod pod) {
        // Get the Flink container status.
        Optional<ContainerStatus> flinContainer = pod.getStatus().getContainerStatuses().stream()
                .filter(s -> s.getName().equals(Constants.MAIN_CONTAINER_NAME))
                .findFirst();
        if (!flinContainer.isPresent()) {
            return false;
        }
        ContainerStatus containerStatus = flinContainer.get();
        Yaml yaml = new Yaml(new IgnoreNullRepresenter());
        String logStr = StrFormatter.format(
                "Got Flink Container State:\nPod: {},\tReady: {},\trestartCount: {},\timage: {}\n"
                        + "------CurrentState------\n{}\n------LastState------\n{}",
                pod.getMetadata().getName(),
                containerStatus.getReady(),
                containerStatus.getRestartCount(),
                containerStatus.getImage(),
                yaml.dumpAsMap(containerStatus.getState()),
                yaml.dumpAsMap(containerStatus.getLastState()));
        logger.info(logStr);

        if (containerStatus.getRestartCount() > 0 || containerStatus.getState().getTerminated() != null) {
            throw new GatewayException("Deploy k8s failed, pod have restart or terminated");
        }
        return containerStatus.getReady();
    }

    /**
     * Deploys an application to Kubernetes.
     *
     * @return A ClusterClientProvider<String> object for accessing the Kubernetes cluster.
     * @throws ClusterDeploymentException if deployment to Kubernetes fails.
     */
    public ClusterClientProvider<String> deployApplication(FlinkKubeClient client) throws ClusterDeploymentException {
        // Build the commit information
        AppConfig appConfig = config.getAppConfig();
        String[] userJarParas =
                Asserts.isNotNull(appConfig.getUserJarParas()) ? appConfig.getUserJarParas() : new String[0];
        ClusterSpecification.ClusterSpecificationBuilder clusterSpecificationBuilder =
                createClusterSpecificationBuilder();
        // Deploy to k8s
        ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(userJarParas, appConfig.getUserJarMainAppClass());
        ClusterDescriptorAdapterImpl clusterDescriptorAdapter = new ClusterDescriptorAdapterImpl();
        KubernetesClusterDescriptor kubernetesClusterDescriptor =
                clusterDescriptorAdapter.createKubernetesClusterDescriptor(configuration, client);
        return kubernetesClusterDescriptor.deployApplicationCluster(
                clusterSpecificationBuilder.createClusterSpecification(), applicationConfiguration);
    }

    /**
     * Waits for the JobManager and the Job to start in Kubernetes.
     *
     * @param deployment    The deployment in Kubernetes.
     * @param clusterClient The ClusterClientProvider<String> object for accessing the Kubernetes cluster.
     * @return A KubernetesResult object containing the Kubernetes gateway's Web URL, the Job ID, and the cluster ID.
     * @throws InterruptedException if waiting is interrupted.
     */
    public KubernetesResult waitForJmAndJobStart(
            KubernetesClient kubernetesClient, Deployment deployment, ClusterClientProvider<String> clusterClient)
            throws InterruptedException {
        KubernetesResult result = KubernetesResult.build(getType());
        long waitSends = SystemConfiguration.getInstances().GetJobIdWaitValue() * 1000L;
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < waitSends) {
            List<Pod> pods = kubernetesClient
                    .pods()
                    .inNamespace(deployment.getMetadata().getNamespace())
                    .withLabelSelector(deployment.getSpec().getSelector())
                    .list()
                    .getItems();
            for (Pod pod : pods) {
                if (!checkPodStatus(pod)) {
                    logger.info("Kubernetes Pod have not ready, reTry at 5 sec later");
                    continue;
                }
                try (ClusterClient<String> client = clusterClient.getClusterClient()) {
                    // Kubernetes 集群内的 Flink REST 已可直接访问时，绕过可能无法完成的 ClusterClient 异步请求，
                    // 避免作业实际已运行但 Dinky 因 listJobs Future 超时而将提交误判为失败。
                    String webUrl = client.getWebInterfaceURL();
                    String queryUrl = resolveRestQueryUrl(kubernetesClient, deployment, webUrl);
                    String accessibleWebUrl = queryUrl;
                    logger.info("Start get Kubernetes application job overview from {}", queryUrl);
                    JobDetails jobDetails = invokeJobsOverviewApi(queryUrl);
                    // ClusterIP 可能对集群外部署的 Dinky 不可达，失败时回退到 Flink 提供的 NodePort/外部地址。
                    if (Objects.isNull(jobDetails) && !StringUtils.equals(queryUrl, webUrl)) {
                        logger.warn("Get job overview from {} failed, fallback to {}", queryUrl, webUrl);
                        jobDetails = invokeJobsOverviewApi(webUrl);
                        accessibleWebUrl = webUrl;
                    }
                    if (Objects.isNull(jobDetails) || CollectionUtils.isEmpty(jobDetails.getJobs())) {
                        logger.info("Kubernetes application job is not ready, will retry later");
                        continue;
                    }
                    JobOverviewInfo job =
                            jobDetails.getJobs().stream().findFirst().get();
                    // To create a cluster ID, you need to combine the cluster ID with the jobID to ensure uniqueness
                    String cid = configuration.getString(KubernetesConfigOptions.CLUSTER_ID) + job.getJid();
                    logger.info("Success get Kubernetes application job status: {}", job.getState());
                    // 监控复用本轮已验证可访问的 REST 地址，避免 ClusterIP 模式保存 DNS 后被探活清空。
                    return result.setWebURL(accessibleWebUrl)
                            .setJids(Collections.singletonList(job.getJid()))
                            .setId(cid);
                } catch (GatewayException e) {
                    throw e;
                } catch (Exception ex) {
                    // 输出完整异常堆栈，避免 TimeoutException 等无 message 异常被记录成 null，影响提交故障定位。
                    logger.error("Get Kubernetes application job status failed.", ex);
                }
            }
            Thread.sleep(5000);
        }
        throw new GatewayException(
                "The number of retries exceeds the limit, check the K8S cluster for more information");
    }

    /** ClusterIP 模式使用当前 Service 地址规避旧 DNS 缓存，其他暴露模式保留 Flink 返回的外部地址。 */
    protected String resolveRestQueryUrl(KubernetesClient kubernetesClient, Deployment deployment, String webUrl) {
        String namespace = deployment.getMetadata().getNamespace();
        String clusterId = deployment.getMetadata().getName();
        Service restService = kubernetesClient
                .services()
                .inNamespace(namespace)
                .withName(clusterId + "-rest")
                .get();
        if (Objects.isNull(restService) || Objects.isNull(restService.getSpec())) {
            return webUrl;
        }
        return buildRestQueryUrl(
                restService.getSpec().getType(), restService.getSpec().getClusterIP(), webUrl);
    }

    /** NodePort、LoadBalancer、Headless 等场景需要使用 Flink 返回的可访问地址。 */
    static String buildRestQueryUrl(String serviceType, String clusterIp, String webUrl) {
        if (!"ClusterIP".equalsIgnoreCase(serviceType)
                || StringUtils.isBlank(clusterIp)
                || "None".equalsIgnoreCase(clusterIp)) {
            return webUrl;
        }
        return StrFormatter.format("http://{}:8081", clusterIp);
    }

    /**
     * Waits for the JobManager and the Job to start in Kubernetes by ingress.
     *
     * @param deployment    The deployment in Kubernetes.
     * @param clusterClient The ClusterClientProvider<String> object for accessing the Kubernetes cluster.
     * @return A KubernetesResult object containing the Kubernetes gateway's Web URL, the Job ID, and the cluster ID.
     * @throws InterruptedException if waiting is interrupted.
     */
    public KubernetesResult waitForJmAndJobStartByIngress(
            KubernetesClient kubernetesClient, Deployment deployment, ClusterClientProvider<String> clusterClient)
            throws InterruptedException {
        KubernetesResult result = KubernetesResult.build(getType());
        long waitSends = SystemConfiguration.getInstances().GetJobIdWaitValue() * 1000L;
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < waitSends) {
            List<Pod> pods = kubernetesClient
                    .pods()
                    .inNamespace(deployment.getMetadata().getNamespace())
                    .withLabelSelector(deployment.getSpec().getSelector())
                    .list()
                    .getItems();
            for (Pod pod : pods) {
                if (!checkPodStatus(pod)) {
                    logger.info("Kubernetes Pod have not ready, reTry at 5 sec later");
                    continue;
                }
                try {
                    logger.info("Start get job list ....");
                    JobDetails jobDetails = fetchApplicationJob(kubernetesClient, deployment);
                    if (Objects.isNull(jobDetails) || CollectionUtils.isEmpty(jobDetails.getJobs())) {
                        logger.error("Get job is empty, will be reconnect alter 5 sec later....");
                        Thread.sleep(5000);
                        continue;
                    }
                    JobOverviewInfo jobOverviewInfo =
                            jobDetails.getJobs().stream().findFirst().get();
                    // To create a cluster ID, you need to combine the cluster ID with the jobID to ensure uniqueness
                    String cid = configuration.getString(KubernetesConfigOptions.CLUSTER_ID) + jobOverviewInfo.getJid();
                    logger.info("Success get job status: {}", jobOverviewInfo.getState());

                    return result.setJids(Collections.singletonList(jobOverviewInfo.getJid()))
                            .setWebURL(jobDetails.getWebUrl())
                            .setId(cid);
                } catch (GatewayException e) {
                    throw e;
                } catch (Exception ex) {
                    logger.error("Get job status failed,{}", ex.getMessage());
                }
            }
            Thread.sleep(5000);
        }
        throw new GatewayException(
                "The number of retries exceeds the limit, check the K8S cluster for more information");
    }

    private JobDetails fetchApplicationJob(KubernetesClient kubernetesClient, Deployment deployment) {
        // 判断是不是存在ingress, 如果存在ingress的话返回ingress地址
        Ingress ingress = kubernetesClient
                .network()
                .v1()
                .ingresses()
                .inNamespace(deployment.getMetadata().getNamespace())
                .withName(deployment.getMetadata().getName())
                .get();
        String ingressUrl = getIngressUrl(
                ingress,
                deployment.getMetadata().getNamespace(),
                deployment.getMetadata().getName());
        logger.info("Get dinky ingress url:{}", ingressUrl);
        return invokeJobsOverviewApi(ingressUrl);
    }

    private JobDetails invokeJobsOverviewApi(String restUrl) {
        try {
            String body;
            try (HttpResponse execute = HttpUtil.createGet(restUrl + "/jobs/overview")
                    .timeout(10000)
                    .execute()) {
                // 判断状态码，如果是504的话可能是因为task manage节点还未启动
                if (Objects.equals(execute.getStatus(), HttpStatus.HTTP_GATEWAY_TIMEOUT)) {
                    return null;
                }
                body = execute.body();
            }
            if (StringUtils.isNotEmpty(body)) {
                JobDetails jobDetails = JSONObject.parseObject(body, JobDetails.class);
                jobDetails.setWebUrl(restUrl);
                return jobDetails;
            }
        } catch (Exception e) {
            // REST 暂未就绪属于启动阶段的可重试状态，但保留异常类型与地址便于区分网络和响应解析问题。
            logger.warn("Get job overview from {} failed: {}", restUrl, e.toString());
        }
        return null;
    }

    private String getIngressUrl(Ingress ingress, String namespace, String clusterId) {
        if (Objects.nonNull(ingress)
                && Objects.nonNull(ingress.getSpec())
                && Objects.nonNull(ingress.getSpec().getRules())
                && !ingress.getSpec().getRules().isEmpty()) {
            String host = ingress.getSpec().getRules().get(0).getHost();
            return StrFormatter.format("http://{}/{}/{}", host, namespace, clusterId);
        }
        throw new GatewayException(
                StrFormatter.format("Dinky clusterId {} ingress not found in namespace {}", clusterId, namespace));
    }

    /**
     * Determine whether to use the ingress agent service
     * @return ingress domain
     */
    private String checkUseIngress() {
        Map<String, Object> ingressConfig = k8sConfig.getIngressConfig();
        if (MapUtils.isNotEmpty(ingressConfig)) {
            boolean ingressEnable = Boolean.parseBoolean(ingressConfig
                    .getOrDefault(DINKY_K8S_INGRESS_ENABLED_KEY, "false")
                    .toString());
            String ingressDomain = ingressConfig
                    .getOrDefault(DINKY_K8S_INGRESS_DOMAIN_KEY, StringUtils.EMPTY)
                    .toString();
            if (ingressEnable && StringUtils.isNotEmpty(ingressDomain)) {
                return ingressDomain;
            }
        }
        return StringUtils.EMPTY;
    }
}
