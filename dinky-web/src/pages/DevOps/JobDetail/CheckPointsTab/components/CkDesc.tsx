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

import { JobProps } from '@/pages/DevOps/JobDetail/data';

import EllipsisMiddle from '@/components/Typography/EllipsisMiddle';
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  ExclamationCircleOutlined,
  RocketOutlined,
  SyncOutlined
} from '@ant-design/icons';
import { Button, Descriptions, Space, Tag } from 'antd';
import { recoveryCheckPoint } from '@/pages/DevOps/JobDetail/CheckPointsTab/components/functions';
import { l } from '@/utils/intl';

const CkDesc = (props: JobProps) => {
  const { jobDetail } = props;

  const counts = jobDetail?.jobDataDto?.checkpoints?.counts;
  const checkpoints = jobDetail?.jobDataDto?.checkpoints;
  const latest = checkpoints?.latest;
  const checkpointHistory = checkpoints?.history ?? [];
  const isFinishedJob = ['CANCELED', 'CANCEL', 'FAILED', 'FINISHED', 'SUCCESS', 'UNKNOWN'].includes(
    jobDetail?.instance?.status
  );
  const historyCompleted = checkpointHistory
    .filter((checkpoint: any) => checkpoint.status === 'COMPLETED' && !checkpoint.is_savepoint)
    .sort(
      (left: any, right: any) => (right.trigger_timestamp ?? 0) - (left.trigger_timestamp ?? 0)
    );
  const historySavepoints = checkpointHistory
    .filter((checkpoint: any) => checkpoint.status === 'COMPLETED' && checkpoint.is_savepoint)
    .sort(
      (left: any, right: any) => (right.trigger_timestamp ?? 0) - (left.trigger_timestamp ?? 0)
    );
  const historyFailed = checkpointHistory
    .filter((checkpoint: any) => checkpoint.status === 'FAILED')
    .sort(
      (left: any, right: any) => (right.trigger_timestamp ?? 0) - (left.trigger_timestamp ?? 0)
    );

  // 已结束任务的 Flink REST 可能不再返回 latest/config，使用持久化历史补齐展示，运行中实时数据优先。
  const effectiveLatest = {
    ...latest,
    completed: isFinishedJob ? latest?.completed ?? historyCompleted[0] : latest?.completed,
    savepoint: isFinishedJob ? latest?.savepoint ?? historySavepoints[0] : latest?.savepoint,
    failed: isFinishedJob ? latest?.failed ?? historyFailed[0] : latest?.failed
  };
  const liveCheckpointConfig = jobDetail?.jobDataDto?.checkpointsConfig;
  const historyConfig = jobDetail?.history?.configJson?.configJson;
  const checkpointsConfigInfo =
    !isFinishedJob || liveCheckpointConfig?.mode || liveCheckpointConfig?.interval
      ? liveCheckpointConfig
      : {
          ...liveCheckpointConfig,
          interval: historyConfig?.['execution.checkpointing.interval']
        };

  return (
    <>
      <Descriptions bordered size='small' column={4}>
        <Descriptions.Item label='Checkpoint Mode'>
          <Tag color='blue' title={'Checkpoint Mode'}>
            {checkpointsConfigInfo?.mode?.toUpperCase() ?? 'None'}
          </Tag>
        </Descriptions.Item>

        <Descriptions.Item label='Interval'>
          <Tag color='blue' title={'Interval'}>
            {checkpointsConfigInfo?.interval ?? 'None'}
          </Tag>
        </Descriptions.Item>

        <Descriptions.Item label='Timeout'>
          <Tag color='blue' title={'Timeout'}>
            {checkpointsConfigInfo?.timeout ?? 'None'}
          </Tag>
        </Descriptions.Item>

        <Descriptions.Item label='Unaligned Checkpoints '>
          <Tag color='blue' title={'Unaligned Checkpoints'}>
            {checkpointsConfigInfo?.unaligned_checkpoints ? 'Enabled' : 'Disabled'}
          </Tag>
        </Descriptions.Item>

        <Descriptions.Item label='Latest Restored'>
          <Tag color='green' title={'Latest Completed CheckPoint'}>
            <EllipsisMiddle maxCount={30}>
              {effectiveLatest?.restored?.external_path ?? 'None'}
            </EllipsisMiddle>
          </Tag>
        </Descriptions.Item>

        <Descriptions.Item label='Latest Failed CheckPoint'>
          <Tag color='red' title={'Latest Failed CheckPoint'}>
            id: {effectiveLatest?.failed?.id ?? 'None'}
          </Tag>
        </Descriptions.Item>

        <Descriptions.Item label='Latest Completed CheckPoint'>
          <Tag color='green' title={'Latest Completed CheckPoint'}>
            <EllipsisMiddle maxCount={30}>
              {effectiveLatest?.completed?.external_path ?? 'None'}
            </EllipsisMiddle>
          </Tag>
          {effectiveLatest?.completed?.external_path ? (
            <Button
              onClick={() =>
                recoveryCheckPoint(
                  jobDetail?.instance?.taskId,
                  effectiveLatest.completed.external_path
                )
              }
            >
              {l('devops.jobinfo.ck.recovery.recoveryTo')}
            </Button>
          ) : undefined}
        </Descriptions.Item>

        <Descriptions.Item label='Persist Checkpoints Externally Enabled'>
          <Tag color='blue' title={'Persist Checkpoints Externally Enabled'}>
            {checkpointsConfigInfo?.externalization?.enabled ? 'Enabled' : 'Disabled'}
          </Tag>
        </Descriptions.Item>
        <Descriptions.Item label='Latest Savepoint'>
          <Tag color='purple' title={'Latest Savepoint'}>
            <EllipsisMiddle maxCount={30}>
              {effectiveLatest?.savepoint?.external_path ?? 'None'}
            </EllipsisMiddle>
          </Tag>
        </Descriptions.Item>

        <Descriptions.Item label='CheckPoint Counts'>
          <Space direction={'horizontal'}>
            <Tag color='blue' title={'Total'}>
              <RocketOutlined /> Total: {counts?.total ?? 0}
            </Tag>
            <Tag color='red' title={'Failed'}>
              <CloseCircleOutlined /> Failed: {counts?.failed ?? 0}
            </Tag>
            <Tag color='cyan' title={'Restored'}>
              <ExclamationCircleOutlined /> Restored: {counts?.restored ?? 0}
            </Tag>
            <Tag color='green' title={'Completed'}>
              <CheckCircleOutlined /> Completed: {counts?.completed ?? 0}
            </Tag>
            <Tag color='orange' title={'In Progress'}>
              <SyncOutlined spin /> In Progress: {counts?.in_progress ?? 0}
            </Tag>
          </Space>
        </Descriptions.Item>
      </Descriptions>
    </>
  );
};

export default CkDesc;
