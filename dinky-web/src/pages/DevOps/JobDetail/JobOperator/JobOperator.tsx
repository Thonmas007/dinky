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

import { cancelTask, savePointTask } from '@/pages/DataStudio/service';
import { isStatusDone } from '@/pages/DevOps/function';
import EditJobInstanceForm from '@/pages/DevOps/JobDetail/JobOperator/components/EditJobInstanceForm';
import RestartForm from '@/pages/DevOps/JobDetail/JobOperator/components/RestartForm';
import { discoverJobId } from '@/pages/DevOps/JobDetail/srvice';
import { API_CONSTANTS } from '@/services/endpoints';
import { Jobs } from '@/types/DevOps/data';
import { l } from '@/utils/intl';
import { EllipsisOutlined, RedoOutlined, SearchOutlined } from '@ant-design/icons';
import { Button, Dropdown, message, Modal, Space } from 'antd';
import { useState } from 'react';

const operatorType = {
  CANCEL_JOB: 'canceljob',
  SAVEPOINT_CANCEL: 'cancel',
  SAVEPOINT_TRIGGER: 'trigger',
  SAVEPOINT_STOP: 'stop',
  AUTO_STOP: 'autostop'
};
export type OperatorType = {
  jobDetail: Jobs.JobInfoDetail;
  refesh: (isForce: boolean) => void;
};
const JobOperator = (props: OperatorType) => {
  const { jobDetail, refesh } = props;
  const [discovering, setDiscovering] = useState(false);
  const jobManagerHost = jobDetail?.clusterInstance?.jobManagerHost;
  const clusterType = jobDetail?.clusterInstance?.type;
  const canDiscoverJobId = [
    'ka',
    'kao',
    'kubernetes-application',
    'kubernetes-application-operator'
  ].includes(clusterType as string);
  const webUri =
    jobManagerHost?.startsWith('http://') || jobManagerHost?.startsWith('https://')
      ? jobManagerHost
      : `${API_CONSTANTS.BASE_URL}/api/flink/${jobManagerHost}/#/job/running/${jobDetail?.instance?.jid}/overview`;

  const handleJobOperator = (key: string) => {
    Modal.confirm({
      title: l('devops.jobinfo.job.key', '', { key: key }),
      content: l('devops.jobinfo.job.keyConfirm', '', { key: key }),
      okText: l('button.confirm'),
      cancelText: l('button.cancel'),
      onOk: async () => {
        if (key == operatorType.CANCEL_JOB) {
          await cancelTask('', jobDetail?.instance?.taskId, false);
        } else if (key == operatorType.SAVEPOINT_CANCEL) {
          await savePointTask('', jobDetail?.instance?.taskId, 'cancel');
        } else if (key == operatorType.SAVEPOINT_STOP) {
          await savePointTask('', jobDetail?.instance?.taskId, 'stop');
        } else if (key == operatorType.SAVEPOINT_TRIGGER) {
          await savePointTask('', jobDetail?.instance?.taskId, 'trigger');
        } else if (key == operatorType.AUTO_STOP) {
          await cancelTask('', jobDetail?.instance?.taskId);
        }
        message.success(l('devops.jobinfo.job.key.success', '', { key: key }));
      }
    });
  };

  /** 人工恢复入口会重新关联新 JID；请求完成后立即刷新详情，避免等待下一次三秒轮询。 */
  const handleDiscoverJobId = async () => {
    const jobInstanceId = jobDetail?.instance?.id;
    if (!jobInstanceId) {
      return;
    }
    setDiscovering(true);
    try {
      await discoverJobId(jobInstanceId);
      message.success(l('devops.jobinfo.discover.jobid.success'));
      refesh(false);
    } finally {
      setDiscovering(false);
    }
  };

  return (
    <Space>
      <EditJobInstanceForm jobDetail={jobDetail} refeshJob={refesh} />
      <Button icon={<RedoOutlined />} onClick={() => refesh(true)} />

      {canDiscoverJobId && (
        <Button
          key='discover-job-id'
          icon={<SearchOutlined />}
          loading={discovering}
          onClick={handleDiscoverJobId}
        >
          {l('devops.jobinfo.discover.jobid')}
        </Button>
      )}

      <Button key='flinkwebui' href={webUri} target={'_blank'}>
        FlinkWebUI
      </Button>

      <RestartForm
        lastCheckpoint={jobDetail?.jobDataDto?.checkpoints?.latest}
        instance={jobDetail?.instance}
      />

      {isStatusDone(jobDetail?.instance?.status as string) ? (
        <></>
      ) : (
        <>
          <Button
            key='autostop'
            type='primary'
            onClick={() => handleJobOperator(operatorType.AUTO_STOP)}
            danger
          >
            {jobDetail?.instance?.step == 5
              ? l('devops.jobinfo.offline')
              : l('devops.jobinfo.smart_stop')}
          </Button>
          <Dropdown
            key='dropdown'
            trigger={['click']}
            menu={{
              onClick: (e) => handleJobOperator(e.key as string),
              items: [
                {
                  key: operatorType.SAVEPOINT_TRIGGER,
                  label: l('devops.jobinfo.savepoint.trigger')
                },
                {
                  key: operatorType.SAVEPOINT_STOP,
                  label: l('devops.jobinfo.savepoint.stop')
                },
                {
                  key: operatorType.SAVEPOINT_CANCEL,
                  label: l('devops.jobinfo.savepoint.cancel')
                },
                {
                  key: operatorType.CANCEL_JOB,
                  label: l('devops.jobinfo.savepoint.canceljob')
                }
              ]
            }}
          >
            <Button key='4' style={{ padding: '0 8px' }}>
              <EllipsisOutlined />
            </Button>
          </Dropdown>
        </>
      )}
    </Space>
  );
};
export default JobOperator;
