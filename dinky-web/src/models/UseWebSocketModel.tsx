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

import { useEffect, useRef } from 'react';
import { request } from '@umijs/max';
import { ErrorMessage } from '@/utils/messages';
import { v4 as uuidv4 } from 'uuid';
import { TOKEN_KEY } from '@/services/constants';

export type WsData = {
  topic: string;
  data: Record<string, any>;
  type: string;
};

export enum Topic {
  JVM_INFO = 'JVM_INFO',
  PROCESS_CONSOLE = 'PROCESS_CONSOLE',
  PRINT_TABLE = 'PRINT_TABLE',
  METRICS = 'METRICS',
  TASK_RUN_INSTANCE = 'TASK_RUN_INSTANCE'
}

export type SubscriberData = {
  key: string;
  topic: Topic;
  params: string[];
  call: (data: WsData) => void;
};

export default () => {
  const subscriberRef = useRef<SubscriberData[]>([]);
  const lastPongTimeRef = useRef<number>(new Date().getTime());
  const runningSnapshotRef = useRef<WsData>();
  const runningRevisionRef = useRef(0);
  const runningRequestRef = useRef(false);
  const disposedRef = useRef(false);

  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
  const token = JSON.parse(localStorage.getItem(TOKEN_KEY) ?? '{}')?.tokenValue;
  const wsUrl = `${protocol}://${window.location.host}/api/ws/global/${token}`;
  const ws = useRef<WebSocket>();

  // 运行标记共享同一份快照，新打开的项目树和编辑器不再等待下一次状态变化。
  const dispatch = (data: WsData) => {
    if (data.topic === Topic.TASK_RUN_INSTANCE && Array.isArray(data.data?.RunningTaskId)) {
      runningSnapshotRef.current = data;
      runningRevisionRef.current++;
    }
    subscriberRef.current
      .filter((sub) => sub.topic === data.topic)
      .filter((sub) => !sub.params?.length || sub.params.some((key) => key in (data.data ?? {})))
      .forEach((sub) => sub.call(data));
  };

  // 复用监控列表接口补偿丢失的主题消息，HTTP 失败不伪造空列表或停止状态。
  const refreshRunningTasks = async () => {
    if (
      disposedRef.current ||
      runningRequestRef.current ||
      !subscriberRef.current.some((sub) => sub.topic === Topic.TASK_RUN_INSTANCE)
    )
      return;
    runningRequestRef.current = true;
    const revision = runningRevisionRef.current;
    try {
      const result = await request('/api/jobInstance/getRunningTaskIds', {
        method: 'GET',
        skipErrorHandler: true,
        timeout: 8000
      });
      // HTTP 等待期间若已有推送更新，不能用先发请求的旧快照覆盖新状态。
      if (
        !disposedRef.current &&
        revision === runningRevisionRef.current &&
        result?.success &&
        Array.isArray(result.data?.RunningTaskId)
      ) {
        dispatch({ topic: Topic.TASK_RUN_INSTANCE, type: 'SNAPSHOT', data: result.data });
      }
    } catch {
      // 下一轮继续校准，避免短暂网络失败清除真实运行标记。
    } finally {
      runningRequestRef.current = false;
    }
  };

  const reconnect = () => {
    if (disposedRef.current) return;
    if (ws.current) {
      ws.current.onopen = null;
      ws.current.onmessage = null;
      ws.current.close();
    }
    ws.current = new WebSocket(wsUrl);
    ws.current.onopen = () => {
      lastPongTimeRef.current = new Date().getTime();
      receiveMessage();
      subscribe();
    };
  };

  const subscribe = () => {
    const topics: Record<string, string[]> = {};
    subscriberRef.current.forEach((sub) => {
      if (!topics[sub.topic]) {
        topics[sub.topic] = [];
      }
      if (sub.params && sub.params.length > 0) {
        topics[sub.topic] = [...topics[sub.topic], ...sub.params];
      } else {
        topics[sub.topic] = [...topics[sub.topic]];
      }
    });
    if (!ws.current || ws.current.readyState === WebSocket.CLOSED) {
      reconnect();
    } else if (ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify({ topics, type: 'SUBSCRIBE' }));
    } else {
      //TODO do something
    }
  };

  const receiveMessage = () => {
    if (ws.current) {
      ws.current.onmessage = (e) => {
        try {
          const data: WsData = JSON.parse(e.data);
          lastPongTimeRef.current = new Date().getTime();
          dispatch(data);
        } catch (e: any) {
          ErrorMessage(e);
        }
      };
    }
  };

  useEffect(() => {
    disposedRef.current = false;
    receiveMessage();
    const timer = setInterval(() => {
      void refreshRunningTasks();
      if (!ws.current || ws.current.readyState != WebSocket.OPEN) {
        reconnect();
      } else {
        const currentTime = new Date().getTime();
        if (currentTime - lastPongTimeRef.current > 15000) {
          reconnect();
        } else if (currentTime - lastPongTimeRef.current > 5000) {
          ws.current.send(JSON.stringify({ type: 'PING' }));
        }
      }
    }, 10000);
    return () => {
      disposedRef.current = true;
      clearInterval(timer);
      if (ws.current) {
        ws.current.onopen = null;
        ws.current.onmessage = null;
        ws.current.close();
      }
    };
  }, []);

  const subscribeTopic = (topic: Topic, params: string[], onMessage: (data: WsData) => void) => {
    const sub: SubscriberData = { topic: topic, call: onMessage, params: params, key: uuidv4() };
    subscriberRef.current.push(sub);
    if (topic === Topic.TASK_RUN_INSTANCE) {
      if (runningSnapshotRef.current) onMessage(runningSnapshotRef.current);
      void refreshRunningTasks();
    }
    subscribe();
    return () => {
      //组件卸载回调方法，取消订阅此topic
      subscriberRef.current = subscriberRef.current.filter((item) => item.key !== sub.key);
      subscribe();
    };
  };

  return {
    subscribeTopic,
    reconnect
  };
};
