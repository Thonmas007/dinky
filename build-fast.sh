#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

# 使用方式：
# 1. 完整发布模式：重新打包后端、前端和 Assembly，并覆盖解压到运行目录。
#    适用于修改 Web、Admin、配置、Assembly 或需要生成完整发布包的场景。
#    bash build-fast.sh
#    bash build-fast.sh full 1.20
#
# 2. 模块增量模式：只编译指定 Maven 模块及其上游依赖，并覆盖运行目录中的对应 JAR。
#    适用于飞书告警等独立 lib 模块，更新速度明显快于完整发布模式。
#    bash build-fast.sh module dinky-alert/dinky-alert-feishu dinky-alert-feishu-1.2.5.jar
#
# 两种模式更新运行目录后都会自动重启服务；只打包不重启时可设置 AUTO_RESTART=false。
#    AUTO_RESTART=false bash build-fast.sh module dinky-alert/dinky-alert-feishu dinky-alert-feishu-1.2.5.jar
#
# 兼容旧用法：直接传 Flink 版本时仍按完整发布模式执行。
#    bash build-fast.sh 1.20

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAVA_HOME="/Users/liuningbo/applications/java/jdk11/Contents/Home"
PATH="${JAVA_HOME}/bin:${PATH}"
DINKY_VERSION="${DINKY_VERSION:-1.2.5}"
AUTO_RESTART="${AUTO_RESTART:-true}"
MODE="${1:-full}"
MODULE_PATH=""
RUNTIME_JAR_NAME=""

case "${MODE}" in
  full)
    FLINK_VERSION="${2:-1.20}"
    ;;
  module)
    if [[ $# -lt 3 ]]; then
      echo "模块增量模式用法：bash build-fast.sh module <模块路径> <运行时 JAR 名称> [Flink 版本]" >&2
      exit 1
    fi
    MODULE_PATH="$2"
    RUNTIME_JAR_NAME="$3"
    FLINK_VERSION="${4:-1.20}"
    ;;
  1.*)
    FLINK_VERSION="${MODE}"
    MODE="full"
    ;;
  *)
    echo "未知模式：${MODE}，可用模式为 full 或 module" >&2
    exit 1
    ;;
esac

RELEASE_NAME="dinky-release-${FLINK_VERSION}-${DINKY_VERSION}"
RELEASE_ARCHIVE="${PROJECT_DIR}/build/${RELEASE_NAME}.tar.gz"
RUNTIME_DIR="${PROJECT_DIR}/build/runtime"
RUNTIME_RELEASE_DIR="${RUNTIME_DIR}/${RELEASE_NAME}"

export JAVA_HOME PATH

cd "${PROJECT_DIR}"

# 运行目录更新完成后重启服务，确保 JVM 加载刚覆盖的 JAR 和配置。
restart_runtime() {
  if [[ "${AUTO_RESTART}" != "true" ]]; then
    echo "已跳过服务重启：AUTO_RESTART=${AUTO_RESTART}"
    return
  fi

  if [[ ! -f "${RUNTIME_RELEASE_DIR}/bin/auto.sh" ]]; then
    echo "未找到服务管理脚本：${RUNTIME_RELEASE_DIR}/bin/auto.sh" >&2
    exit 1
  fi

  (
    cd "${RUNTIME_RELEASE_DIR}"
    bash bin/auto.sh restart "${FLINK_VERSION}"
  )
  echo "服务已重启：${RUNTIME_RELEASE_DIR}"
}

# 模块模式只替换运行时 lib，不重新构建 Web、Admin 和完整发布压缩包。
if [[ "${MODE}" == "module" ]]; then
  MODULE_DIR="${PROJECT_DIR}/${MODULE_PATH}"
  if [[ ! -d "${MODULE_DIR}" ]]; then
    echo "未找到 Maven 模块目录：${MODULE_DIR}" >&2
    exit 1
  fi
  if [[ ! -d "${RUNTIME_RELEASE_DIR}/lib" ]]; then
    echo "未找到运行目录，请先执行完整发布模式：${RUNTIME_RELEASE_DIR}" >&2
    exit 1
  fi

  ./mvnw -pl "${MODULE_PATH}" -am package \
    -Dmaven.clean.skip=true \
    -Dmaven.test.skip=true \
    -Dspotless.check.skip=true \
    -P "aliyun,prod,flink-${FLINK_VERSION},flink-single-version"

  MODULE_JAR="${MODULE_DIR}/target/${RUNTIME_JAR_NAME}"
  if [[ ! -f "${MODULE_JAR}" ]]; then
    echo "未找到模块产物：${MODULE_JAR}" >&2
    exit 1
  fi

  cp -f "${MODULE_JAR}" "${RUNTIME_RELEASE_DIR}/lib/${RUNTIME_JAR_NAME}"
  echo "模块增量编译完成：${MODULE_JAR}"
  echo "运行时 JAR 已更新：${RUNTIME_RELEASE_DIR}/lib/${RUNTIME_JAR_NAME}"
  restart_runtime
  exit 0
fi

# 完整模式保留发布包生成流程，用于涉及聚合产物或前端资源的改动。
./mvnw package \
  -Dmaven.clean.skip=true \
  -Dmaven.test.skip=true \
  -Dspotless.check.skip=true \
  -P "aliyun,prod,web,flink-${FLINK_VERSION},flink-single-version"

if [[ ! -f "${RELEASE_ARCHIVE}" ]]; then
  echo "未找到发布包：${RELEASE_ARCHIVE}" >&2
  exit 1
fi

# 覆盖发布文件但不清空 runtime，保留本地日志、PID 和临时目录，便于直接重启验证。
mkdir -p "${RUNTIME_DIR}"
tar -xzf "${RELEASE_ARCHIVE}" -C "${RUNTIME_DIR}"

echo "增量打包完成：${RELEASE_ARCHIVE}"
echo "运行目录已更新：${RUNTIME_RELEASE_DIR}"
restart_runtime
