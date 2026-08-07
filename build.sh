#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DINKY_VERSION="${DINKY_VERSION:-1.2.5}"
FLINK_VERSION="${FLINK_VERSION:-1.20}"

# 统一使用 JDK 11，确保 jdk11 Profile 生效并加载对应的 Maven 插件版本管理。
JAVA_HOME="/Users/liuningbo/applications/java/jdk11/Contents/Home"
PATH="${JAVA_HOME}/bin:${PATH}"
export JAVA_HOME PATH DINKY_VERSION FLINK_VERSION

cd "${PROJECT_DIR}"

./mvnw clean package -Dmaven.test.skip=true -Dspotless.check.skip=true -P "aliyun,prod,web,flink-${FLINK_VERSION},flink-single-version"

# 发布包生成后统一走解压脚本，确保上传脚本读取的 build/runtime 目录与最新产物一致，并复用依赖完整性校验。
bash "${PROJECT_DIR}/extract-release.sh" "${PROJECT_DIR}/build/dinky-release-${FLINK_VERSION}-${DINKY_VERSION}.tar.gz"
