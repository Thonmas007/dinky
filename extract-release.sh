#!/usr/bin/env bash

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${PROJECT_DIR}/build"
DINKY_VERSION="${DINKY_VERSION:-1.2.5}"
FLINK_VERSION="${FLINK_VERSION:-1.20}"
EXPECTED_AWS_VERSION="${EXPECTED_AWS_VERSION:-1.12.782}"
RELEASE_NAME="dinky-release-${FLINK_VERSION}-${DINKY_VERSION}"
ARCHIVE_PATH="${1:-${BUILD_DIR}/${RELEASE_NAME}.tar.gz}"
RUNTIME_DIR="${BUILD_DIR}/runtime"
TARGET_DIR="${RUNTIME_DIR}/${RELEASE_NAME}"
STAGING_DIR=""
BACKUP_DIR=""

# 先在独立目录完成解压和依赖校验，避免半成品覆盖当前运行目录。
cleanup() {
  if [[ -n "${STAGING_DIR}" && -d "${STAGING_DIR}" ]]; then
    rm -rf "${STAGING_DIR}"
  fi
}
trap cleanup EXIT

if [[ ! -f "${ARCHIVE_PATH}" ]]; then
  echo "未找到发布包：${ARCHIVE_PATH}" >&2
  exit 1
fi

STAGING_DIR="$(mktemp -d "${BUILD_DIR}/.extract.XXXXXX")"
tar -xzf "${ARCHIVE_PATH}" -C "${STAGING_DIR}"

STAGED_RELEASE_DIR="${STAGING_DIR}/${RELEASE_NAME}"
STAGED_LIB_DIR="${STAGED_RELEASE_DIR}/lib"
if [[ ! -d "${STAGED_LIB_DIR}" ]]; then
  echo "发布包结构不正确，未找到：${STAGED_LIB_DIR}" >&2
  exit 1
fi

# AWS SDK 组件必须使用同一目标版本，避免发布包本身携带混合依赖。
AWS_ARTIFACTS=(aws-java-sdk-core aws-java-sdk-kms aws-java-sdk-s3 jmespath-java)
for artifact in "${AWS_ARTIFACTS[@]}"; do
  expected_jar="${STAGED_LIB_DIR}/${artifact}-${EXPECTED_AWS_VERSION}.jar"
  if [[ ! -f "${expected_jar}" ]]; then
    echo "缺少目标 AWS 依赖：${expected_jar}" >&2
    exit 1
  fi

  unexpected_jar="$(find "${STAGED_LIB_DIR}" -maxdepth 1 -type f -name "${artifact}-*.jar" ! -name "${artifact}-${EXPECTED_AWS_VERSION}.jar" -print -quit)"
  if [[ -n "${unexpected_jar}" ]]; then
    echo "检测到非目标 AWS 依赖：${unexpected_jar}" >&2
    exit 1
  fi
done

mkdir -p "${RUNTIME_DIR}"
if [[ -e "${TARGET_DIR}" ]]; then
  BACKUP_DIR="${TARGET_DIR}.backup-$(date '+%Y%m%d%H%M%S')"
  mv "${TARGET_DIR}" "${BACKUP_DIR}"
fi

# 整体替换发布目录，确保旧版本 JAR 不会因增量覆盖而残留。
if ! mv "${STAGED_RELEASE_DIR}" "${TARGET_DIR}"; then
  if [[ -n "${BACKUP_DIR}" && -d "${BACKUP_DIR}" ]]; then
    mv "${BACKUP_DIR}" "${TARGET_DIR}"
  fi
  echo "更新运行目录失败，已尝试恢复原目录" >&2
  exit 1
fi

echo "发布包解压完成：${TARGET_DIR}"
if [[ -n "${BACKUP_DIR}" ]]; then
  echo "原运行目录已备份：${BACKUP_DIR}"
fi
echo "AWS SDK 版本校验通过：${EXPECTED_AWS_VERSION}"
