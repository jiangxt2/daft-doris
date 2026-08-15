#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

set -Eeuo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${project_root}/docker/compose.yaml"
compose_project="daft-doris-it"
artifact_dir="${project_root}/.artifacts/it"
readiness_pid=""

mkdir -p "${artifact_dir}"

cleanup() {
  local status=$?
  if [[ -n "${readiness_pid}" ]] && kill -0 "${readiness_pid}" 2>/dev/null; then
    kill "${readiness_pid}" 2>/dev/null || true
    wait "${readiness_pid}" 2>/dev/null || true
  fi
  if [[ ${status} -ne 0 ]]; then
    docker compose --project-name "${compose_project}" --file "${compose_file}" logs --no-color \
      >"${artifact_dir}/doris-compose.log" 2>&1 || true
  fi
  docker compose --project-name "${compose_project}" --file "${compose_file}" down \
    --volumes --remove-orphans
  return "${status}"
}
trap cleanup EXIT

cd "${project_root}"
docker compose --project-name "${compose_project}" --file "${compose_file}" up --detach --build
uv run --all-extras python scripts/wait_for_service.py doris
uv run --all-extras python scripts/init_doris.py
uv run --all-extras pytest -p no:cacheprovider tests/integration/doris -vv \
  --junitxml="${artifact_dir}/doris-junit.xml"
