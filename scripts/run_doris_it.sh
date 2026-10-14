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
run_id="$(date +%s)-$$"
artifact_dir="${project_root}/.artifacts/it/${run_id}"
export DAFT_DORIS_FE_IT_IMAGE="${DAFT_DORIS_FE_IT_IMAGE:-daft-doris-fe-it:${run_id}}"
export DAFT_DORIS_BE_IT_IMAGE="${DAFT_DORIS_BE_IT_IMAGE:-daft-doris-be-it:${run_id}}"

mkdir -p "${artifact_dir}"

docker_cmd() {
  env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy docker "$@"
}

compose() {
  docker_cmd compose --project-name "${compose_project}" --file "${compose_file}" "$@"
}

# Refuse to take over another invocation's resources or replace an existing image.
for resource in container network volume; do
  if [[ "${resource}" == container ]]; then
    ids="$(docker_cmd ps --all --quiet --filter "label=com.docker.compose.project=${compose_project}")"
  else
    ids="$(docker_cmd "${resource}" ls --quiet --filter "label=com.docker.compose.project=${compose_project}")"
  fi
  if [[ -n "${ids}" ]]; then
    echo "Refusing existing ${compose_project} ${resource} resources" >&2
    exit 1
  fi
done
for image in "${DAFT_DORIS_FE_IT_IMAGE}" "${DAFT_DORIS_BE_IT_IMAGE}"; do
  if docker_cmd image inspect "${image}" >/dev/null 2>&1; then
    echo "Refusing to replace existing IT image: ${image}" >&2
    exit 1
  fi
done

diagnostics() {
  docker_cmd image ls --filter dangling=true
  docker_cmd image ls --filter "label=io.daft-doris.it.run=${run_id}"
  docker_cmd system df
}
diagnostics >"${artifact_dir}/images-before.log"

cleanup() {
  local status=$?
  trap - EXIT
  compose logs --no-color >"${artifact_dir}/doris-compose.log" 2>&1 || true
  compose down --volumes --remove-orphans || status=1
  # Every candidate has this exact invocation's label, including a failed build.
  while IFS= read -r image_id; do
    [[ -n "${image_id}" ]] || continue
    docker_cmd image inspect "${image_id}" \
      --format '{{.Id}} {{.Created}} {{json .Config.Labels}}' \
      >>"${artifact_dir}/images-cleanup.log" || { status=1; continue; }
    docker_cmd image rm "${image_id}" >>"${artifact_dir}/images-cleanup.log" 2>&1 || status=1
  done < <(docker_cmd image ls --quiet --filter "label=io.daft-doris.it.run=${run_id}" | sort -u)
  diagnostics >"${artifact_dir}/images-after.log" || status=1
  echo "Doris IT artifacts: ${artifact_dir}" >&2
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cd "${project_root}"
for component in fe be; do
  base_image="apache/doris:${component}-4.0.6"
  local_base="daft-doris-${component}-base:4.0.6"
  if ! docker_cmd image inspect "${base_image}" >/dev/null 2>&1; then
    mirror_image="docker.m.daocloud.io/${base_image}"
    docker_cmd pull "${mirror_image}"
    docker_cmd tag "${mirror_image}" "${base_image}"
  fi
  docker_cmd tag "${base_image}" "${local_base}"
  if [[ "${component}" == fe ]]; then
    runtime_image="${DAFT_DORIS_FE_IT_IMAGE}"
  else
    runtime_image="${DAFT_DORIS_BE_IT_IMAGE}"
  fi
  docker_cmd build --build-arg "BASE_IMAGE=${local_base}" \
    --label "io.daft-doris.it.run=${run_id}" \
    --tag "${runtime_image}" --file "docker/${component}.Dockerfile" docker \
    2>&1 | tee "${artifact_dir}/${component}-build.log"
done
compose up --detach --no-build --pull never
uv run --locked --no-sync python scripts/wait_for_service.py doris
uv run --locked --no-sync python scripts/init_doris.py
if [[ $# -eq 0 ]]; then
  set -- tests/integration/doris
fi
uv run --locked --no-sync pytest -p no:cacheprovider "$@" -vv \
  --junitxml="${artifact_dir}/doris-junit.xml" 2>&1 | tee "${artifact_dir}/pytest.log"
