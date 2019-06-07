#!/usr/bin/env bash
readonly ANSIBLE_HOME=benchmark/ansible
readonly DOCKER_COMPOSE=benchmark/ansible/docker-compose.yml

package() {
  mvn -DskipTests=true clean package
}

copy_worker_artifacts() {
  local files="${ANSIBLE_HOME}/roles/flamestream-common/files/"
  local worker=runtime/target/flamestream-runtime-1.0-SNAPSHOT.jar
  local dependencies=runtime/target/lib
  local entrypoint=runtime/target/entrypoint.sh
  local examples=examples/target/flamestream-examples-1.0-SNAPSHOT.jar
  local examples_dependencies=examples/target/lib

  if [[ -f "$worker" && -d "$dependencies" && -f "$entrypoint" && -f "$examples" && -d "$examples_dependencies" ]]; then
    rm -rf "${files}/lib"
    cp "$worker" "$files"
    cp "$entrypoint" "$files"
    cp -r "$dependencies" "$files"
    cp -rp "$examples_dependencies" "$files"
    cp -rp "$examples" "${files}/lib"
  else
    echo "Some artifacts hasn't been found"
    return 1
  fi
}

copy_flink_artifacts() {
  local files="${ANSIBLE_HOME}/roles/flink-bench/files"
  local bench="benchmark/flink-benchmark/target/flink-benchmark-1.0-SNAPSHOT-uber.jar"
  if [[ -f "$bench" ]]; then
    cp "$bench" "$files"
  else
    echo "Some artifacts hasn't been found"
    return 1
  fi
}

docker_compose_up() {
  docker-compose -f "$DOCKER_COMPOSE" up -d
}

local_bench() {
  docker_compose_up \
    && package \
    && copy_worker_artifacts \
    && ansible-playbook -v -i "${ANSIBLE_HOME}/local.yml" "${ANSIBLE_HOME}/flamestream.yml"
}

remote_bench() {
  package \
    && copy_worker_artifacts \
    && ansible-playbook -v -i "${ANSIBLE_HOME}/remote.yml" "${ANSIBLE_HOME}/flamestream.yml"
}

local_flink_bench() {
  docker_compose_up \
    && package \
    && copy_flink_artifacts \
    && ansible-playbook -v -i "${ANSIBLE_HOME}/local.yml" "${ANSIBLE_HOME}/flink.yml"
}

remote_flink_bench() {
  package \
    && copy_flink_artifacts \
    && ansible-playbook -v -i "${ANSIBLE_HOME}/remote.yml" "${ANSIBLE_HOME}/flink.yml"
}

[[ "$0" == "$BASH_SOURCE" ]] && package && copy_worker_artifacts
