#!/usr/bin/env python3
import argparse
import json
import subprocess
import time
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NODES = {"redis-master": 6379, "redis-replica-1": 6380, "redis-replica-2": 6381}


def run(*args, check=True):
    result = subprocess.run(args, cwd=ROOT, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=120)
    output = result.stdout.strip().replace("\r", "")
    if check and result.returncode:
        raise RuntimeError(f"Command failed: {' '.join(args)}\n{output}")
    return output


def compose(*args):
    return run("docker", "compose", *args)


def redis(*args, check=True):
    return run("bash", "scripts/redis.sh", *map(str, args), check=check)


def node_cli(node, *args):
    return compose("exec", "-T", node, "redis-cli", "-e", "--raw", "-p", str(NODES[node]), *map(str, args))


def master():
    host, port = redis("--address").split()
    if host not in NODES or int(port) != NODES[host]:
        raise RuntimeError(f"Unexpected Sentinel address: {host}:{port}")
    return host


def api(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request("http://localhost:8080/api" + path, data=data,
                                     headers={"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def wait_for(description, condition, timeout=90):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if condition():
                print("OK:", description, flush=True)
                return
        except Exception as error:
            last_error = error
        time.sleep(1)
    raise RuntimeError(f"Timeout: {description}; last error: {last_error}")


def healthy():
    return api("GET", "/health")["status"] == "UP"


def restored():
    primary = master()
    return "connected_slaves:2" in node_cli(primary, "INFO", "replication") and all(
        "role:slave" in node_cli(node, "INFO", "replication")
        and "master_link_status:up" in node_cli(node, "INFO", "replication")
        for node in NODES if node != primary
    ) and healthy()


def split_brain():
    wait_for("one master, two synchronized replicas", restored)
    primary = master()
    replicas = [node for node in NODES if node != primary]
    key = "demo:guard:" + uuid.uuid4().hex
    print("Pausing current replicas:", replicas, flush=True)
    try:
        compose("pause", *replicas)
        wait_for("master rejects writes with NOREPLICAS", lambda:
        "NOREPLICAS" in redis("SET", key, "blocked", "EX", 60, check=False), timeout=35)
        print(redis("SET", key, "blocked", "EX", 60, check=False), flush=True)
    finally:
        compose("unpause", *replicas)
        wait_for("replication and API restored", restored)
        redis("DEL", key)


def failover():
    wait_for("one master, two synchronized replicas", restored)
    old = master()
    player_id = "demo-" + uuid.uuid4().hex
    api("POST", "/players/" + player_id, {"name": "Before failover", "level": 1, "region": "EU"})
    replicas = [node for node in NODES if node != old]
    wait_for("profile reached both replicas", lambda: all(
        node_cli(node, "HGET", "player:" + player_id, "name") == "Before failover" for node in replicas))
    print("Stopping current master:", old, flush=True)
    started = time.monotonic()
    try:
        compose("stop", "-t", "2", old)
        wait_for("Sentinel selected a new master", lambda: master() != old, timeout=60)
        wait_for("API can write without an application restart", healthy, timeout=60)
        print(f"New master: {master()}; recovery: {time.monotonic() - started:.1f} seconds", flush=True)
        assert api("GET", "/players/" + player_id)["name"] == "Before failover"
        result = api("PATCH", "/players/" + player_id + "/level", {"delta": 1})
        assert result["level"] == 2
        print("OK: old profile survived; new level update succeeded", flush=True)
    finally:
        compose("start", old)
        wait_for("old master rejoined as a replica; two replicas available", restored)


def persistence():
    wait_for("stack is ready", restored)
    key = "demo:persistence:" + uuid.uuid4().hex
    value = uuid.uuid4().hex
    redis("SET", key, value)
    wait_for("marker replicated", lambda: all(node_cli(node, "GET", key) == value for node in NODES))
    wait_for("no background RDB save is running", lambda:
    "rdb_bgsave_in_progress:0" in redis("INFO", "persistence"))
    redis("BGSAVE")
    wait_for("RDB snapshot saved and AOF enabled", lambda: all(token in redis("INFO", "persistence")
                                                               for token in ("rdb_bgsave_in_progress:0", "rdb_last_bgsave_status:ok", "aof_enabled:1")))
    try:
        compose("stop", "app", "sentinel-1", "sentinel-2", "sentinel-3", *NODES)
    finally:
        compose("up", "-d")
    wait_for("stack restored from existing volumes", restored)
    assert redis("GET", key) == value
    redis("DEL", key)
    print("OK: marker survived a complete graceful stack restart", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=["split-brain", "failover", "persistence"])
    arguments = parser.parse_args()
    {"split-brain": split_brain, "failover": failover, "persistence": persistence}[arguments.scenario]()