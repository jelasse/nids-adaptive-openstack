#!/usr/bin/env python3
"""Add a second Kafka listener (EXTERNAL, port 9094) so that producers on the laptop can
send into Kafka in the VM. Clients inside the VM keep using localhost:9092.

Usage (in the VM):  sudo python3 kafka_external.py 172.24.4.36 [/opt/kafka/config/server.properties]
A backup of the original file is written next to it (server.properties.bak).
"""
import re, shutil, sys

ip = sys.argv[1]
path = sys.argv[2] if len(sys.argv) > 2 else "/opt/kafka/config/server.properties"
s = open(path).read()


def get(k):
    m = re.search(rf"^{re.escape(k)}=(.*)$", s, re.M)
    return m.group(1).strip() if m else None


def put(k, v):
    global s
    if get(k) is None:
        s = s.rstrip("\n") + f"\n{k}={v}\n"
    else:
        s = re.sub(rf"^{re.escape(k)}=.*$", f"{k}={v}", s, flags=re.M)


L = get("listeners") or "PLAINTEXT://:9092,CONTROLLER://:9093"
A = get("advertised.listeners") or "PLAINTEXT://localhost:9092"
M = get("listener.security.protocol.map") or "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
if "EXTERNAL" not in L:
    put("listeners", L + ",EXTERNAL://:9094")
A = ",".join(x for x in A.split(",") if not x.startswith("EXTERNAL://"))
put("advertised.listeners", A + f",EXTERNAL://{ip}:9094")
if "EXTERNAL" not in M:
    put("listener.security.protocol.map", M + ",EXTERNAL:PLAINTEXT")
shutil.copy(path, path + ".bak")
open(path, "w").write(s)
for k in ["listeners", "advertised.listeners", "listener.security.protocol.map",
          "inter.broker.listener.name", "controller.listener.names"]:
    print(f"{k}={get(k)}")
