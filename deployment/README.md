# tsdb — deployment

Docker-based deployments for tsdb clusters and multi-cluster federations.

## Files

| File | Purpose |
|------|---------|
| `Dockerfile` | Two-stage Alpine build producing ≤ 20 MB runtime image |
| `entrypoint.sh` | Role selector: `server` / `cluster-node` / `cli` |
| `docker-compose.yml` | Single 3-node cluster on a bridge network |
| `docker-compose.federation.yml` | Two 3-node clusters + federator |
| `k8s/` *(planned)* | StatefulSet manifests for Kubernetes |

## Single-node development

```bash
docker build -t tsdb:dev -f deployment/Dockerfile .
docker run -d --name tsdb-dev \
    -p 28090:28090 \
    -v tsdb-dev-data:/var/lib/tsdb \
    -e TSDB_ROLE=server \
    tsdb:dev

# connect
docker run --rm -it --network host tsdb:dev \
    tsdb-cli --host 127.0.0.1 --port 28090
```

## Three-node cluster

```bash
cd deployment
docker compose up -d

# watch gossip convergence (node1 should see 3 alive members within 5s)
docker compose logs -f node1 | grep -i gossip

# connect to any node
docker compose exec node1 tsdb-cli --host node1 --port 28090 <<'EOF'
CREATE GROUP factory_a (region='us-east-1', retention='30d');
CREATE DEVICE sensor_001 IN GROUP factory_a (type='temperature');
LIST DEVICES IN GROUP factory_a;
EOF

# the write above was quorum-replicated — read from a different node
docker compose exec node3 tsdb-cli --host node3 --port 28090 <<'EOF'
LIST DEVICES IN GROUP factory_a;
EOF

# inspect cluster state
docker compose exec node1 tsdb-cli --host node1 --port 28090 <<'EOF'
STATS;
EOF

# graceful shutdown of one node — others stay W=2 writable
docker compose stop node2
docker compose exec node1 tsdb-cli --host node1 --port 28090 <<'EOF'
CREATE DEVICE sensor_002 IN GROUP factory_a (type='pressure');
EOF
# restart and verify catch-up via rawblock replication
docker compose start node2
sleep 5
docker compose exec node2 tsdb-cli --host node2 --port 28090 <<'EOF'
LIST DEVICES IN GROUP factory_a;
EOF
```

### Port map

| Host port | Container | Purpose |
|-----------|-----------|---------|
| 28091 | node1:28090 | client/TCP |
| 28092 | node2:28090 | client/TCP |
| 28093 | node3:28090 | client/TCP |
| — | *:28081 | cluster RPC (internal only) |
| — | *:28080/udp | gossip (internal only) |

RPC and gossip are **not** exposed to the host — they stay on the
`tsdb-net` bridge network. If you need to debug across docker hosts,
expose them explicitly and set `TSDB_SEEDS` to the public address.

## Federated two-cluster deployment

```bash
cd deployment
docker compose -f docker-compose.federation.yml up -d

# write to east
docker compose -f docker-compose.federation.yml exec east-1 \
    tsdb-cli --host east-1 --port 28090 <<'EOF'
CREATE GROUP api_servers (region='us-east-1');
CREATE DEVICE api_01 IN GROUP api_servers (type='pod');
EOF

# write to west
docker compose -f docker-compose.federation.yml exec west-1 \
    tsdb-cli --host west-1 --port 28090 <<'EOF'
CREATE GROUP api_servers (region='us-west-2');
CREATE DEVICE api_99 IN GROUP api_servers (type='pod');
EOF

# federated query — sees both clusters
docker compose -f docker-compose.federation.yml exec federator \
    tsdb-cli --host east-1 --port 28090 <<'EOF'
LIST DEVICES;
EOF
```

## Production tuning

### Image size and architecture

The default `ARCH="-O3"` builds a generic x86-64 / aarch64 binary that
runs on any Alpine-compatible CPU. For known-homogeneous fleets, rebuild
with micro-architecture tuning:

```bash
docker build -f deployment/Dockerfile \
    --build-arg ARCH="-O3 -march=x86-64-v3" \
    -t tsdb:x86-v3 .
```

AVX-512 kernels use `__attribute__((target("avx512f")))` and are
safe on non-AVX-512 CPUs (the runtime dispatcher skips them).

### Persistent volumes

Each node's data directory is ≈ 1 MB per 1 M rows at the average
compression ratio. The default compose volumes are named so `docker
compose down` (without `-v`) preserves data.

For production, mount a dedicated fast block device:

```yaml
volumes:
  node1-data:
    driver: local
    driver_opts:
      type: ext4
      device: /dev/nvme0n1p1
      o: noatime,discard
```

### Resource limits

Recommended minimum per node at 1 M rows/s sustained:

```yaml
deploy:
  resources:
    limits:  { memory: 2G, cpus: "2.0" }
    reservations: { memory: 512M, cpus: "0.5" }
```

### Auto-balance tuning

```yaml
environment:
  TSDB_BALANCE_ALPHA:       0.6      # write-rate weight
  TSDB_BALANCE_BETA:        0.4      # storage weight
  TSDB_BALANCE_DAMPEN:      0.5      # max VN adjustment
  TSDB_BALANCE_INTERVAL_MS: 30000    # rebalance period
```

### Observability

There is no Prometheus exporter yet (planned). For now, use
`STATS;` via the CLI or `tsdb_cluster_stats_str()` from the C API.
Log output goes to stdout and is collected by docker's logging driver.

## Kubernetes *(planned)*

A `StatefulSet`-based manifest with a headless service for gossip and a
per-pod client service is planned for v0.6. The current Dockerfile is
k8s-ready (read-only root FS compatible, non-root user, clean shutdown
on SIGTERM via tini).

## Troubleshooting

**Nodes don't discover each other.** Verify the seed addresses resolve
inside the containers (`docker compose exec node2 nslookup node1`) and
that the cluster RPC port is reachable (`nc -z node1 28081`). Gossip
uses UDP — some overlay networks drop UDP; switch to the `bridge`
driver if you see this.

**Writes hang with W=2.** Check that at least two nodes show `ALIVE`
in `STATS`. If one is `SUSPECT`, the RPC port is unreachable.

**Raw-block replication off.** Currently enabled by default. To
disable for debugging, set `TSDB_REPLICATION_MODE=row` on the primary.

**Clock skew across nodes.** tsdb tolerates moderate skew (1 s); for
production, run NTP/chrony. Clock regressions larger than the partition
size (1 day by default) can confuse block-skip pruning.
