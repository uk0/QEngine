# Raft acceptance harness

Every scenario here describes a behavior the Raft layer MUST satisfy
before we flip `TSDB_CONSENSUS` default from `fanout` to `raft`.  If a
scenario fails, the bug is in the consensus implementation, not the
test — no weakening.

## Scenarios

| # | Name                              | Property being tested                                                                  |
|---|-----------------------------------|----------------------------------------------------------------------------------------|
| 1 | `s01_elect_leader.sh`             | 3 masters → exactly one leader within 1 s; followers agree on the same leader id      |
| 2 | `s02_ddl_replicates.sh`           | `CREATE DATABASE` on the leader is visible on both followers after the server ACKs     |
| 3 | `s03_kill_leader.sh`              | Kill leader → within 5 s a new leader exists and accepts a fresh DDL                   |
| 4 | `s04_old_leader_returns.sh`       | Dead leader rejoins → its stale log tail is truncated and replaced by the new leader's |
| 5 | `s05_network_partition.sh`        | Minority side rejects writes; majority side keeps accepting; heal → minority catches up |
| 6 | `s06_log_divergence.sh`           | Follower whose log forks mid-term is forced back in line on the next AppendEntries     |
| 7 | `s07_concurrent_ddl_same_name.sh` | Two clients `CREATE DATABASE foo` at the same instant → exactly one succeeds cluster-wide |

## Running

```
cd tests/raft
./runall.sh                 # runs all scenarios, tears down cluster between each
./scenarios/s01_elect_leader.sh     # single scenario for focused debugging
```

Each scenario self-starts a 3-master docker-compose stack under
`/tmp/tsdb-raft-<scenario>/`, runs its assertions, tears down on exit
(via a trap).  Non-zero exit = failure.

## Helper expectations

- `../../build/tsdb-cli` is built (`make`).
- `/root/stress-200m` is not required — scenarios drive via `curl`.
- `jq` must be on PATH (for JSON parsing in assertions).

## Skip list (to update as Raft matures)

While the Raft implementation is in flight, scenarios expected to fail
are listed in `skip.txt` — CI greps that file to avoid spurious red.
When a scenario passes, remove it from `skip.txt`.
