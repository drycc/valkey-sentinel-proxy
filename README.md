# Valkey Proxy

Valkey Proxy is a lightweight, fast but powerful Valkey Sentinel Proxy written in Go.

## Usage

### Build

```bash
make build
```

### Command

```bash
# ./_dist/valkey-sentinel-proxy --help
Usage of valkey-sentinel-proxy:
  -listen string
        listen address (default ":9999")
  -master string
        name of the master valkey node
  -max-procs int
        sets the maximum number of CPUs that can be executing (default 1)
  -sentinel-addr string
        comma-separated Sentinel addresses (default ":26379")
  -sentinel-pass string
        password to use when connecting to Sentinel servers
  -sentinel-user string
        username to use when connecting to Sentinel servers
  -node-pass string
        password to use when validating Valkey data nodes
  -node-user string
        username to use when validating Valkey data nodes
  -topology-refresh-interval duration
        interval for reconciling the Sentinel topology (default 5s)
```

Example:

```bash
./_dist/valkey-sentinel-proxy \
  -listen :9999 \
  -master mymaster \
  -sentinel-addr "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379" \
  -sentinel-user "<sentinel-user>" \
  -sentinel-pass "<sentinel-password>" \
  -node-user "<data-node-user>" \
  -node-pass "<data-node-password>"
```

Sentinel credentials and data-node credentials are independent. The proxy does not implicitly reuse Sentinel credentials on data nodes. Leave `node-user` and `node-pass` empty when data nodes allow unauthenticated `ROLE` commands. If data nodes use the same credentials as Sentinel, configure the same values explicitly in both places.

## Architecture

For each new downstream connection, the proxy asks the configured Sentinels for the current primary and establishes a short-lived connection to the candidate to execute `ROLE`. Only a candidate reporting `master` is accepted. The proxy then starts transparent TCP forwarding; it does not route individual commands or replay writes.

The proxy subscribes to Sentinel `+switch-master` events and reconciles the topology every 5 seconds by default. Events are triggers for revalidation, not authoritative routing decisions. When a new primary is confirmed, sessions connected to the old address are closed. A session whose upstream is explicitly found to be a replica is also closed even if no replacement primary is available yet. Temporary Sentinel or validation failures alone do not cause mass disconnection.

Clients must enable automatic reconnection. On reconnection, the proxy resolves and validates the primary again. Existing connection state such as authentication, selected DB, transactions, and subscriptions is not migrated. The additional validation connection adds one short-lived data-node connection per new client connection.

### Split-brain limitations

`ROLE` cannot identify an old primary that still reports `master`, so this proxy does not claim to prevent split-brain writes or data loss. Use at least three Sentinels in independent failure domains and consider `min-replicas-to-write`, `min-replicas-max-lag`, and external fencing where required. Valkey replication is asynchronous; acknowledged writes can still be lost during failover.

### Failover integration test

With Podman and a Valkey container image available, run:

```bash
make test-failover
```

Set `PODMAN` or `VALKEY_IMAGE` to override the container runtime or image. The test starts real data nodes and Sentinels, verifies old-session disconnection and reconnection to the new primary, checks rejection of a known replica, and cleans up its containers and proxy process.

## Performance

Redis includes the redis-benchmark utility that simulates running commands done by N clients at the same time sending M total queries (it is similar to the Apache's ab utility). Below you'll find the full output of a benchmark executed against a Linux box.

The following options are supported:

```bash
Usage: redis-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests]> [-k <boolean>]

 -h <hostname>      Server hostname (default 127.0.0.1)
 -p <port>          Server port (default 6379)
 -s <socket>        Server socket (overrides host and port)
 -a <password>      Password for Redis Auth
 --user <username>  Used to send ACL style 'AUTH username pass'. Needs -a.
 -c <clients>       Number of parallel connections (default 50)
 -n <requests>      Total number of requests (default 100000)
 -d <size>          Data size of SET/GET value in bytes (default 3)
 --dbnum <db>       SELECT the specified db number (default 0)
 --threads <num>    Enable multi-thread mode.
 --cluster          Enable cluster mode.
 --enable-tracking  Send CLIENT TRACKING on before starting benchmark.
 -k <boolean>       1=keep alive 0=reconnect (default 1)
 -r <keyspacelen>   Use random keys for SET/GET/INCR, random values for
                    SADD, random members and scores for ZADD.
                    Using this option the benchmark will expand the string
                    __rand_int__ inside an argument with a 12 digits number
                    in the specified range from 0 to keyspacelen-1. The
                    substitution changes every time a command is executed.
                    Default tests use this to hit random keys in the
                    specified range.
 -P <numreq>        Pipeline <numreq> requests. Default 1 (no pipeline).
 -e                 If server replies with errors, show them on stdout.
                    (No more than 1 error per second is displayed.)
 -q                 Quiet. Just show query/sec values
 --precision        Number of decimal places to display in latency output (default 0)
 --csv              Output in CSV format
 -l                 Loop. Run the tests forever
 -t <tests>         Only run the comma separated list of tests. The test
                    names are the same as the ones produced as output.
 -I                 Idle mode. Just open N idle connections and wait.
 --help             Output this help and exit.
 --version          Output version and exit.
```

You need to have a running Redis instance before launching the benchmark. A typical example would be:

```bash
redis-benchmark -p 8088 -c 500 -n 5000000 -P 100 -r 10000 -t get,set
```

Using this tool is quite easy, and you can also write your own benchmark, but as with any benchmarking activity, there are some pitfalls to avoid.

## Development

The Drycc project welcomes contributions from all developers. The high-level process for development matches many other open source projects. See below for an outline.

* Fork this repository
* Make your changes
* [Submit a pull request][prs] (PR) to this repository with your changes, and unit tests whenever possible.
  * If your PR fixes any [issues][issues], make sure you write Fixes #1234 in your PR description (where #1234 is the number of the issue you're closing)
* Drycc project maintainers will review your code.
* After two maintainers approve it, they will merge your PR.

[prs]: https://github.com/drycc/valkey-sentinel-proxy/pulls
[issues]: https://github.com/drycc/valkey-sentinel-proxy/issues
