# Chouette-iot Collection Plugins

Collections Plugins collect stats from different sources and turn them into useful metrics.

The main idea of these Plugins is to collect *as little* data as necessary.

Some plugins provide the same data (e.g.: both **HostStatsCollector** and **TegrastatsCollector** return RAM), but it should be possible to choose what data should be sent to avoid sending duplicated data from different sources.

Almost every plugin has its `METRICS` environment variable to determine what data should be collected and dispatched. Every plugin collects not everything that it could, but just data that originally was expected to be useful. Plugins can and possibly should be extended.

Most of the metrics are `gauge`, because for most of the stats we care about the latest, actual value.

## HostStats Collector

*Label*: `host`  
*Purpose*: Collects stats about host system via Python's `psutil` package.

HostStatsCollector collects data about system RAM, disk usage and CPU usage.

It has an environment variable `HOST_COLLECT_METRICS` that defines what stats it should collect.  
Its default value is `["cpu", "fs", "la", "ram"]`.

* `cpu` collects CPU Usage percentage and returns a single `Chouette.host.cpu.percentage` metric.
* `fs` collects data about filesystems usage. For every device that `psutil` can find, it returns 2 metrics: `Chouette.host.fs.used` and `Chouette.host.fs.free`. Their values are in bytes. They can be easily used to calculate used space percentage, for example. Sometimes Docker tricks psutil and tells it, that the same device is mounted more than once to different mountpoints. Plugin doesn't handle this situation, so if the same device is mounted twice, two sets of identic metrics will be sent.
* `ram` collects data about RAM usage. It sends two metrics: `Chouette.host.memory.used` and `Chouette.host.memory.available`. Percentage and total amount of memory can be easily calculated using this data.
* `la` collects Load Average values and sends the same metric `Chouette.host.la` with three different tags: `period:1m`, `period:5m`, `period:15m`. 

`network` stats are not being collected, because them seem pretty useless, but it can always be changed.

## Tegrastats Collector

*Label*: `tegrastats`  
*Purpose*: Collect stats about Jetson device via command line Tegrastats utility.

TegrastatsCollector gets data about RAM and temperature. It doesn't collect data about CPU usage, because HostStatsCollector data seems to be more accurate.

It has an environment variable `TEGRASTATS_METRICS` that defines what stats it should collect.  
Its default value is `["ram", "temp"]`.

* `ram` data is expected to be less accurate than what HostStatsCollector provides. Tegrastats returns data in MBs, so actual value is an approximation of what it could be in bytes. It returns two metrics: `Chouette.tegrastats.ram.used` and `Chouette.tegrastats.ram.free`.
* `temp` returns information about device temperature. Different Jetson devices return information about different zones. Plugin takes all the zones, throws away `PMIC` some and returns a single metric `Chouette.tegrastats.temperature` with a `zone` tag. E.g.: `["zone:gpu"]` provides data about GPU temperature.

## Dramatiq Collector (for Redis Broker)

*Label*: `dramatiq`  
*Purpose*: Collect sizes of Dramatiq queues.

Actual version of this plugin works only for Dramatiq whose broker is Redis. Probably it's possible to modify it to be able to handle RabbitMQ as well.

It has no environment variables, because it collects the only metric: `Chouette.dramatiq.queue_size`. It takes all the hashes in Redis whose names match the pattern `dramatiq:*.msgs` and return their values in tags. Queue name in a tag is being stripped of both `dramatiq:` prefix and `.msgs` postfix. So for a queue `dramatiq:pizza.msgs` this plugin will generate a metric with tags `["queue:pizza"]`.

## K8s Collector

*Label*: `k8s`  
*Purpose*: Mainly to monitor K8s pods state. There is an optional `node` metrics option, but HostStatsCollector data seems to be more accurate anyway.

It connects to a Stats Service using a URL `https://<K8S_STATS_SERVICE_IP>:<K8S_STATS_SERVICE_PORT>/stats/summary`, parses its output and wraps it into metrics.  
This plugin was tested both with normal K8s installation and with microk8s.

It has the following environment variables:
* `K8S_STATS_SERVICE_IP` is normally the IP of the node, it could be an external IP (e.g. `192.168.1.104`) or an internal IP (e.g. `10.1.18.1`). It's better to use an internal IP if your connectivity isn't too good. because if you rely on your external IP and connection disappears, the plugin will stop collecting metrics. In case of an internal IP that won't happen.
* `K8S_STATS_SERVICE_PORT` is a Stats Service's port. By default it's `10250`.
* `K8S_CERT_PATH` is a path to a client certificate to pass the Stats Server authorization. In microk8s on the node it's usually `/var/snap/microk8s/current/certs/server.crt`. But this value really depends on this cert is represented in Choette's container.
* `K8S_KEY_PATH` is a path to a client key to pass the Stats Server authorization. In microk8s on the node it's usually `/var/snap/microk8s/current/certs/server.key`.
* `K8S_METRICS` is a list of metrics to collect. By default it's just pods, because node information can be collected via other plugins and it's likely to be more accurate.

K8sCollector returns quite a lot of metrics, so it's easier to check its source file. Both node and pods metrics are actually only subsets of what K8s returns from its Stats Service, so it's quite expandable.