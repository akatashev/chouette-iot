# Chouette-IoT

[![Chouette-IoT](https://img.shields.io/badge/version-0.0.2-blue.svg)](https://github.com/akatashev/chouette-iot)
[![Python 3.6+](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/)
[![Pypy 3.6](https://img.shields.io/badge/pypy-3.6-blue.svg)](https://www.pypy.org/)
[![CircleCI](https://circleci.com/gh/akatashev/chouette-iot/tree/dev.svg?style=svg)](https://app.circleci.com/pipelines/github/akatashev/chouette-iot)


**Chouette** is a [Pykka](https://www.pykka.org/) based **Datadog** compatible modular metrics collection agent and system monitoring solution for IoT solutions based on such devices as Raspberry Pi or Nvidia Jetson.

It uses Redis as a broker for metrics from other applications and as a storage for ready to dispatch metrics. Periodically it tries to dispatch data to Datadog. If it doesn't happen for some reason, metrics are being preserved in their queue to be dispatched later, when connectivity is restored or Chouette is redeployed with correct configuration.

Applications should use [Chouette-IoT-client](https://github.com/akatashev/chouette-iot-client) library to successfully send metrics via Chouette-Iot.

## Metrics collection agent?

Chouette is expected to run along with Redis and monitored applications in a system handled by **Kubernetes** (e.g. [microk8s](https://microk8s.io/)) or **Docker-compose**. It can be run in other environments, but it was designed to be used as a container in some kind of an orchestrator.

Monitored applications, using **chouette-client** put their metrics into Redis to be aggregated and processed.

Every 10 seconds (*by default*) Chouette collects all the raw metrics from their queues and [aggregates](https://docs.datadoghq.com/developers/dogstatsd/data_aggregation/) them.

Aggregation means that metrics of the same type produced during the same period of time are being merged together and then their value is being used to calculate values for a set of metrics that is being sent to Datadog.

Chouette calls this process of transforming raw metrics to ready to dispatch metrics **wrapping**. It uses **MetricWrapper** objects to do this. By using custom **wrappers** a user is able to cast raw metrics into any number and kind of actual Datadog metrics.

Every 60 seconds (*by default*) a component named MetricsSender gets ready to dispatch metrics from a queue, compresses them and sends them to Datadog.

## System monitoring solution?

Since IoT devices also needs monitoring especially if they run heavy AI computations, it seems handy to let a metric collector also be a metrics producer.

Chouette uses **CollectorPlugins** to gather data about the node where it's running. Every 30 seconds (*by default*) it collects stats from all its plugins and stores this data as ready to dispatch metrics. On the next successful connection to Datadog these metrics are dispatched and are able to be seen on your Datadog dashboard.

Chouette philosophy is "*send as little data as possible*", so it's expected that plugins return only the minimal necessary amount of data.  Datadog dashboards are quite powerful and they provide a possibility to calculate lots of useful things. 

E.g.: There is no need to send total amount of RAM and percentage of used RAM, both these values can be easily calculated by using absolute value of used RAM and available RAM.

**Examples of CollectorPlugins:**
* HostStatsCollector - collects data about system RAM, filesystem and CPU usage.
* K8sCollector - collects data about pods RAM and CPU usage. Optionally - node related data.
* TegrastatsCollector - collects data about Nvidia Jetson (like GPU temperature) using Nvidia's [tegrastats](https://docs.nvidia.com/jetson/l4t/index.html#page/Tegra%2520Linux%2520Driver%2520Package%2520Development%2520Guide%2FAppendixTegraStats.html%23) Utility.

Plugins have their README.md with detailed description in the folder `chouette/metrics/plugins`.

## Configuration

Chouette should be configured via environment variables. Most of orchestrators make it simple to do it in their deployment files.

**CollectorPlugins** can have their own environment variables, but general Chouette environment variables are described in the `chouette/_configuration.py` file.

They are:
* **API_KEY**: Datadog API key used by Datadog to authenticate you. 
* **GLOBAL_TAGS**: List of tags that you want to send along with every metric. E.g.: `["host:my-iot-device", "location:London"]`.
* **COLLECT_PLUGINS**: List of collector plugins that Chouette should use to collect metrics. Empty by default. If you don't specify anything, it won't collect any metrics. E.g.: `["host", "k8s"]`.
* **AGGREGATE_INTERVAL**: How often raw metrics should be aggregated. Default value is 10 for 10 seconds just like in Datadog Agent's "flush interval".
* **CAPTURE_INTERVAL**: How often Chouette should collect stats from its plugins. Default value is 30.
* **DATADOG_URL**: By default `https://api.datadoghq.com/api`, but if you have your own small Datadog, you can change it!
* **LOG_LEVEL**: INFO by default, however most of the interesting stuff is hidden in DEBUG which can be too noisy.
* **METRICS_BULK_SIZE**: Maximum amount of metrics Chouette will try to collect every dispatching attempt. By default it's `10000`. It should be fine not only to handle normal minutely pace, but also to recover relatively fast after a period of lost connectivity.
* **METRIC_TTL**: Metric Time-To-Live in seconds. Datadog rejects outdated metrics if their timestamp is older than 4 hours. So there is no sense in spending traffic on them. Therefore before every dispatch attempt outdated metrics are being cleaned. It's default value is 14400 for 4 hours. It can be decreased if you don't care about what happened during connectivity problems.
* **METRICS_WRAPPER**: Name of a metrics wrapper to use. Default is `datadog`. Another option is `simple` or any other that you implement yourself. Just don't forget to add it to the `WrappersFactory` class in `chouette/metrics/wrappers/__init__.py`.
* **RELEASE_INTERVAL**: How often Chouette should dispatch compressed metrics to Datadog. Default value is 60.
* **SEND_SELF_METRICS**: Whether Chouette should also send its owl metrics like an amount of sent bytes and number of sent metrics. By default `True`.

## Documentation

Chouette documentation is available [here](https://github.com/akatashev/chouette-iot/tree/dev/docs).

## License
Chouette-IoT is licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).