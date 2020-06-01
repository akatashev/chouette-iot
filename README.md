# Chouette-iot

**Chouette** is a [Pykka](https://www.pykka.org/) based **Datadog** compatible modular metrics collection agent and system monitoring solution for IoT solutions based on such devices as Raspberry Pi or Nvidia Jetson.

It uses Redis as a broker for metrics from other applications and as a storage for ready to dispatch metrics. Periodically it tries to dispatch data to Datadog. If it doesn't happen for some reason, metrics are being preserved in their queue to be dispatched later, when connectivity is restored or Chouette is redeployed with correct configuration.

## Metrics collection agent?

Chouette is expected to run along with Redis and monitored applications in a system handled by **Kubernetes** (e.g. [microk8s](https://microk8s.io/)) or **Docker-compose**. It can be run in other environments, but it was designed to be used as a container in some kind of an orchestrator.

Monitored applications, using **chouette-client** put their metrics into Redis to be aggregated and processed.

Every 10 seconds (*by default*) Chouette collects all the raw metrics from their queues and [aggregate](https://docs.datadoghq.com/developers/dogstatsd/data_aggregation/) them.

Aggregation means that metrics of the same type produced during the same period of time are being merged together and then their value is being used to calculate values for a set of metrics that is being sent to Datadog.

Chouette calls this process of transforming raw metrics to ready to dispatch metrics **wrapping**. It uses **MetricWrapper** objects to do this. By using custom **wrappers** a user is able to cast raw metrics into any number and kind of actual Datadog metrics.

Every 60 seconds (*by default*) a component named MetricsSender gets ready to dispatch metrics from a queue, compresses them and sends them to Datadog.

### MetricWrappers

While the standard Datadog [metric types](https://docs.datadoghq.com/developers/metrics/types/) are pretty nice, sometimes you probably want to perform some custom data transformation on the device side and send the minimal possible amount of data to Datadog.

That's where custom MetricWrappers can be used.

E.g.: **SimpleWrapper** object knows only two kinds of metrics:
* Count - that is absolutely the same as in standard Datadog.
* Gauge - that is actually not `gauge` from Datadog description, but is an **average** value of the metrics emitted during the aggregation interval.

Every `count` metric sends a single Datadog metric of type `count`. 

Every `gauge` metrics sends one Datadog metric of type `gauge` that contains an average value of aggregated metrics and one Datadog metric of type `count` that tells how many metrics were used to calculate that average value.

All other kinds of metrics are considered being a `gauge` metric.

It violates metric types descriptions, but it can be handy in a sense of sending only the data that you need and not a single unnecessary metric.

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
* **GLOBAL_TAGS**: List of tags that you want to send along with every metric. E.g.: `'["host:my-iot-device", "location:London"]'`.
* **COLLECT_PLUGINS**: List of collector plugins that Chouette should use to collect metrics. Empty by default. If you don't specify anything, it won't collect any metrics. E.g.: `'["host", "k8s"]'`.
* **AGGREGATE_INTERVAL**: How often raw metrics should be aggregated. Default value is 10 for 10 seconds just like in Datadog Agent's "flush interval".
* **CAPTURE_INTERVAL**: How often Chouette should collect stats from its plugins. Default value is 30.
* **DATADOG_URL**: By default `https://api.datadoghq.com/api`, but if you have your own small Datadog, you can change it!
* **LOG_LEVEL**: INFO by default, however most of the interesting stuff is hidden in DEBUG which can be too noisy.
* **METRICS_BULK_SIZE**: Maximum amount of metrics Chouette will try to collect every dispatching attempt. By default it's `10000`. It should be fine not only to handle normal minutely pace, but also to recover relatively fast after a period of lost connectivity.
* **METRIC_TTL**: Metric Time-To-Live in seconds. Datadog rejects outdated metrics if their timestamp is older than 4 hours. So there is no sense in spending traffic on them. Therefore before every dispatch attempt outdated metrics are being cleaned. It's default value is 14400 for 4 hours. It can be decreased if you don't care about what happened during connectivity problems.
* **METRICS_WRAPPER**: Name of a metrics wrapper to use. Default is `datadog`. Another option is `simple` or any other that you implement yourself. Just don't forget to add it to the `WrappersFactory` class in `chouette/metrics/wrappers/__init__.py`.
* **RELEASE_INTERVAL**: How often Chouette should dispatch compressed metrics to Datadog. Default value is 60.
* **SEND_SELF_METRICS**: Whether Chouette should also send its owl metrics like an amount of sent bytes and number of sent metrics. By default `True`.

## Microk8s example deployment file

If we suppose, there is a Nvidia Nano device that is running microk8s that has a Redis service whos hostname of other pods is `redis`, application deployment file can look like this:
```
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    app: chouette
  name: chouette
spec:
  replicas: 1
  selector:
    matchLabels:
      app: chouette
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        app: chouette
    spec:
      containers:
      - name: chouette
        image: localhost:32000/chouette:latest
        command: ["python3", "app.py"]        
        env:
        - name: REDIS_HOST
          value: redis
        - name: REDIS_PORT
          value: '6379'
        - name: API_KEY
          value: 1234567890abc
        - name: COLLECTOR_PLUGINS
          value: '["k8s", "host", "tegrastats"]'
        - name: GLOBAL_TAGS
          value: '["host:nvidia-nano", "environment:Development"]'
        - name: METRICS_WRAPPER
          value: "datadog"
        - name: K8S_KEY_PATH
          value: /chouette/k8s-key.key
        - name: K8S_CERT_PATH
          value: /chouette/k8s-cert.crt
        - name: K8S_STATS_SERVICE_IP
          value: 10.1.18.1
        imagePullPolicy: Always
        volumeMounts:
        - mountPath: /chouette/k8s-cert.crt
          name: k8s-cert
        - mountPath: /chouette/k8s-key.key
          name: k8s-key
        - mountPath: /usr/bin/tegrastats
          name: tegrastats
      imagePullSecrets:
      - name: development-ecr-registry
      volumes:
      - hostPath:
          path: /var/snap/microk8s/current/certs/server.crt
        name: k8s-cert
      - hostPath:
          path: /var/snap/microk8s/current/certs/server.key
        name: k8s-key
      - hostPath:
          path: /usr/bin/tegrastats
        name: tegrastats
```
This deployment file creates a Deployment with a single chouette pod.

`Volumes` part describes volumes necessary for `k8s` and `tegrastats` plugins.  
Both `k8s-key` and `k8s-cert` are used by `K8sCollector` to collect data from K8s Stats Service.  
`tegrastats` volume is used by `TegrastatsCollector`.

## License
Chouette is licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).