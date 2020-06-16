# Chouette-IoT Documentation

Chouette-IoT is developed with a **DIY** philosophy in mind. Whilst it can be used as is, as a package, it was intended to be a modular, easily expandable system.

IoT world brings lots of challenges and there is lots of data to collect in different projects and experiments. To address this need of a flexibility, Chouette has a concept of **Collector Plugins** - actors, collecting useful data and wrapping it into a metric that can be sent to Datadog.

These plugins should follow Unix philosophy - they should collect data from one source only. Examples of such plugins are **HostStatsCollector** that collects data about a host using a *psutil* Python package or **K8sCollector** that collects data about Kuberentes pods, using K8s Stats Service. Another example (yet not implemented) could be a plugin that collects temperature from external Raspberry Pi USB thermometer.

Another useful Chouette concept is a **MetricsWrapper**. That's an object that decides how to handle metrics received from other applications. E.g. A single *HISTOGRAM* metric normally sends to Datadog 5 actual metrics: count, avg, max, median and 95percentile. This can be modified by environment variables, but what if you want to send just a single Datadog metric for a single raw histogram metric type? In this case you can create your own custom MetricsWrapper that wraps it any way that you like. To get deeper into this approach you can take a look at the **SimpleWrapper** object.

### Links:
1. **Deployment examples** can be found [here](./DEPLOYMENT_EXAMPLES.md).
2. Chouette **application design** is described [here](./DESIGN.md).
3. Information about available metrics **Collector Plugins** can be found [here](./COLLECTOR_PLUGINS.md).

