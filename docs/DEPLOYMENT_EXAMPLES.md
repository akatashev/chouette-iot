# Chouette-IoT Deployment Examples

Chouette-IoT was developed to be running on IoT devices like Raspberry Pi or Nvidia Jetson.

Since these days it's pretty handy to use containerization, two main approaches to running Chouette-IoT would be using **Kubernetes** and **Docker-compose**.

Both of them have plugins to track what's going on with containers.

## Kubernetes

Chouette-IoT was tested both on "normal" Kubernetes and [microk8s](https://microk8s.io/).

The main preprequisite for running Chouette-IoT on Kubernetes is Redis. There must be Redis installed and running, because it's used as a broker.

If we suppose that we're using microk8s deployed on Nvidia Nano, Redis is installed, its local hostname is **redis** and its port is a standard one, and we want to collect data about K8s pods, host information and GPU temperature, K8S deployment file example would look like this.
```
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    app: chouette-iot
  name: chouette-iot
spec:
  replicas: 1
  selector:
    matchLabels:
      app: chouette-iot
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        app: chouette-iot
    spec:
      containers:
      - name: chouette-iot
        image: chouette-iot:0.0.1
        command: ["python3", "app.py"]        
        env:
        - name: REDIS_HOST
          value: redis
        - name: REDIS_PORT
          value: '6379'
        - name: API_KEY
          value: <your Datadog API key goes here>
        - name: COLLECTOR_PLUGINS
          value: '["k8s", "host", "tegrastats"]'  # List of plugins to use as a JSON list.
        - name: GLOBAL_TAGS
          value: '["host:nvidia-nano", "environment:Development"]'  # List of tags to send as a JSON list.
        - name: METRICS_WRAPPER
          value: "datadog"
        - name: K8S_KEY_PATH
          value: /chouette/k8s-key.key
        - name: K8S_CERT_PATH
          value: /chouette/k8s-cert.crt
        - name: K8S_STATS_SERVICE_IP
          value: 10.1.18.1  # Your IP can be different, check it with ifconfig.
        - name: TEGRASTATS_METRICS
          value: '["temp"]'  # We want to collect only temperature
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

In this configuration file we specify what plugins should be used to collect metrics from the mode, what tags must be sent with every metric, what **MetricsWrapper** object should be used, where to connect to get K8s data and where cert and key files are placed.

These cert and key files (as well as the tegrastats binary) are added as volumes from the host. Otherwise it won't work.

## Docker-compose

Docker-compose has basically the same prerequisites - it needs Redis to be specified in your `docker-compose.yml` file.

An example of a deployment file that reads stats about the host and about docker images running on it would be like that:
```
version: "3.5"
networks:
  default:
    name: my-network

services:
  redis:
    image: redis:5.0.5

  chouette:
    image: chouette-iot:0.0.1
    links:
      - redis
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - /usr/bin/tegrastats:/usr/bin/tegrastats
    environment:
      - API_KEY=<your Datadog API key goes here>
      - GLOBAL_TAGS=["host:nvidia-nano", "environment:Development"]  # Let's say, we use the same Nano.
      - COLLECTOR_PLUGINS=["host", "tegrastats", "docker"]  # 'k8s' plugin is replaced by 'docker'
      - METRICS_WRAPPER=datadog
    command: python3 app.py

  (other containers with their applications)
```
It needs `docker.sock` and `tegrastats` to be added as volumes as well. If there are other application sending metrics to the same Redis, Chouette-IoT will aggregate them and send them to Datadog.

## No containerization

Surely, it can be run as a standalone application in a screen or something. But it would be a bit more complicated, because it's being controlled via environment variables, and whilst that's pretty handy for containerization, it doesn't work hard for old good "bare metal or VM" ways.