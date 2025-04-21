---
title : "Running in a Kubernetes cluster"
date: 2021-03-08T00:00:00Z
layout: docs
menu:
  docs:
    parent: "infrastructure-guides"
weight: 310
---

This guide will show you how to run Litestream as a sidecar to your application
within [Kubernetes][] using [StatefulSets][]. Setting up & managing Kubernetes is a
complex topic that will not be covered here. This guide assumes you are
comfortable with Kubernetes and have a running cluster.

It also assumes you have a replica destination (e.g. S3) for your Litestream
data already setup and you are comfortable with basic operations in Litestream.


For an introduction to Litestream, please see the [Getting Started](/getting-started)
tutorial.

{{< alert icon="⏱" text="This should take approximately 20 minutes to complete." >}}

[kubernetes]: https://kubernetes.io/
[StatefulSets]: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/


## Overview

Kubernetes is a popular orchestration platform for deploying services. Services
are typically stateless, however, Litestream can only be run on a single node at
a time. Kubernetes offers a StatefulSet construct which we can leverage to
ensure that we only have one node at a time.

We'll walk through several steps to integrate Litestream into your application:

1. Configure secrets to pass in S3 credentials to Litestream.
2. Create a configmap to pass in the configuration file to Litestream.
3. Create a StatefulSet with a [persistent volume claim (PVC)][pvc] to run our
   application and Litestream in a single pod.

For this tutorial, we'll use a toy application called [myapp][] that simply
runs a web server on port `8080` and persists a count of requests to a SQLite
database. You can replace this with your own application.

[myapp]: https://github.com/benbjohnson/myapp
[pvc]: https://kubernetes.io/docs/concepts/storage/persistent-volumes/


## Configure secrets for AWS credentials

Litestream requires credentials to connect to AWS S3. You can provide them to
your Litestream container by creating a secret. Update the command to include
your specific credentials.

```sh
kubectl create secret generic litestream \
    --from-literal=LITESTREAM_ACCESS_KEY_ID="..." \
    --from-literal=LITESTREAM_SECRET_ACCESS_KEY="..."
```


## Create a ConfigMap for Litestream configuration

It's recommended to run Litestream using a [configuration file][config] for
flexibility. We'll pass this configuration file in by using a Kubernetes
[ConfigMap][].

First, create a file called `litestream.yml` locally. Update the `YOURBUCKET`
text in configuration file to your S3 bucket name. This configuration file
is set up to replicate a database at `/var/lib/myapp/db` to a remote S3 bucket.

```yml
dbs:
  - path: /var/lib/myapp/db
    replicas:
      - url: s3://YOURBUCKET/db
```

Next, we'll need to add this as a ConfigMap in our Kubernetes cluster:

```sh
kubectl create configmap litestream --from-file=litestream.yml
```

[config]: /reference/config
[ConfigMap]: https://kubernetes.io/docs/concepts/configuration/configmap/


## Create a StatefulSet

### Ensure single replica

First, it's important that your application only runs on a single node at a
time. Litestream does not currently support multiple live replicas, however, that
is an upcoming feature.

To only run a single node, we'll set the `spec.replicas` field to `1` on our
StatefulSet:

```yml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: myapp
spec:
  selector:
    matchLabels:
      app: myapp
  serviceName: myapp

  replicas: 1
```

StatefulSet pods are numbered incrementally so your pod name will always be `myapp-0`.


### Using a PVC

Although not required, it's recommended that pods using SQLite & Litestream run
with a persistent volume claim (PVC). This provides better durability
guarantees in the event that a pod crashes and Litestream may have a small
buffer of data that has not been replicated yet.

We'll create a PVC template on our StatefulSet named `data` by setting the
`spec.volumeClaimTemplates` property:

```yml
volumeClaimTemplates:
- metadata:
    name: data
  spec:
    accessModes: ["ReadWriteOnce"]
    resources:
      requests:
        storage: 100Mi
```

### Pass in Litestream configuration

The `litestream.yml` configuration that we passed in earlier can be mapped into
our containers by creating a volume from it. We'll set the `spec.template.spec.volumes`
property on our StatefulSet:

```yml
volumes:
- name: configmap
  configMap:
    name: litestream
```


### Automatic recovery

When our application starts up for the first time, it will automatically create
a new SQLite database at `/var/lib/myapp/db`. However, if our pod and underlying
storage volume fail then we want to first restore our database from our replica
before starting our application.

We can have this restore check occur automatically before our application starts
by running it in a Kubernetes [init container][]. We'll configure it in our
StatefulSet by setting the `spec.template.spec.initContainers` to:

```yml
initContainers:
- name: init-litestream
  image: litestream/litestream:0.3
  args: ['restore', '-if-db-not-exists', '-if-replica-exists', '/var/lib/myapp/db']
  volumeMounts:
  - name: data
    mountPath: /var/lib/myapp
  - name: configmap
    mountPath: /etc/litestream.yml
    subPath: litestream.yml
  env:
  - name: LITESTREAM_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: litestream
        key: LITESTREAM_ACCESS_KEY_ID
  - name: LITESTREAM_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: litestream
        key: LITESTREAM_SECRET_ACCESS_KEY
```

This `initContainers` block does several things. First, it uses the [official
Litestream Docker image][image] and executes the following command:

```sh
litestream restore -if-db-not-exists -if-replica-exists -v /var/lib/myapp/db
```

This restores the database `/var/lib/myapp/db` specified in our config file,
however, it will skip the restore if the database already exists or if there
are no replicas. It also enables verbose logging.

Next, it specifies volume mounts to the data directory from our PVC and to the
configuration directory from our ConfigMap.

Finally, it sets the environment variables for our access key ID & secret access
key. You could also use the `AWS_` prefixed environment variables instead of
`LITESTREAM_` prefixed variables.

[init container]: https://kubernetes.io/docs/concepts/workloads/pods/init-containers/
[image]: https://hub.docker.com/r/litestream/litestream


### Your application container

Our example application, `myapp`, simply needs to make port `8080` available
and mount the data directory that it shares with Litestream. We can set the
`spec.template.spec.containers` property on our StatefulSet:

```yml
containers:
- name: myapp
  image: benbjohnson/myapp:latest
  ports:
  - name: http
    containerPort: 8080
  volumeMounts:
  - name: data
    mountPath: /var/lib/myapp
```

### Litestream application container

Litestream will run as a sidecar to our application and monitor the shared data
directory. Most of the configuration is the same as the init container specified
above except this time we will be running the `litestream replicate` command.

You can specify Litestream as the second container in the
`spec.template.spec.containers` property on our StatefulSet:

```yml
- name: litestream
  image: litestream/litestream:0.3
  args: ['replicate']
  volumeMounts:
  - name: data
    mountPath: /var/lib/myapp
  - name: configmap
    mountPath: /etc/litestream.yml
    subPath: litestream.yml
  env:
  - name: LITESTREAM_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: litestream
        key: LITESTREAM_ACCESS_KEY_ID
  - name: LITESTREAM_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: litestream
        key: LITESTREAM_SECRET_ACCESS_KEY
  ports:
  - name: metrics
    containerPort: 9090
```

Additionally, we open port `9090` to provide Prometheus metrics. 


### Applying changes

The full Kubernetes configuration for our StatefulSet & Service can be found
at [litestream-k8s.yml][]. We can deploy this on our Kubernetes cluster:

```sh
kubectl apply -f litestream-k8s.yml
```

[litestream-k8s.yml]: https://raw.githubusercontent.com/benbjohnson/litestream.io/main/content/guides/kubernetes/litestream-k8s.yml


## Using our application

Exposing our service from Kubernetes is outside the scope of this guide so we'll
test our replication from within the pod using `kubectl exec`.

### Initial startup

First, you should be able to see your pod running:

```sh
$ kubectl get pods
NAME      READY   STATUS    RESTARTS   AGE
myapp-0   2/2     Running   0          1m
```

Next, we can cURL our `myapp` web server by running:

```sh
kubectl exec -it -c myapp myapp-0 -- curl localhost:8080
This server has been visited 1 times.
```

Each time we run this command, the counter will increase by one.

### Deleting the pod

We can see that it's persisted to our PVC by deleting our pod and running it
again:

```sh
kubectl delete pod myapp-0
pod "myapp-0" deleted
```

```sh
kubectl exec -it -c myapp myapp-0 -- curl localhost:8080
This server has been visited 2 times.
```

### Deleting the PVC

Next, we can simulate a catastrophic failure of our PVC by deleting it:

```sh
kubectl delete pvc --wait=false data-myapp-0
```

Then delete the pod that is using it:

```sh
kubectl delete pod myapp-0
```

Once our pod is back up and running, we can read the logs from the init
container to see that it was restored from S3:

```sh
$ kubectl logs myapp-0 init-litestream
2000/01/01 00:00:00.000000 /var/lib/myapp/db(s3): restoring snapshot 24456b497507f0c5/00000000 to /var/lib/myapp/db.tmp
2000/01/01 00:00:00.000000 /var/lib/myapp/db(s3): restoring wal files: generation=24456b497507f0c5 index=[00000000,00000001]
2000/01/01 00:00:00.000000 /var/lib/myapp/db(s3): downloaded wal 24456b497507f0c5/00000000 elapsed=613.525612ms
2000/01/01 00:00:00.000000 /var/lib/myapp/db(s3): downloaded wal 24456b497507f0c5/00000001 elapsed=599.573817ms
2000/01/01 00:00:00.000000 /var/lib/myapp/db(s3): applied wal 24456b497507f0c5/00000000 elapsed=2.357647ms
2000/01/01 00:00:00.000000 /var/lib/myapp/db(s3): applied wal 24456b497507f0c5/00000001 elapsed=1.800142ms
2000/01/01 00:00:00.000000 /var/lib/myapp/db(s3): renaming database from temporary location
```

We can then cURL our endpoint again and see that the count continues from where
it left off before the PVC deletion:

```sh
$ kubectl exec -it -c myapp myapp-0 -- curl localhost:8080
This server has been visited 3 times.
```

## Conclusion

Litestream can provide a simple persistence layer for Kubernetes services
by using StatefulSets and PVCs. Currently, Litestream only allows a single node
at a time in a StatefulSet but future versions will allow read replication to
additional nodes.

