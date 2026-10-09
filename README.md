# Week 5.b: Putting It All Together

Local classroom demo only. This is a teaching prototype, not a production deployment.
The five components are a Flask web server, a worker, PostgreSQL, Kafka, and Redis.
No AWS account, Helm, custom image build, or external email service is needed.

## Before class

- Use a disposable local Kubernetes cluster, such as Docker Desktop with Kubernetes enabled.
- Check `kubectl config current-context` and confirm it is your intended local cluster.
- Resources and commands use the `default` namespace. Names begin with `w5-`.
- A default dynamic StorageClass is required for the PostgreSQL volume. Check `kubectl get storageclass`.
- Reserve roughly 4 GB of available memory for this demo as a starting estimate. Image downloads and installation require internet access. Prepare before class.
- The Python packages install when the web and worker containers start. They do not install on students' computers.
- Test the complete walkthrough on the classroom laptop before teaching. This package has not yet been run end-to-end on that laptop.

## Start (PowerShell on Windows or Terminal on Mac)

Open a terminal in the extracted `week5b-demo` folder. Each command below is a single line.

```
kubectl apply -f infra.yaml
kubectl -n default rollout status deployment/w5-db --timeout=300s
kubectl -n default rollout status deployment/w5-kafka --timeout=300s
kubectl -n default rollout status deployment/w5-redis --timeout=300s
kubectl apply -f app.yaml
kubectl -n default rollout status deployment/w5-web --timeout=300s
kubectl -n default rollout status deployment/w5-worker --timeout=300s
kubectl -n default port-forward service/w5-web 8080:8080
```

Keep that terminal running. Open http://localhost:8080 in your browser.
Use a second terminal for the following checks.

## Walkthrough

1. Place an order. Notice the ID and `event_published: true`.
2. Wait a few seconds, then look up the order. The worker changes `pending` to `packed`.
3. Look it up twice quickly. The response source changes from `database` to `cache`.
4. Wait at least 20 seconds without looking it up, then look it up again. The entry expired, so the response source is `database`.
5. Inspect the actual database row and worker log:

```
kubectl -n default exec deployment/w5-db -- psql -U demo -d orders -c "SELECT * FROM orders;"
kubectl -n default logs deployment/w5-worker --tail=10
```

6. Stop the worker. Wait until no worker Pods remain, then place a new order:

```
kubectl -n default scale deployment/w5-worker --replicas=0
kubectl -n default get pods -l app=w5-worker
```

The new order stays pending. Kafka retains its event while the worker is stopped.

7. Restart the worker and check that the order becomes packed:

```
kubectl -n default scale deployment/w5-worker --replicas=1
kubectl -n default rollout status deployment/w5-worker --timeout=300s
kubectl -n default logs deployment/w5-worker --tail=10
```

8. Replace the database Pod and verify that its rows survive on its PVC:

```
kubectl -n default rollout restart deployment/w5-db
kubectl -n default rollout status deployment/w5-db --timeout=300s
kubectl -n default exec deployment/w5-db -- psql -U demo -d orders -c "SELECT * FROM orders;"
```

The database is briefly unavailable during this single-instance restart. Do not submit orders until it is ready again.

## Troubleshooting

`kubectl -n default get pods` shows progress. For a Pending Pod, use `kubectl -n default describe pod POD_NAME` and check Events. A Pending PVC usually means the cluster lacks a default StorageClass. For a web or worker startup problem, inspect its logs. First startup can take several minutes. If port 8080 is busy, use `8081:8080` and open localhost:8081 instead. Do not run this on a shared or production cluster.

## What this deliberately leaves out

- One broker, one topic partition, one worker. This demonstrates asynchronous processing, not high availability or parallel partition scaling.
- Kafka uses temporary storage. Stopping only the worker preserves the broker and its events. Replacing the Kafka Pod loses demo events and consumer offsets.
- Redis is a disposable cache with persistence disabled. PostgreSQL is the source of truth.
- Cache entries expire after 20 seconds. Worker updates also invalidate the relevant entry. Concurrent requests can still see temporarily stale data; this is not a strong-consistency design.
- The database write and Kafka publication are separate operations. If publication fails, the UI explicitly reports the saved order and missing notification. An outbox/reliable retry design is a later topic.
- The worker commits its offset after the database update. A crash can cause repeat processing. Setting the same status again is harmless here; charging a credit card twice would not be.
- Sample credentials, plaintext internal connections, and Flask's development server are for this isolated demonstration only.

## Cleanup and fresh start

Stop the port-forward terminal with Ctrl+C. The following deletes only the resources defined in these two demo manifests, including the PVC and saved demo orders. A cluster's storage reclaim policy determines the final disk disposal. Do not run it until you no longer need the demo data.

```
kubectl delete -f app.yaml -f infra.yaml --ignore-not-found
```

Run the Start commands again for a fresh demonstration. Existing unrelated resources remain untouched.
