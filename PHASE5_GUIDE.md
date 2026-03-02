# ==========================================
# Phase 5 - Getting Started Guide
# Cloud-Scale Distributed System
# ==========================================

## Overview

Phase 5 transforms the Migration Platform into a cloud-native, horizontally scalable, distributed system capable of handling enterprise-scale workloads.

## Architecture

```
[Load Balancer]
      |
[API Gateway (Nginx/Ingress)]
      |
[Control Plane (API Pods x3)]
      |
[Kafka Cluster]
      |
[Worker Pods (Auto-scaling 5-50)]
      |
[PostgreSQL + PgBouncer]
[MinIO (S3)]
[Prometheus + Grafana]
```

## Key Features

### 1. Event Streaming with Kafka
- **Durable message log** - No job loss on pod restart
- **Horizontal scalability** - millions of events/sec
- **Ordered execution** - Partitioned by tenant_id
- **Exactly-once semantics** - Idempotent producers

### 2. Object Storage (S3/MinIO)
- **Chunk buffering** - Decoupled from worker pods
- **Checkpoint storage** - Resume from any point
- **Export storage** - Large file handling
- **Cross-pod data** - Accessible from any worker

### 3. Observability Stack
- **Prometheus** - Metrics collection
- **Grafana** - Visualization dashboards
- **Elasticsearch** - Centralized logging
- **Kibana** - Log analysis

### 4. Horizontal Pod Autoscaling (HPA)
- **CPU-based scaling** - Scale at 70% CPU
- **Memory-based scaling** - Scale at 75% memory
- **Kafka lag-based** - Scale on queue depth
- **Custom metrics** - rows/sec, queue size

### 5. High Availability
- **Zero downtime** - Rolling updates
- **Self-healing** - Automatic pod restart
- **Distributed locking** - No double-processing
- **Stateless services** - Any pod can handle any request

### 6. Security Hardening
- **Network policies** - Pod-to-pod isolation
- **TLS everywhere** - Encrypted communication
- **Secret management** - Kubernetes secrets
- **RBAC** - Role-based access control

## Local Development Setup

### Prerequisites
```bash
- Docker Desktop with Kubernetes enabled
- kubectl CLI
- Helm (optional)
- docker-compose
```

### Quick Start (Local with docker-compose)

```bash
# Start the entire cloud stack locally
docker-compose -f docker-compose.cloud.yaml up -d

# Services:
- Kafka: localhost:29092
- Kafka UI: localhost:8080
- MinIO: localhost:9000
- MinIO Console: localhost:9001
- Prometheus: localhost:9090
- Grafana: localhost:3001
- Elasticsearch: localhost:9200
- Kibana: localhost:5601
- PostgreSQL: localhost:5432
- PgBouncer: localhost:6432
```

### Install Python Dependencies

```bash
pip install -r requirements.txt
```

New dependencies for Phase 5:
- `confluent-kafka` - Kafka client
- `boto3` - S3 client
- `prometheus-client` - Metrics

### Run API Server

```bash
python -m uvicorn services.api.main:app --host 0.0.0.0 --port 8000 --reload
```

Endpoints:
- http://localhost:8000 - API root
- http://localhost:8000/docs - Swagger UI
- http://localhost:8000/health - Health check
- http://localhost:8000/metrics - Prometheus metrics

### Run Worker (Kafka Consumer)

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:29092
export S3_ENDPOINT=http://localhost:9000
python -m services.worker.kafka_worker
```

## Kubernetes Deployment

### Deploy to Kubernetes

```bash
# Create namespace
kubectl apply -f k8s/namespace.yaml

# Create secrets (update with real values first!)
kubectl apply -f k8s/secrets.yaml

# Deploy infrastructure
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/network-policy.yaml

# Deploy API
kubectl apply -f k8s/api-deployment.yaml

# Deploy Workers
kubectl apply -f k8s/worker-deployment.yaml

# Check status
kubectl get pods -n migration-platform
kubectl get hpa -n migration-platform
```

### View Logs

```bash
# API logs
kubectl logs -f deployment/api-server -n migration-platform

# Worker logs
kubectl logs -f deployment/migration-worker -n migration-platform

# Tail logs from all workers
kubectl logs -f -l app=migration-worker -n migration-platform
```

### Scale Manually

```bash
# Scale workers
kubectl scale deployment migration-worker --replicas=10 -n migration-platform

# Scale API
kubectl scale deployment api-server --replicas=5 -n migration-platform
```

### Monitor HPA

```bash
# Watch autoscaling
kubectl get hpa -n migration-platform -w

# Detailed HPA status
kubectl describe hpa migration-worker-hpa -n migration-platform
```

## Monitoring & Observability

### Prometheus Metrics

Access Prometheus: http://localhost:9090

Key metrics:
- `migration_job_created_total` - Jobs created
- `migration_chunk_processed_total` - Chunks processed
- `migration_rows_per_second` - Throughput
- `migration_kafka_consumer_lag` - Queue backlog
- `migration_worker_tasks_processing` - Active tasks

### Grafana Dashboards

Access Grafana: http://localhost:3001 (admin/admin)

Pre-configured dashboards:
1. Migration Overview
2. Worker Performance
3. Kafka Metrics
4. Database Performance

### Centralized Logging

Access Kibana: http://localhost:5601

Features:
- Structured JSON logs
- Correlation IDs for request tracing
- Tenant-aware filtering
- Error analysis

## Configuration

### Environment Variables

**API Server:**
```env
METADATA_DB_HOST=postgres-service
METADATA_DB_PORT=5432
KAFKA_BOOTSTRAP_SERVERS=kafka-service:9092
S3_ENDPOINT=http://minio-service:9000
PROMETHEUS_PORT=9090
METRICS_ENABLED=true
```

**Worker:**
```env
KAFKA_CONSUMER_GROUP=migration-workers
WORKER_REPLICAS=5
CHUNK_SIZE=100000
MAX_WORKERS_PER_JOB=10
```

### Resource Limits

**API Pods:**
- Requests: 500m CPU, 512Mi memory
- Limits: 2000m CPU, 2Gi memory

**Worker Pods:**
- Requests: 1000m CPU, 1Gi memory
- Limits: 4000m CPU, 4Gi memory

## Testing

### Test Kafka Connection

```bash
# List topics
docker exec migration-kafka kafka-topics --list --bootstrap-server localhost:9092

# Produce test message
docker exec -it migration-kafka kafka-console-producer \
  --topic migration-jobs \
  --bootstrap-server localhost:9092

# Consume test message
docker exec -it migration-kafka kafka-console-consumer \
  --topic migration-jobs \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

### Test S3/MinIO

```bash
# Access MinIO console: http://localhost:9001
# Default credentials: minioadmin/minioadmin

# Or use AWS CLI
aws --endpoint-url http://localhost:9000 s3 ls
aws --endpoint-url http://localhost:9000 s3 mb s3://migration-data
```

### Load Testing

```bash
# Simulate high load to trigger autoscaling
for i in {1..100}; do
  curl -X POST http://localhost:8000/api/migrations/jobs \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d @test-job.json &
done

# Watch pods scale up
kubectl get pods -n migration-platform -w
```

## Production Checklist

### Before Production Deployment

- [ ] Update secrets in `k8s/secrets.yaml`
- [ ] Configure persistent volumes for databases
- [ ] Set up managed Kafka (Confluent Cloud, AWS MSK)
- [ ] Configure managed S3 (AWS S3, Google Cloud Storage)
- [ ] Set up TLS certificates
- [ ] Configure ingress/load balancer
- [ ] Set up monitoring alerts
- [ ] Configure backup strategy
- [ ] Test disaster recovery
- [ ] Perform load testing
- [ ] Document runbooks

### Security Hardening

- [ ] Enable network policies
- [ ] Rotate all secrets
- [ ] Enable RBAC
- [ ] Configure IP allowlists
- [ ] Enable audit logging
- [ ] Scan images for vulnerabilities
- [ ] Set up intrusion detection

### Cost Optimization

- [ ] Configure HPA properly (avoid over-provisioning)
- [ ] Set resource limits
- [ ] Use spot/preemptible instances
- [ ] Configure autoscaler scale-down delay
- [ ] Monitor idle resources
- [ ] Set up cost alerts

## Troubleshooting

### Pod CrashLoopBackOff

```bash
# Check pod logs
kubectl logs <pod-name> -n migration-platform

# Describe pod for events
kubectl describe pod <pod-name> -n migration-platform

# Check resource limits
kubectl top pods -n migration-platform
```

### Kafka Consumer Lag

```bash
# Check Kafka UI: http://localhost:8080
# Or CLI:
docker exec migration-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group migration-workers
```

### Database Connection Issues

```bash
# Check PgBouncer
psql -h localhost -p 6432 -U postgres -d migration_metadata

# Check connections
kubectl exec -it deployment/api-server -n migration-platform -- \
  psql -h postgres-service -U postgres -c "SELECT count(*) FROM pg_stat_activity"
```

## Migration from Phase 4

### Data Migration

No data migration needed - same PostgreSQL database.

### Code Changes

1. **Replace Redis with Kafka** - Job queue now uses Kafka
2. **Add Object Storage** - Chunks stored in S3/MinIO
3. **Add Metrics** - Prometheus instrumentation
4. **Stateless Workers** - No local state

### Deployment

1. Deploy infrastructure (Kafka, MinIO, Prometheus)
2. Update API with new version
3. Rolling update of workers
4. Validate with canary deployments

## Performance Benchmarks

### Expected Throughput

- **Small migrations (<1GB)**: 100+ concurrent
- **Medium migrations (1-100GB)**: 50+ concurrent
- **Large migrations (>100GB)**: 10+ concurrent
- **Rows/second**: 50,000 - 500,000 (depending on workers)
- **Throughput**: 100 MB/s per worker

### Scaling Limits

- **API Pods**: 3-10 (more for high API traffic)
- **Worker Pods**: 5-50 (auto-scales based on load)
- **Kafka Partitions**: 10 per topic (increase for more tenants)
- **Concurrent Jobs**: Limited by database connections

## Support

For issues or questions:
- Check logs: `kubectl logs -n migration-platform`
- Monitor metrics: http://localhost:9090
- View traces: http://localhost:5601

## Next Steps

- Configure production Kubernetes cluster
- Set up CI/CD pipeline
- Implement multi-region deployment
- Add advanced monitoring alerts
- Perform chaos engineering tests
