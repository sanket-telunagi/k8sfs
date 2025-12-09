Key Design Patterns:
- Factory Pattern for collectors
- Strategy Pattern for exporters
- Repository Pattern for K8s client
- Observer Pattern for metrics
- Singleton Pattern for config
- Thread Pool Executor for concurrent collection


# Kubernetes Filesystem Monitor

Production-grade, modular application for monitoring Kubernetes filesystem usage across nodes and namespaces.

## Features

- **Parallel Collection**: Concurrent gathering of filesystem data using thread pools
- **Modular Architecture**: Clean separation of concerns with pluggable collectors and exporters
- **Multiple Export Formats**: JSON, Console, and Prometheus metrics
- **Caching**: Built-in caching with TTL support
- **Retry Logic**: Automatic retry with exponential backoff
- **Metrics**: Comprehensive Prometheus metrics
- **Configurable**: Environment-based configuration

## Installation

```bash
pip install -r requirements.txt
python setup.py install
```

## Usage

### Basic Usage

```bash
# Monitor specific namespaces
k8s-fs-monitor --namespaces default kube-system production

# With label selector
k8s-fs-monitor --namespaces default --label-selector app=nginx

# Export to JSON
k8s-fs-monitor --namespaces default --output json --output-dir ./reports

# Start Prometheus metrics server
k8s-fs-monitor --namespaces default --output prometheus --prometheus-port 9090
```

### Environment Variables

```bash
# Kubernetes config
export KUBECONFIG=/path/to/kubeconfig
export IN_CLUSTER=false

# Performance tuning
export MAX_WORKERS=20
export TIMEOUT_SECONDS=30
export BATCH_SIZE=10

# Caching
export CACHE_ENABLED=true
export CACHE_TTL=300

# Logging
export LOG_LEVEL=INFO
export LOG_FORMAT=json
```

## Architecture

### Design Patterns

- **Repository Pattern**: K8s API abstraction
- **Factory Pattern**: Collector instantiation
- **Strategy Pattern**: Pluggable exporters
- **Template Method**: Base collector lifecycle
- **Singleton**: Metrics and config
- **Observer**: Metrics collection

### Key Components

1. **Collectors**: Gather data from Kubernetes API
2. **Processors**: Aggregate and transform data
3. **Exporters**: Output data in various formats
4. **Core**: Orchestration and execution
5. **Utils**: Cross-cutting concerns (retry, cache, metrics)

## Performance

- Parallel namespace processing
- Configurable worker pool size
- Request-level caching
- Batch processing support
- Timeout protection

## Monitoring

Prometheus metrics exposed:
- `k8s_fs_collection_duration_seconds`: Collection timing
- `k8s_fs_collection_errors_total`: Error counts
- `k8s_fs_pods_processed_total`: Pods processed
- `k8s_fs_nodes_current`: Active nodes

## Testing

```bash
pytest tests/
```

## License

MIT