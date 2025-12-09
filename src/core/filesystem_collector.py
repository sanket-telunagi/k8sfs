from typing import List, Dict
from src.core.k8s_client import K8sClient
from src.core.executor_pool import ExecutorPool
from src.collectors.pod_storage_collector import PodStorageCollector
from src.collectors.pv_storage_collector import PVStorageCollector
from src.models.node_storage import NodeStorage, PodStorage
from src.models.namespace_config import NamespaceConfig
from src.utils.metrics import MetricsCollector
from src.utils.cache import SimpleCache
from config.logging_config import LoggerFactory
from config.settings import Settings
from collections import defaultdict

logger = LoggerFactory.get_logger(__name__)


class FilesystemCollector:
    """
    Main orchestrator for filesystem collection.
    Coordinates collectors and aggregates results by node.
    """

    def __init__(self, settings: Settings):
        """Initialize filesystem collector with dependencies."""
        self.settings = settings
        self.k8s_client = K8sClient(settings)
        self.metrics = MetricsCollector()
        self.cache = SimpleCache(ttl=settings.cache_ttl)

        # Initialize collectors
        self.pod_collector = PodStorageCollector(
            self.k8s_client, self.metrics, self.cache
        )
        self.pv_collector = PVStorageCollector(
            self.k8s_client, self.metrics, self.cache
        )

    def collect_all_namespaces(
        self, namespace_configs: List[NamespaceConfig]
    ) -> Dict[str, List[NodeStorage]]:
        """
        Collect filesystem data for all configured namespaces in parallel.

        Args:
            namespace_configs: List of namespace configurations

        Returns:
            Dictionary mapping namespace names to list of NodeStorage objects
        """
        logger.info(f"Starting collection for {len(namespace_configs)} namespaces")

        with ExecutorPool(self.settings) as pool:
            results = pool.execute_parallel(self.collect_namespace, namespace_configs)

        # Build result dictionary
        result_dict = {}
        for namespace_config, node_storages in results:
            result_dict[namespace_config.name] = node_storages

        logger.info("Collection completed for all namespaces")
        return result_dict

    def collect_namespace(
        self, namespace_config: NamespaceConfig
    ) -> tuple[NamespaceConfig, List[NodeStorage]]:
        """
        Collect filesystem data for a single namespace.

        Args:
            namespace_config: Namespace configuration

        Returns:
            Tuple of (namespace_config, list of NodeStorage objects)
        """
        namespace = namespace_config.name
        logger.info(f"Collecting data for namespace: {namespace}")

        try:
            # Collect pod storage data
            pod_storages = self.pod_collector.collect(
                namespace,
                label_selector=namespace_config.label_selector,
                field_selector=namespace_config.field_selector,
            )

            # Collect PVC data if enabled
            pvc_map = {}
            if namespace_config.include_pvcs:
                pvc_map = self.pv_collector.collect(namespace)

            # Enrich pod storage with PVC data
            self._enrich_with_pvc_data(pod_storages, pvc_map)

            # Aggregate by node
            node_storages = self._aggregate_by_node(namespace, pod_storages)

            # Enrich with node-level data
            self._enrich_with_node_data(node_storages)

            self.metrics.set_nodes(namespace, len(node_storages))

            logger.info(
                f"Completed collection for namespace {namespace}",
                extra={
                    "pod_count": len(pod_storages),
                    "node_count": len(node_storages),
                },
            )

            return (namespace_config, node_storages)

        except Exception as e:
            logger.error(
                f"Failed to collect data for namespace {namespace}",
                extra={"error": str(e)},
            )
            self.metrics.record_error(namespace, type(e).__name__)
            return (namespace_config, [])

    def _enrich_with_pvc_data(
        self, pod_storages: List[PodStorage], pvc_map: Dict[str, Dict]
    ) -> None:
        """Enrich pod storage objects with PVC capacity data."""
        for pod_storage in pod_storages:
            for volume in pod_storage.volumes:
                if volume.pvc_name and volume.pvc_name in pvc_map:
                    pvc_info = pvc_map[volume.pvc_name]
                    volume.capacity = pvc_info.get("capacity")

    def _aggregate_by_node(
        self, namespace: str, pod_storages: List[PodStorage]
    ) -> List[NodeStorage]:
        """
        Aggregate pod storage data by node.

        Args:
            namespace: Namespace name
            pod_storages: List of PodStorage objects

        Returns:
            List of NodeStorage objects
        """
        node_map = defaultdict(list)

        for pod_storage in pod_storages:
            node_map[pod_storage.node].append(pod_storage)

        node_storages = []
        for node_name, pods in node_map.items():
            node_storage = NodeStorage(
                node_name=node_name, namespace=namespace, pods=pods
            )
            node_storages.append(node_storage)

        return node_storages

    def _enrich_with_node_data(self, node_storages: List[NodeStorage]) -> None:
        """Enrich node storage objects with node-level capacity data."""
        try:
            nodes = self.k8s_client.list_nodes()
            node_info_map = {node.metadata.name: node for node in nodes}

            for node_storage in node_storages:
                if node_storage.node_name in node_info_map:
                    node = node_info_map[node_storage.node_name]

                    if node.status.capacity:
                        node_storage.total_capacity = node.status.capacity.get(
                            "ephemeral-storage"
                        )

                    if node.status.allocatable:
                        node_storage.total_allocatable = node.status.allocatable.get(
                            "ephemeral-storage"
                        )

        except Exception as e:
            logger.warning(f"Failed to enrich node data: {e}")
