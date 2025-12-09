from typing import List, Optional
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from config.logging_config import LoggerFactory
from config.settings import Settings
from src.utils.retry import retry_with_backoff

logger = LoggerFactory.get_logger(__name__)


class K8sClient:
    """
    Repository pattern wrapper for Kubernetes API client.
    Provides abstraction and error handling for K8s operations.
    """

    def __init__(self, settings: Settings):
        """Initialize Kubernetes client based on settings."""
        self.settings = settings
        self._initialize_client()

    def _initialize_client(self):
        """Initialize Kubernetes client configuration."""
        try:
            if self.settings.in_cluster:
                logger.info("Loading in-cluster Kubernetes configuration")
                config.load_incluster_config()
            else:
                logger.info("Loading Kubernetes configuration from kubeconfig")
                config.load_kube_config(config_file=self.settings.kubeconfig_path)

            self.core_v1 = client.CoreV1Api()
            self.storage_v1 = client.StorageV1Api()

            logger.info("Kubernetes client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kubernetes client: {e}")
            raise

    @retry_with_backoff(max_retries=3, delay=1.0, exceptions=(ApiException,))
    def list_namespaces(self) -> List[str]:
        """
        List all namespaces in the cluster.

        Returns:
            List of namespace names
        """
        try:
            namespaces = self.core_v1.list_namespace(
                timeout_seconds=self.settings.timeout_seconds
            )
            return [ns.metadata.name for ns in namespaces.items]
        except ApiException as e:
            logger.error(f"Error listing namespaces: {e}")
            raise

    @retry_with_backoff(max_retries=3, delay=1.0, exceptions=(ApiException,))
    def list_pods(
        self,
        namespace: str,
        label_selector: Optional[str] = None,
        field_selector: Optional[str] = None,
    ) -> List[client.V1Pod]:
        """
        List pods in a namespace.

        Args:
            namespace: Namespace name
            label_selector: Label selector for filtering
            field_selector: Field selector for filtering

        Returns:
            List of V1Pod objects
        """
        try:
            pods = self.core_v1.list_namespaced_pod(
                namespace=namespace,
                label_selector=label_selector,
                field_selector=field_selector,
                timeout_seconds=self.settings.timeout_seconds,
            )
            return pods.items
        except ApiException as e:
            logger.error(f"Error listing pods in namespace {namespace}: {e}")
            raise

    @retry_with_backoff(max_retries=3, delay=1.0, exceptions=(ApiException,))
    def list_nodes(self) -> List[client.V1Node]:
        """
        List all nodes in the cluster.

        Returns:
            List of V1Node objects
        """
        try:
            nodes = self.core_v1.list_node(
                timeout_seconds=self.settings.timeout_seconds
            )
            return nodes.items
        except ApiException as e:
            logger.error(f"Error listing nodes: {e}")
            raise

    @retry_with_backoff(max_retries=3, delay=1.0, exceptions=(ApiException,))
    def list_persistent_volume_claims(
        self, namespace: str
    ) -> List[client.V1PersistentVolumeClaim]:
        """
        List PVCs in a namespace.

        Args:
            namespace: Namespace name

        Returns:
            List of V1PersistentVolumeClaim objects
        """
        try:
            pvcs = self.core_v1.list_namespaced_persistent_volume_claim(
                namespace=namespace, timeout_seconds=self.settings.timeout_seconds
            )
            return pvcs.items
        except ApiException as e:
            logger.error(f"Error listing PVCs in namespace {namespace}: {e}")
            raise

    @retry_with_backoff(max_retries=3, delay=1.0, exceptions=(ApiException,))
    def get_node(self, node_name: str) -> client.V1Node:
        """
        Get a specific node.

        Args:
            node_name: Name of the node

        Returns:
            V1Node object
        """
        try:
            return self.core_v1.read_node(name=node_name)
        except ApiException as e:
            logger.error(f"Error getting node {node_name}: {e}")
            raise
