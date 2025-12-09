from typing import List
from kubernetes import client
from src.collectors.base_collector import BaseCollector
from src.models.node_storage import PodStorage, VolumeStorage


class PodStorageCollector(BaseCollector):
    """Collector for pod storage information."""

    def _collect_data(
        self, namespace: str, label_selector: str = None, field_selector: str = None
    ) -> List[client.V1Pod]:
        """
        Collect pod data from Kubernetes.

        Args:
            namespace: Namespace to collect from
            label_selector: Optional label selector
            field_selector: Optional field selector

        Returns:
            List of V1Pod objects
        """
        self.logger.info(
            f"Collecting pods from namespace {namespace}",
            extra={"label_selector": label_selector, "field_selector": field_selector},
        )

        pods = self.k8s_client.list_pods(
            namespace=namespace,
            label_selector=label_selector,
            field_selector=field_selector,
        )

        self.metrics.record_pods(namespace, len(pods))
        return pods

    def _process_data(self, raw_data: List[client.V1Pod]) -> List[PodStorage]:
        """
        Process pod data into PodStorage objects.

        Args:
            raw_data: List of V1Pod objects

        Returns:
            List of PodStorage objects
        """
        pod_storages = []

        for pod in raw_data:
            try:
                pod_storage = self._extract_pod_storage(pod)
                pod_storages.append(pod_storage)
            except Exception as e:
                self.logger.warning(
                    f"Failed to process pod {pod.metadata.name}",
                    extra={"error": str(e)},
                )

        return pod_storages

    def _extract_pod_storage(self, pod: client.V1Pod) -> PodStorage:
        """
        Extract storage information from a pod.

        Args:
            pod: V1Pod object

        Returns:
            PodStorage object
        """
        volumes = []

        if pod.spec.volumes:
            for volume in pod.spec.volumes:
                volume_storage = self._extract_volume_info(volume)
                if volume_storage:
                    volumes.append(volume_storage)

        # Extract ephemeral storage from resource requests/limits
        ephemeral_storage = None
        if pod.spec.containers:
            for container in pod.spec.containers:
                if container.resources:
                    if container.resources.requests:
                        ephemeral = container.resources.requests.get(
                            "ephemeral-storage"
                        )
                        if ephemeral:
                            ephemeral_storage = ephemeral
                    if container.resources.limits:
                        ephemeral = container.resources.limits.get("ephemeral-storage")
                        if ephemeral:
                            ephemeral_storage = ephemeral

        return PodStorage(
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            node=pod.spec.node_name or "unscheduled",
            volumes=volumes,
            ephemeral_storage=ephemeral_storage,
        )

    def _extract_volume_info(self, volume: client.V1Volume) -> VolumeStorage:
        """
        Extract volume information.

        Args:
            volume: V1Volume object

        Returns:
            VolumeStorage object or None
        """
        volume_type = self._determine_volume_type(volume)
        pvc_name = None

        if volume.persistent_volume_claim:
            pvc_name = volume.persistent_volume_claim.claim_name

        return VolumeStorage(name=volume.name, type=volume_type, pvc_name=pvc_name)

    def _determine_volume_type(self, volume: client.V1Volume) -> str:
        """Determine volume type from V1Volume object."""
        if volume.persistent_volume_claim:
            return "pvc"
        elif volume.config_map:
            return "configmap"
        elif volume.secret:
            return "secret"
        elif volume.empty_dir:
            return "emptydir"
        elif volume.host_path:
            return "hostpath"
        else:
            return "unknown"
