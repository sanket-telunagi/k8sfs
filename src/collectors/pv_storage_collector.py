from typing import List, Dict
from kubernetes import client
from src.collectors.base_collector import BaseCollector


class PVStorageCollector(BaseCollector):
    """Collector for persistent volume information."""

    def _collect_data(self, namespace: str) -> List[client.V1PersistentVolumeClaim]:
        """
        Collect PVC data from Kubernetes.

        Args:
            namespace: Namespace to collect from

        Returns:
            List of V1PersistentVolumeClaim objects
        """
        self.logger.info(f"Collecting PVCs from namespace {namespace}")
        return self.k8s_client.list_persistent_volume_claims(namespace)

    def _process_data(
        self, raw_data: List[client.V1PersistentVolumeClaim]
    ) -> Dict[str, Dict]:
        """
        Process PVC data into structured format.

        Args:
            raw_data: List of V1PersistentVolumeClaim objects

        Returns:
            Dictionary mapping PVC names to their details
        """
        pvc_map = {}

        for pvc in raw_data:
            pvc_info = {
                "name": pvc.metadata.name,
                "namespace": pvc.metadata.namespace,
                "status": pvc.status.phase,
                "capacity": None,
                "storage_class": pvc.spec.storage_class_name,
                "access_modes": pvc.spec.access_modes,
                "volume_name": pvc.spec.volume_name,
            }

            if pvc.status.capacity:
                pvc_info["capacity"] = pvc.status.capacity.get("storage")

            pvc_map[pvc.metadata.name] = pvc_info

        return pvc_map
