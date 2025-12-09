from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime


@dataclass
class VolumeStorage:
    """Represents storage information for a volume."""

    name: str
    type: str
    capacity: Optional[str] = None
    used: Optional[str] = None
    available: Optional[str] = None
    mount_path: Optional[str] = None
    pvc_name: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "type": self.type,
            "capacity": self.capacity,
            "used": self.used,
            "available": self.available,
            "mount_path": self.mount_path,
            "pvc_name": self.pvc_name,
        }


@dataclass
class PodStorage:
    """Represents storage information for a pod."""

    name: str
    namespace: str
    node: str
    volumes: List[VolumeStorage] = field(default_factory=list)
    ephemeral_storage: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "namespace": self.namespace,
            "node": self.node,
            "volumes": [v.to_dict() for v in self.volumes],
            "ephemeral_storage": self.ephemeral_storage,
        }


@dataclass
class NodeStorage:
    """Represents aggregated storage information for a node."""

    node_name: str
    namespace: str
    pods: List[PodStorage] = field(default_factory=list)
    total_capacity: Optional[str] = None
    total_allocatable: Optional[str] = None
    total_used: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict:
        return {
            "node_name": self.node_name,
            "namespace": self.namespace,
            "pods": [p.to_dict() for p in self.pods],
            "total_capacity": self.total_capacity,
            "total_allocatable": self.total_allocatable,
            "total_used": self.total_used,
            "timestamp": self.timestamp.isoformat(),
            "pod_count": len(self.pods),
        }
