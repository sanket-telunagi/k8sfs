from dataclasses import dataclass
from typing import List, Optional


@dataclass
class NamespaceConfig:
    """Configuration for namespace monitoring."""

    name: str
    include_pods: bool = True
    include_pvcs: bool = True
    label_selector: Optional[str] = None
    field_selector: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "include_pods": self.include_pods,
            "include_pvcs": self.include_pvcs,
            "label_selector": self.label_selector,
            "field_selector": self.field_selector,
        }
