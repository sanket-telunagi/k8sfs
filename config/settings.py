import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Settings:
    """Centralized configuration management with environment override support."""

    # Kubernetes Configuration
    kubeconfig_path: Optional[str] = field(
        default_factory=lambda: os.getenv("KUBECONFIG", None)
    )
    in_cluster: bool = field(
        default_factory=lambda: os.getenv("IN_CLUSTER", "false").lower() == "true"
    )

    # Performance Configuration
    max_workers: int = field(
        default_factory=lambda: int(os.getenv("MAX_WORKERS", "20"))
    )
    timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("TIMEOUT_SECONDS", "30"))
    )
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "10")))

    # Cache Configuration
    cache_enabled: bool = field(
        default_factory=lambda: os.getenv("CACHE_ENABLED", "true").lower() == "true"
    )
    cache_ttl: int = field(default_factory=lambda: int(os.getenv("CACHE_TTL", "300")))

    # Retry Configuration
    max_retries: int = field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "3")))
    retry_delay: float = field(
        default_factory=lambda: float(os.getenv("RETRY_DELAY", "1.0"))
    )

    # Export Configuration
    export_format: str = field(
        default_factory=lambda: os.getenv("EXPORT_FORMAT", "json")
    )
    prometheus_port: int = field(
        default_factory=lambda: int(os.getenv("PROMETHEUS_PORT", "9090"))
    )

    # Logging Configuration
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    log_format: str = field(default_factory=lambda: os.getenv("LOG_FORMAT", "json"))

    @classmethod
    def from_env(cls) -> "Settings":
        """Create settings instance from environment variables."""
        return cls()

    def validate(self) -> None:
        """Validate configuration settings."""
        if self.max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        if self.timeout_seconds < 1:
            raise ValueError("timeout_seconds must be at least 1")
        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1")
