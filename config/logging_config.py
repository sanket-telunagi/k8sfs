import logging
import sys
from pythonjsonlogger import jsonlogger


class LoggerFactory:
    """Factory for creating configured loggers."""

    _configured = False

    @classmethod
    def configure(cls, log_level: str = "INFO", log_format: str = "json") -> None:
        """Configure root logger with specified format."""
        if cls._configured:
            return

        logger = logging.getLogger()
        logger.setLevel(getattr(logging, log_level.upper()))

        handler = logging.StreamHandler(sys.stdout)

        if log_format == "json":
            formatter = jsonlogger.JsonFormatter(
                "%(asctime)s %(name)s %(levelname)s %(message)s"
            )
        else:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

        handler.setFormatter(formatter)
        logger.addHandler(handler)
        cls._configured = True

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """Get a logger instance."""
        return logging.getLogger(name)
