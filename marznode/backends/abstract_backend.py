"""Abstract VPN Backend Interface"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from enum import Enum
from typing import Any, Optional

from marznode.models import User, Inbound

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status enumeration for backends"""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"
    STARTING = "starting"
    STOPPING = "stopping"


class HealthMonitor:
    """Enhanced health monitoring for backends with metrics and history"""

    def __init__(self, backend_name: str, max_failures: int = 3):
        self.backend_name = backend_name
        self.status = HealthStatus.UNKNOWN
        self.last_check = None
        self.last_healthy = None
        self.failure_count = 0
        self.max_failures = max_failures
        self.failure_history = []
        self.status_history = []
        self._lock = asyncio.Lock()

    async def set_healthy(self):
        """Mark backend as healthy"""
        async with self._lock:
            old_status = self.status
            self.status = HealthStatus.HEALTHY
            self.failure_count = 0
            self.last_check = time.time()
            self.last_healthy = time.time()

            if old_status != HealthStatus.HEALTHY:
                logger.info(
                    f"Backend '{self.backend_name}' is now healthy (was: {old_status.value})"
                )
                self._add_status_history(HealthStatus.HEALTHY)

    async def set_unhealthy(self, reason: str = ""):
        """Mark backend as unhealthy with reason"""
        async with self._lock:
            old_status = self.status
            self.failure_count += 1

            # Determine new status based on failure count
            if self.failure_count >= self.max_failures:
                self.status = HealthStatus.UNHEALTHY
            else:
                self.status = HealthStatus.DEGRADED

            self.last_check = time.time()

            # Record failure
            self.failure_history.append(
                {
                    "timestamp": time.time(),
                    "reason": reason,
                    "failure_count": self.failure_count,
                }
            )

            # Keep only last 10 failures
            if len(self.failure_history) > 10:
                self.failure_history = self.failure_history[-10:]

            if old_status != self.status:
                logger.warning(
                    f"Backend '{self.backend_name}' health degraded to {self.status.value} "
                    f"(failure {self.failure_count}/{self.max_failures}): {reason}"
                )
                self._add_status_history(self.status)
            else:
                logger.debug(
                    f"Backend '{self.backend_name}' still {self.status.value} "
                    f"(failure {self.failure_count}/{self.max_failures}): {reason}"
                )

    async def set_starting(self):
        """Mark backend as starting"""
        async with self._lock:
            self.status = HealthStatus.STARTING
            self.last_check = time.time()
            logger.debug(f"Backend '{self.backend_name}' is starting")
            self._add_status_history(HealthStatus.STARTING)

    async def set_stopping(self):
        """Mark backend as stopping"""
        async with self._lock:
            self.status = HealthStatus.STOPPING
            self.last_check = time.time()
            logger.debug(f"Backend '{self.backend_name}' is stopping")
            self._add_status_history(HealthStatus.STOPPING)

    def _add_status_history(self, status: HealthStatus):
        """Add status change to history"""
        self.status_history.append({"timestamp": time.time(), "status": status.value})

        # Keep only last 20 status changes
        if len(self.status_history) > 20:
            self.status_history = self.status_history[-20:]

    def is_healthy(self) -> bool:
        """Check if backend is healthy"""
        return self.status == HealthStatus.HEALTHY

    def is_operational(self) -> bool:
        """Check if backend is operational (healthy or degraded)"""
        return self.status in (HealthStatus.HEALTHY, HealthStatus.DEGRADED)

    def get_metrics(self) -> dict:
        """Get health metrics"""
        uptime = None
        if self.last_healthy:
            uptime = time.time() - self.last_healthy

        return {
            "status": self.status.value,
            "failure_count": self.failure_count,
            "max_failures": self.max_failures,
            "last_check": self.last_check,
            "last_healthy": self.last_healthy,
            "uptime_seconds": uptime,
            "recent_failures": len(self.failure_history),
            "status_changes": len(self.status_history),
        }


class VPNBackend(ABC):
    """Abstract base class for VPN backends"""

    backend_type: str
    config_format: int

    def __init__(self, max_health_failures: int = 3):
        """
        Initialize VPN backend

        Args:
            max_health_failures: Maximum consecutive failures before marking unhealthy
        """
        self._health_monitor = HealthMonitor(
            self.__class__.__name__, max_failures=max_health_failures
        )
        logger.debug(f"Initialized {self.__class__.__name__} backend")

    @property
    @abstractmethod
    def version(self) -> Optional[str]:
        """Get backend version"""
        raise NotImplementedError

    @property
    @abstractmethod
    def running(self) -> bool:
        """Check if backend is running"""
        raise NotImplementedError

    @property
    def health_status(self) -> HealthStatus:
        """Get current health status"""
        return self._health_monitor.status

    def get_health_metrics(self) -> dict:
        """Get detailed health metrics"""
        return self._health_monitor.get_metrics()

    async def _set_healthy(self):
        """Mark backend as healthy (async wrapper)"""
        await self._health_monitor.set_healthy()

    async def _set_unhealthy(self, reason: str = ""):
        """Mark backend as unhealthy (async wrapper)"""
        await self._health_monitor.set_unhealthy(reason)

    async def _set_starting(self):
        """Mark backend as starting"""
        await self._health_monitor.set_starting()

    async def _set_stopping(self):
        """Mark backend as stopping"""
        await self._health_monitor.set_stopping()

    # Synchronous wrappers for backwards compatibility
    def _set_healthy_sync(self):
        """Mark backend as healthy (sync version)"""
        asyncio.create_task(self._set_healthy())

    def _set_unhealthy_sync(self, reason: str = ""):
        """Mark backend as unhealthy (sync version)"""
        asyncio.create_task(self._set_unhealthy(reason))

    @abstractmethod
    def contains_tag(self, tag: str) -> bool:
        """Check if backend contains the specified inbound tag"""
        raise NotImplementedError

    @abstractmethod
    async def start(self, backend_config: Any) -> None:
        """
        Start the backend

        Args:
            backend_config: Backend configuration
        """
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        """Stop the backend"""
        raise NotImplementedError

    @abstractmethod
    async def restart(self, backend_config: Any) -> Any:
        """
        Restart the backend

        Args:
            backend_config: Optional new configuration

        Returns:
            Any restart-specific data
        """
        raise NotImplementedError

    @abstractmethod
    async def add_user(self, user: User, inbound: Inbound) -> None:
        """
        Add a user to an inbound

        Args:
            user: User to add
            inbound: Inbound to add user to
        """
        raise NotImplementedError

    @abstractmethod
    async def remove_user(self, user: User, inbound: Inbound) -> None:
        """
        Remove a user from an inbound

        Args:
            user: User to remove
            inbound: Inbound to remove user from
        """
        raise NotImplementedError

    @abstractmethod
    def get_logs(self, include_buffer: bool) -> AsyncIterator:
        """
        Get backend logs

        Args:
            include_buffer: Whether to include buffered logs

        Returns:
            Async iterator of log lines
        """
        raise NotImplementedError

    @abstractmethod
    async def get_usages(self, reset: bool = True) -> dict[int, int]:
        """
        Get user usage statistics

        Args:
            reset: Whether to reset statistics after fetching

        Returns:
            Dictionary mapping user IDs to usage in bytes
        """
        raise NotImplementedError

    @abstractmethod
    def list_inbounds(self) -> list[Inbound]:
        """
        List all inbounds in this backend

        Returns:
            List of inbound objects
        """
        raise NotImplementedError

    @abstractmethod
    def get_config(self) -> str:
        """
        Get current backend configuration

        Returns:
            Configuration as string
        """
        raise NotImplementedError

    async def cleanup(self) -> None:
        """
        Cleanup resources on shutdown
        This is optional and can be overridden by implementations
        """
        logger.debug(f"Cleanup called for {self.__class__.__name__}")
        pass

    def __repr__(self) -> str:
        """String representation of backend"""
        return (
            f"{self.__class__.__name__}("
            f"type={self.backend_type}, "
            f"running={self.running}, "
            f"health={self.health_status.value})"
        )
