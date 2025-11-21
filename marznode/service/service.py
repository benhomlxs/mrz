"""
The grpc Service to add/update/delete users
Right now it only supports Xray but that is subject to change
"""

import asyncio
import json
import logging
import time
from collections import defaultdict
from functools import wraps

from grpclib import GRPCError, Status
from grpclib.server import Stream

from marznode.backends.abstract_backend import VPNBackend
from marznode.storage import BaseStorage
from .service_grpc import MarzServiceBase

logger = logging.getLogger(__name__)


# Enhanced retry decorator with better error handling
def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Enhanced retry decorator for async functions with exponential backoff"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except (OSError, ConnectionError, asyncio.TimeoutError) as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = delay * (backoff**attempt)
                        logger.warning(
                            f"Retry {attempt + 1}/{max_retries} for {func.__name__}: {e}. "
                            f"Waiting {wait_time:.2f}s before retry"
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"All retries exhausted for {func.__name__}: {e}")
                        raise
                except GRPCError:
                    raise
                except Exception as e:
                    logger.error(f"Non-retryable error in {func.__name__}: {e}")
                    raise
            raise last_exception

        return wrapper

    return decorator


# Enhanced error handler with better context
def handle_errors(func):
    """Enhanced error handling decorator with detailed logging"""

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        start_time = time.time()
        method_name = func.__name__
        try:
            logger.debug(f"Starting {method_name}")
            result = await func(self, *args, **kwargs)
            duration = time.time() - start_time
            logger.debug(f"{method_name} completed successfully in {duration:.3f}s")
            return result
        except GRPCError as e:
            duration = time.time() - start_time
            logger.error(
                f"{method_name} failed with gRPC error in {duration:.3f}s: {e}"
            )
            raise
        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                f"{method_name} failed with exception in {duration:.3f}s: {e}",
                exc_info=True,
            )
            raise GRPCError(
                Status.INTERNAL, f"Internal error in {method_name}: {str(e)}"
            )

    return wrapper


from .service_pb2 import (
    BackendConfig as BackendConfig_pb2,
    Backend,
    BackendLogsRequest,
    RestartBackendRequest,
    BackendStats,
)
from .service_pb2 import (
    UserData,
    UsersData,
    Empty,
    BackendsResponse,
    Inbound,
    UsersStats,
    LogLine,
)
from ..models import User, Inbound as InboundModel


class MarzService(MarzServiceBase):
    """Add/Update/Delete users based on calls from the client"""

    def __init__(self, storage: BaseStorage, backends: dict[str, VPNBackend]):
        self._backends = backends
        self._storage = storage
        self._service_healthy = True
        self._operation_lock = asyncio.Lock()  # Global operation lock
        self._user_locks = {}  # Per-user locks
        self._backend_locks = {}  # Per-backend locks

        # Initialize backend locks
        for backend_name in backends.keys():
            self._backend_locks[backend_name] = asyncio.Lock()

    def _get_user_lock(self, user_id: int) -> asyncio.Lock:
        """Get or create a lock for a specific user"""
        if user_id not in self._user_locks:
            self._user_locks[user_id] = asyncio.Lock()
        return self._user_locks[user_id]

    def _resolve_tag(self, inbound_tag: str) -> VPNBackend:
        """Resolve inbound tag to backend with validation"""
        for backend in self._backends.values():
            if backend.contains_tag(inbound_tag):
                return backend
        logger.error(f"No backend found for inbound tag: {inbound_tag}")
        raise GRPCError(
            Status.NOT_FOUND, f"Inbound tag '{inbound_tag}' not found in any backend"
        )

    @retry_on_failure(max_retries=3, delay=1.0)
    async def _add_user(self, user: User, inbounds: list[InboundModel]):
        """Add user to inbounds with retry logic"""
        if not inbounds:
            logger.warning(f"No inbounds provided for user {user.username}")
            return

        for inbound in inbounds:
            try:
                backend = self._resolve_tag(inbound.tag)

                # Check backend health before adding
                if not backend.running:
                    logger.warning(
                        f"Backend not running for inbound {inbound.tag}, skipping add for user {user.username}"
                    )
                    continue

                logger.debug(
                    f"Adding user '{user.username}' to inbound '{inbound.tag}'"
                )
                await asyncio.wait_for(
                    backend.add_user(user, inbound), timeout=10.0  # 10 second timeout
                )
                logger.info(
                    f"Successfully added user '{user.username}' to inbound '{inbound.tag}'"
                )

            except asyncio.TimeoutError:
                logger.error(
                    f"Timeout adding user '{user.username}' to inbound '{inbound.tag}'"
                )
                raise
            except GRPCError:
                raise
            except Exception as e:
                logger.error(
                    f"Failed to add user '{user.username}' to inbound '{inbound.tag}': {e}"
                )
                raise

    @retry_on_failure(max_retries=3, delay=1.0)
    async def _remove_user(self, user: User, inbounds: list[InboundModel]):
        """Remove user from inbounds with retry logic"""
        if not inbounds:
            logger.warning(f"No inbounds provided for removing user {user.username}")
            return

        for inbound in inbounds:
            try:
                backend = self._resolve_tag(inbound.tag)

                # Check backend health before removing
                if not backend.running:
                    logger.warning(
                        f"Backend not running for inbound {inbound.tag}, skipping removal for user {user.username}"
                    )
                    continue

                logger.debug(
                    f"Removing user '{user.username}' from inbound '{inbound.tag}'"
                )
                await asyncio.wait_for(
                    backend.remove_user(user, inbound),
                    timeout=10.0,  # 10 second timeout
                )
                logger.info(
                    f"Successfully removed user '{user.username}' from inbound '{inbound.tag}'"
                )

            except asyncio.TimeoutError:
                logger.error(
                    f"Timeout removing user '{user.username}' from inbound '{inbound.tag}'"
                )
                # Continue with other inbounds even if one times out
                continue
            except GRPCError:
                raise
            except Exception as e:
                logger.error(
                    f"Failed to remove user '{user.username}' from inbound '{inbound.tag}': {e}"
                )
                # Continue with other inbounds
                continue

    async def _validate_inbounds(
        self, inbound_tags: list[str]
    ) -> tuple[list[InboundModel], list[str]]:
        """
        Validate inbound tags and return available inbounds and missing tags

        Returns:
            tuple: (available_inbounds, missing_tags)
        """
        if not inbound_tags:
            return [], []

        available_inbounds = await self._storage.list_inbounds(tag=inbound_tags)
        available_tags = {i.tag for i in available_inbounds}
        missing_tags = set(inbound_tags) - available_tags

        return available_inbounds, list(missing_tags)

    async def _update_user(self, user_data: UserData):
        """Update user with enhanced error handling and validation"""
        user = User(
            id=user_data.user.id,
            username=user_data.user.username,
            key=user_data.user.key,
        )

        # Use per-user lock to prevent concurrent modifications
        async with self._get_user_lock(user.id):
            try:
                storage_user = await self._storage.list_users(user.id)

                # Extract and validate inbound tags
                inbound_tags = (
                    [i.tag for i in user_data.inbounds] if user_data.inbounds else []
                )

                # Validate inbound tags if provided
                available_inbounds = []
                if inbound_tags:
                    available_inbounds, missing_tags = await self._validate_inbounds(
                        inbound_tags
                    )

                    if missing_tags:
                        logger.warning(
                            f"Missing inbounds for user '{user.username}': {missing_tags}. "
                            f"Available: {[i.tag for i in available_inbounds]}"
                        )

                        # For non-sudo admins, this might be expected behavior
                        # Only use available inbounds
                        if not available_inbounds:
                            logger.error(
                                f"No valid inbounds found for user '{user.username}'"
                            )
                            # If no valid inbounds, treat as removal request
                            if storage_user:
                                await self._remove_user(
                                    storage_user, storage_user.inbounds
                                )
                                await self._storage.remove_user(user)
                                logger.info(
                                    f"Removed user '{user.username}' due to no valid inbounds"
                                )
                            return

                # Case 1: New user with inbounds
                if not storage_user and available_inbounds:
                    logger.info(
                        f"Adding new user '{user.username}' to {len(available_inbounds)} inbounds"
                    )
                    await self._add_user(user, available_inbounds)
                    await self._storage.update_user_inbounds(user, available_inbounds)
                    logger.info(f"Successfully added new user '{user.username}'")
                    return

                # Case 2: Remove user (empty inbounds list)
                elif not inbound_tags and storage_user:
                    logger.info(f"Removing user '{user.username}' from all inbounds")
                    await self._remove_user(storage_user, storage_user.inbounds)
                    await self._storage.remove_user(user)
                    logger.info(f"Successfully removed user '{user.username}'")
                    return

                # Case 3: User doesn't exist and no inbounds (no-op)
                elif not inbound_tags and not storage_user:
                    logger.debug(
                        f"No operation needed for user '{user.username}' (doesn't exist, no inbounds)"
                    )
                    return

                # Case 4: Update existing user's inbounds
                if storage_user and available_inbounds:
                    storage_tags = {i.tag for i in storage_user.inbounds}
                    new_tags = {i.tag for i in available_inbounds}

                    added_tags = new_tags - storage_tags
                    removed_tags = storage_tags - new_tags

                    if not added_tags and not removed_tags:
                        logger.debug(
                            f"No inbound changes needed for user '{user.username}'"
                        )
                        return

                    logger.info(
                        f"Updating user '{user.username}': "
                        f"adding {len(added_tags)} inbounds, removing {len(removed_tags)} inbounds"
                    )

                    # Get inbound objects for additions and removals
                    removed_inbounds = [
                        i for i in storage_user.inbounds if i.tag in removed_tags
                    ]
                    added_inbounds = [
                        i for i in available_inbounds if i.tag in added_tags
                    ]

                    # Remove from old inbounds first
                    if removed_inbounds:
                        await self._remove_user(storage_user, removed_inbounds)
                        logger.debug(
                            f"Removed user '{user.username}' from: {[i.tag for i in removed_inbounds]}"
                        )

                    # Add to new inbounds
                    if added_inbounds:
                        await self._add_user(storage_user, added_inbounds)
                        logger.debug(
                            f"Added user '{user.username}' to: {[i.tag for i in added_inbounds]}"
                        )

                    # Update storage with new inbound list
                    await self._storage.update_user_inbounds(
                        storage_user, available_inbounds
                    )
                    logger.info(f"Successfully updated user '{user.username}'")

            except GRPCError:
                raise
            except Exception as e:
                logger.error(
                    f"Failed to update user '{user.username}': {e}", exc_info=True
                )
                raise GRPCError(Status.INTERNAL, f"Failed to update user: {str(e)}")

    @handle_errors
    async def SyncUsers(self, stream: Stream[UserData, Empty]) -> None:
        """Sync users with enhanced error handling and health checks"""
        if not self._service_healthy:
            raise GRPCError(Status.UNAVAILABLE, "Service is currently unhealthy")

        success_count = 0
        failure_count = 0

        try:
            async for user_data in stream:
                try:
                    await self._update_user(user_data)
                    success_count += 1
                    logger.debug(
                        f"Successfully synced user '{user_data.user.username}' ({success_count} total)"
                    )
                except GRPCError as e:
                    failure_count += 1
                    logger.error(
                        f"gRPC error syncing user '{user_data.user.username}': {e}"
                    )
                    # Continue processing other users
                    continue
                except Exception as e:
                    failure_count += 1
                    logger.error(
                        f"Error syncing user '{user_data.user.username}': {e}",
                        exc_info=True,
                    )
                    # Continue processing other users
                    continue

            logger.info(
                f"SyncUsers completed: {success_count} successful, {failure_count} failed"
            )

        except Exception as e:
            logger.error(f"SyncUsers stream error: {e}", exc_info=True)
            raise GRPCError(Status.INTERNAL, f"Stream error: {str(e)}")

    @handle_errors
    async def FetchBackends(
        self,
        stream: Stream[Empty, BackendsResponse],
    ) -> None:
        """Fetch backends with health status"""
        if not self._service_healthy:
            raise GRPCError(Status.UNAVAILABLE, "Service is currently unhealthy")

        await stream.recv_message()

        backends = []
        for name, backend in self._backends.items():
            try:
                inbounds = [
                    Inbound(tag=i.tag, config=json.dumps(i.config))
                    for i in backend.list_inbounds()
                ]

                backends.append(
                    Backend(
                        name=name,
                        type=backend.backend_type,
                        version=backend.version or "unknown",
                        inbounds=inbounds,
                    )
                )
                logger.debug(f"Fetched backend '{name}' with {len(inbounds)} inbounds")
            except Exception as e:
                logger.error(f"Error fetching backend '{name}': {e}")
                # Continue with other backends
                continue

        await stream.send_message(BackendsResponse(backends=backends))
        logger.info(f"FetchBackends completed: {len(backends)} backends returned")

    @handle_errors
    async def RepopulateUsers(
        self,
        stream: Stream[UsersData, Empty],
    ) -> None:
        """Repopulate users with complete synchronization"""
        async with self._operation_lock:
            try:
                users_data = (await stream.recv_message()).users_data
                logger.info(f"Starting RepopulateUsers with {len(users_data)} users")

                # Update all users from the request
                for user_data in users_data:
                    try:
                        await self._update_user(user_data)
                    except Exception as e:
                        logger.error(
                            f"Error during repopulate for user '{user_data.user.username}': {e}"
                        )
                        continue

                # Remove users not in the request
                user_ids = {user_data.user.id for user_data in users_data}
                storage_users = await self._storage.list_users()

                removed_count = 0
                for storage_user in storage_users:
                    if storage_user.id not in user_ids:
                        try:
                            await self._remove_user(storage_user, storage_user.inbounds)
                            await self._storage.remove_user(storage_user)
                            removed_count += 1
                            logger.debug(
                                f"Removed orphaned user '{storage_user.username}'"
                            )
                        except Exception as e:
                            logger.error(
                                f"Error removing user '{storage_user.username}': {e}"
                            )
                            continue

                logger.info(
                    f"RepopulateUsers completed: {len(users_data)} synced, {removed_count} removed"
                )
                await stream.send_message(Empty())

            except Exception as e:
                logger.error(f"RepopulateUsers error: {e}", exc_info=True)
                raise GRPCError(Status.INTERNAL, f"Repopulate failed: {str(e)}")

    @handle_errors
    async def FetchUsersStats(self, stream: Stream[Empty, UsersStats]) -> None:
        """Fetch user statistics with error handling"""
        await stream.recv_message()
        all_stats = defaultdict(int)

        for backend_name, backend in self._backends.items():
            try:
                if not backend.running:
                    logger.warning(
                        f"Backend '{backend_name}' not running, skipping stats"
                    )
                    continue

                stats = await asyncio.wait_for(
                    backend.get_usages(), timeout=30.0  # 30 second timeout
                )

                for user, usage in stats.items():
                    all_stats[user] += usage

                logger.debug(
                    f"Fetched stats from backend '{backend_name}': {len(stats)} users"
                )

            except asyncio.TimeoutError:
                logger.error(f"Timeout fetching stats from backend '{backend_name}'")
                continue
            except Exception as e:
                logger.error(f"Error fetching stats from backend '{backend_name}': {e}")
                continue

        user_stats = [
            UsersStats.UserStats(uid=uid, usage=usage)
            for uid, usage in all_stats.items()
        ]

        await stream.send_message(UsersStats(users_stats=user_stats))
        logger.info(f"FetchUsersStats completed: {len(user_stats)} users")

    @handle_errors
    async def StreamBackendLogs(
        self, stream: Stream[BackendLogsRequest, LogLine]
    ) -> None:
        """Stream backend logs with validation"""
        req = await stream.recv_message()

        if req.backend_name not in self._backends:
            raise GRPCError(Status.NOT_FOUND, f"Backend '{req.backend_name}' not found")

        backend = self._backends[req.backend_name]
        log_count = 0

        try:
            async for line in backend.get_logs(req.include_buffer):
                await stream.send_message(LogLine(line=line))
                log_count += 1

            logger.debug(
                f"Streamed {log_count} log lines from backend '{req.backend_name}'"
            )

        except Exception as e:
            logger.error(f"Error streaming logs from backend '{req.backend_name}': {e}")
            raise

    @handle_errors
    async def FetchBackendConfig(
        self, stream: Stream[Backend, BackendConfig_pb2]
    ) -> None:
        """Fetch backend configuration"""
        req = await stream.recv_message()

        if req.name not in self._backends:
            raise GRPCError(Status.NOT_FOUND, f"Backend '{req.name}' not found")

        backend = self._backends[req.name]

        try:
            config = backend.get_config()
            await stream.send_message(
                BackendConfig_pb2(
                    configuration=config, config_format=backend.config_format
                )
            )
            logger.info(f"Fetched config for backend '{req.name}'")

        except Exception as e:
            logger.error(f"Error fetching config for backend '{req.name}': {e}")
            raise GRPCError(Status.INTERNAL, f"Failed to fetch config: {str(e)}")

    @handle_errors
    async def RestartBackend(
        self, stream: Stream[RestartBackendRequest, Empty]
    ) -> None:
        """Restart backend with proper locking and validation"""
        if not self._service_healthy:
            raise GRPCError(Status.UNAVAILABLE, "Service is currently unhealthy")

        message = await stream.recv_message()
        backend_name = message.backend_name

        if backend_name not in self._backends:
            raise GRPCError(Status.NOT_FOUND, f"Backend '{backend_name}' not found")

        # Use backend-specific lock to prevent concurrent restarts
        async with self._backend_locks[backend_name]:
            try:
                backend = self._backends[backend_name]

                # Determine if config has changed
                config_changed = False
                new_config = None

                if message.config and message.config.configuration:
                    current_config = backend.get_config()
                    new_config = message.config.configuration

                    # Normalize configs for comparison (remove whitespace differences)
                    current_normalized = json.dumps(
                        json.loads(current_config), sort_keys=True
                    )
                    new_normalized = json.dumps(json.loads(new_config), sort_keys=True)

                    config_changed = current_normalized != new_normalized

                if config_changed:
                    logger.info(
                        f"Restarting backend '{backend_name}' with new configuration"
                    )
                else:
                    logger.info(
                        f"Restarting backend '{backend_name}' with current configuration"
                    )

                # Perform restart with timeout
                await asyncio.wait_for(
                    backend.restart(new_config if config_changed else None),
                    timeout=60.0,  # 60 second timeout for restart
                )

                # Wait a bit for backend to stabilize
                await asyncio.sleep(2)

                # Verify backend is running after restart
                if not backend.running:
                    raise Exception("Backend not running after restart")

                logger.info(f"Backend '{backend_name}' restarted successfully")
                await stream.send_message(Empty())

            except asyncio.TimeoutError:
                logger.error(f"Timeout restarting backend '{backend_name}'")
                raise GRPCError(Status.DEADLINE_EXCEEDED, "Restart operation timed out")
            except Exception as e:
                logger.error(
                    f"Failed to restart backend '{backend_name}': {e}", exc_info=True
                )
                raise GRPCError(Status.INTERNAL, f"Restart failed: {str(e)}")

    @handle_errors
    async def GetBackendStats(self, stream: Stream[Backend, BackendStats]):
        """Get backend statistics with comprehensive health check"""
        if not self._service_healthy:
            raise GRPCError(Status.UNAVAILABLE, "Service is currently unhealthy")

        backend_req = await stream.recv_message()

        if backend_req.name not in self._backends:
            raise GRPCError(Status.NOT_FOUND, f"Backend '{backend_req.name}' not found")

        try:
            backend = self._backends[backend_req.name]
            running = backend.running

            # Perform comprehensive health check
            if running:
                try:
                    # Check if version is available (indicates backend is responsive)
                    version = backend.version
                    if not version:
                        running = False
                        logger.warning(
                            f"Backend '{backend_req.name}' has no version, marking as not running"
                        )

                    # Check health status
                    health_status = backend.health_status
                    if health_status.value != "healthy":
                        logger.warning(
                            f"Backend '{backend_req.name}' health status: {health_status.value}"
                        )
                        # Still report as running but log the health issue

                except asyncio.TimeoutError:
                    running = False
                    logger.warning(
                        f"Backend '{backend_req.name}' health check timed out"
                    )
                except Exception as e:
                    running = False
                    logger.warning(
                        f"Backend '{backend_req.name}' health check failed: {e}"
                    )

            logger.debug(f"Backend '{backend_req.name}' stats: running={running}")
            await stream.send_message(BackendStats(running=running))

        except Exception as e:
            logger.error(f"Error getting stats for backend '{backend_req.name}': {e}")
            # Return not running status on error
            await stream.send_message(BackendStats(running=False))
