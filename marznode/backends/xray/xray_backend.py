"""What a vpn server should do"""

import asyncio
import json
import logging
from collections import defaultdict

from marznode.backends.abstract_backend import VPNBackend
from marznode.backends.xray._config import XrayConfig
from marznode.backends.xray._runner import XrayCore
from marznode.backends.xray.api import XrayAPI
from marznode.backends.xray.api.exceptions import (
    EmailExistsError,
    EmailNotFoundError,
    TagNotFoundError,
)
from marznode.backends.xray.api.types.account import accounts_map
from marznode.config import XRAY_RESTART_ON_FAILURE, XRAY_RESTART_ON_FAILURE_INTERVAL
from marznode.models import User, Inbound
from marznode.storage import BaseStorage
from marznode.utils.network import find_free_port

logger = logging.getLogger(__name__)


class XrayBackend(VPNBackend):
    backend_type = "xray"
    config_format = 1

    def __init__(
        self,
        executable_path: str,
        assets_path: str,
        config_path: str,
        storage: BaseStorage,
    ):
        super().__init__()  # Initialize health monitor
        self._config = None
        self._inbound_tags = set()
        self._inbounds = list()
        self._api = None
        self._runner = XrayCore(executable_path, assets_path)
        self._storage = storage
        self._config_path = config_path
        self._restart_lock = asyncio.Lock()
        
        # Start background tasks
        asyncio.create_task(self._restart_on_failure())

    @property
    def running(self) -> bool:
        return self._runner.running

    @property
    def version(self):
        return self._runner.version
    
    def _check_health(self):
        """Simple health check"""
        if not self._runner.running:
            self._set_unhealthy("Xray process not running")
            return False
        
        if self._api:
            try:
                # This is a simple sync check - in real implementation you'd want async
                self._set_healthy()
                return True
            except Exception as e:
                self._set_unhealthy(f"API not responsive: {e}")
                return False
        
        self._set_healthy()
        return True

    def contains_tag(self, tag: str) -> bool:
        return tag in self._inbound_tags

    def list_inbounds(self) -> list:
        return self._inbounds

    def get_config(self) -> str:
        with open(self._config_path) as f:
            return f.read()

    def save_config(self, config: str) -> None:
        with open(self._config_path, "w") as f:
            f.write(config)

    async def add_storage_users(self):
        for inbound in self._inbounds:
            for user in await self._storage.list_inbound_users(inbound.tag):
                await self.add_user(user, inbound)

    async def _restart_on_failure(self):
        while True:
            try:
                await self._runner.stop_event.wait()
                self._runner.stop_event.clear()
                
                if self._restart_lock.locked():
                    logger.debug("Xray restarting as planned")
                else:
                    logger.warning("Xray stopped unexpectedly")
                    if XRAY_RESTART_ON_FAILURE:
                        logger.info(f"Waiting {XRAY_RESTART_ON_FAILURE_INTERVAL}s before restart")
                        await asyncio.sleep(XRAY_RESTART_ON_FAILURE_INTERVAL)
                        
                        try:
                            # Try to restart with current config
                            await self.start()
                            await self.add_storage_users()
                            logger.info("Xray restarted successfully after failure")
                        except Exception as e:
                            logger.error(f"Failed to restart Xray after failure: {e}")
                            # Wait longer before next attempt
                            await asyncio.sleep(30)
                            
            except asyncio.CancelledError:
                logger.debug("Restart on failure task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in restart on failure handler: {e}")
                await asyncio.sleep(10)  # Wait before retrying

    async def start(self, backend_config: str | None = None):
        if backend_config is None:
            with open(self._config_path) as f:
                backend_config = f.read()
        else:
            self.save_config(json.dumps(json.loads(backend_config), indent=2))
        xray_api_port = find_free_port()
        self._config = XrayConfig(backend_config, api_port=xray_api_port)
        self._config.register_inbounds(self._storage)
        self._inbound_tags = {i["tag"] for i in self._config.inbounds}
        self._inbounds = list(self._config.list_inbounds())
        self._api = XrayAPI("127.0.0.1", xray_api_port)
        await self._runner.start(self._config)

    async def stop(self):
        try:
            # Stop the runner
            await self._runner.stop()
            
            # Clean up storage
            for tag in self._inbound_tags:
                self._storage.remove_inbound(tag)
            self._inbound_tags = set()
            self._inbounds = []
            
            self._set_unhealthy("Backend stopped")
            logger.info("Xray backend stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping Xray backend: {e}")
            raise

    async def restart(self, backend_config: str | None) -> list[Inbound] | None:
        async with self._restart_lock:
            try:
                logger.info("Starting backend restart process")
                
                # Store current users before restart
                current_users = []
                for inbound in self._inbounds:
                    users = await self._storage.list_inbound_users(inbound.tag)
                    current_users.extend(users)
                
                if not backend_config:
                    # Restart with current config
                    logger.info("Restarting with current configuration")
                    result = await self._runner.restart(self._config)
                    
                    # Re-add users after restart
                    await self.add_storage_users()
                    return result
                else:
                    # Restart with new config
                    logger.info("Restarting with new configuration")
                    await self.stop()
                    await self.start(backend_config)
                    
                    # Re-add users after restart
                    await self.add_storage_users()
                    
                logger.info("Backend restart completed successfully")
                return None
                
            except Exception as e:
                logger.error(f"Backend restart failed: {e}")
                # Try to recover by starting with current config
                try:
                    if not self.running:
                        logger.info("Attempting recovery start")
                        await self.start()
                        await self.add_storage_users()
                except Exception as recovery_error:
                    logger.error(f"Recovery start failed: {recovery_error}")
                raise

    async def add_user(self, user: User, inbound: Inbound):
        email = f"{user.id}.{user.username}"

        # Check if backend is running before attempting to add user
        if not self.running:
            logger.warning(f"Cannot add user {user.username}: backend is not running")
            return

        try:
            account_class = accounts_map[inbound.protocol]
            flow = inbound.config.get("flow", "") or ""
            logger.debug(f"Adding user {user.username} with flow: {flow}")
            
            user_account = account_class(
                email=email,
                seed=user.key,
                flow=flow,
            )

            await self._api.add_inbound_user(inbound.tag, user_account)
            logger.debug(f"Successfully added user {user.username} to inbound {inbound.tag}")
            
        except EmailExistsError:
            logger.debug(f"User {user.username} already exists in inbound {inbound.tag}")
            # This is not an error, user already exists
        except TagNotFoundError:
            logger.error(f"Inbound tag {inbound.tag} not found when adding user {user.username}")
            raise
        except (OSError, ConnectionError) as e:
            logger.warning(f"Connection error adding user {user.username}: {e}")
            self._set_unhealthy(f"Connection error: {e}")
            # Don't raise, let it be retried later
        except Exception as e:
            logger.error(f"Unexpected error adding user {user.username}: {e}")
            raise

    async def remove_user(self, user: User, inbound: Inbound):
        email = f"{user.id}.{user.username}"
        
        # Check if backend is running before attempting to remove user
        if not self.running:
            logger.warning(f"Cannot remove user {user.username}: backend is not running")
            return

        try:
            await self._api.remove_inbound_user(inbound.tag, email)
            logger.debug(f"Successfully removed user {user.username} from inbound {inbound.tag}")
            
        except EmailNotFoundError:
            logger.debug(f"User {user.username} not found in inbound {inbound.tag}")
            # This is not an error, user doesn't exist
        except TagNotFoundError:
            logger.error(f"Inbound tag {inbound.tag} not found when removing user {user.username}")
            raise
        except (OSError, ConnectionError) as e:
            logger.warning(f"Connection error removing user {user.username}: {e}")
            self._set_unhealthy(f"Connection error: {e}")
            # Don't raise, user will be cleaned up on next restart
        except Exception as e:
            logger.error(f"Unexpected error removing user {user.username}: {e}")
            raise

    async def get_usages(self, reset: bool = True) -> dict[int, int]:
        try:
            api_stats = await self._api.get_users_stats(reset=reset)
        except OSError:
            api_stats = []
        stats = defaultdict(int)
        for stat in api_stats:
            uid = int(stat.name.split(".")[0])
            stats[uid] += stat.value
        return stats

    async def get_logs(self, include_buffer: bool = True):
        if include_buffer:
            for line in self._runner.get_buffer():
                yield line
        log_stm = self._runner.get_logs_stm()
        async with log_stm:
            async for line in log_stm:
                yield line
