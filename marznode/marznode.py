"""Start up and run marznode with enhanced error handling"""

import logging
import os
import sys
import asyncio
import signal

from grpclib.health.service import Health
from grpclib.server import Server
from grpclib.utils import graceful_exit

from marznode import config
from marznode.backends.hysteria2.hysteria2_backend import HysteriaBackend
from marznode.backends.singbox.singbox_backend import SingBoxBackend
from marznode.backends.xray.xray_backend import XrayBackend
from marznode.config import (
    HYSTERIA_EXECUTABLE_PATH,
    HYSTERIA_CONFIG_PATH,
    XRAY_CONFIG_PATH,
    HYSTERIA_ENABLED,
    XRAY_ENABLED,
    XRAY_EXECUTABLE_PATH,
    XRAY_ASSETS_PATH,
    SING_BOX_ENABLED,
    SING_BOX_EXECUTABLE_PATH,
    SING_BOX_CONFIG_PATH,
)
from marznode.service import MarzService
from marznode.storage import MemoryStorage
from marznode.utils.ssl import generate_keypair, create_secure_context

logger = logging.getLogger(__name__)


class MarzNodeApp:
    """Main application class for managing Marznode lifecycle"""

    def __init__(self):
        self.storage = None
        self.backends = {}
        self.server = None
        self.health_service = None
        self.marz_service = None
        self.shutdown_event = asyncio.Event()

    async def setup_logging(self):
        """Setup logging configuration"""
        log_level = logging.DEBUG if config.DEBUG else logging.INFO

        logging.basicConfig(
            level=log_level,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)],
        )

        logger.info(f"Logging initialized at {logging.getLevelName(log_level)} level")

    async def setup_ssl(self):
        """Setup SSL context for secure communication"""
        if config.INSECURE:
            logger.warning("Running in INSECURE mode - SSL disabled!")
            return None

        # Check and generate server certificates if needed
        if not all(
            (os.path.isfile(config.SSL_CERT_FILE), os.path.isfile(config.SSL_KEY_FILE))
        ):
            logger.info("Generating server keypair for Marznode")
            try:
                generate_keypair(config.SSL_KEY_FILE, config.SSL_CERT_FILE)
                logger.info("Server keypair generated successfully")
            except Exception as e:
                logger.error(f"Failed to generate keypair: {e}")
                raise

        # Check client certificate
        if not os.path.isfile(config.SSL_CLIENT_CERT_FILE):
            logger.error(f"Client certificate not found: {config.SSL_CLIENT_CERT_FILE}")
            logger.error("Cannot start without client certificate for authentication")
            sys.exit(1)

        # Create SSL context
        try:
            ssl_context = create_secure_context(
                config.SSL_CERT_FILE,
                config.SSL_KEY_FILE,
                trusted=config.SSL_CLIENT_CERT_FILE,
            )
            logger.info("SSL context created successfully")
            return ssl_context
        except Exception as e:
            logger.error(f"Failed to create SSL context: {e}")
            raise

    async def initialize_storage(self):
        """Initialize storage backend"""
        logger.info("Initializing storage backend...")
        self.storage = MemoryStorage()
        logger.info("Storage backend initialized")

    async def initialize_backends(self):
        """Initialize all enabled VPN backends"""
        logger.info("Initializing VPN backends...")

        # Initialize Xray backend
        if XRAY_ENABLED:
            try:
                logger.info("Starting Xray backend...")
                xray_backend = XrayBackend(
                    XRAY_EXECUTABLE_PATH,
                    XRAY_ASSETS_PATH,
                    XRAY_CONFIG_PATH,
                    self.storage,
                )
                await xray_backend.start()
                self.backends["xray"] = xray_backend
                logger.info("Xray backend started successfully")
            except Exception as e:
                logger.error(f"Failed to start Xray backend: {e}")
                raise

        # Initialize Hysteria2 backend
        if HYSTERIA_ENABLED:
            try:
                logger.info("Starting Hysteria2 backend...")
                hysteria_backend = HysteriaBackend(
                    HYSTERIA_EXECUTABLE_PATH, HYSTERIA_CONFIG_PATH, self.storage
                )
                await hysteria_backend.start()
                self.backends["hysteria2"] = hysteria_backend
                logger.info("Hysteria2 backend started successfully")
            except Exception as e:
                logger.error(f"Failed to start Hysteria2 backend: {e}")
                # Continue with other backends
                logger.warning("Continuing without Hysteria2 backend")

        # Initialize Sing-Box backend
        if SING_BOX_ENABLED:
            try:
                logger.info("Starting Sing-Box backend...")
                sing_box_backend = SingBoxBackend(
                    SING_BOX_EXECUTABLE_PATH, SING_BOX_CONFIG_PATH, self.storage
                )
                await sing_box_backend.start()
                self.backends["sing-box"] = sing_box_backend
                logger.info("Sing-Box backend started successfully")
            except Exception as e:
                logger.error(f"Failed to start Sing-Box backend: {e}")
                # Continue with other backends
                logger.warning("Continuing without Sing-Box backend")

        if not self.backends:
            logger.error("No backends were successfully initialized!")
            raise RuntimeError("No backends available")

        logger.info(
            f"Successfully initialized {len(self.backends)} backend(s): {list(self.backends.keys())}"
        )

    async def initialize_services(self):
        """Initialize gRPC services"""
        logger.info("Initializing gRPC services...")

        # Create health service
        self.health_service = Health()

        # Create main service
        self.marz_service = MarzService(self.storage, self.backends)

        logger.info("gRPC services initialized")

    async def start_server(self, ssl_context):
        """Start the gRPC server"""
        logger.info("Starting gRPC server...")

        # Create server with both services
        self.server = Server([self.marz_service, self.health_service])

        # Start server
        await self.server.start(
            config.SERVICE_ADDRESS, config.SERVICE_PORT, ssl=ssl_context
        )

        logger.info(
            f"✓ Marznode server running on {config.SERVICE_ADDRESS}:{config.SERVICE_PORT}"
        )

        # Set health status to serving
        for backend_name in self.backends.keys():
            self.health_service.set(
                f"backend.{backend_name}", self.health_service.SERVING
            )
        self.health_service.set("", self.health_service.SERVING)

        logger.info("Health status set to SERVING")

    async def wait_for_shutdown(self):
        """Wait for shutdown signal"""
        logger.info("Server is ready and waiting for requests...")

        # Wait for server to close
        await self.server.wait_closed()

        logger.info("Server shutdown initiated")

    async def shutdown_backends(self):
        """Shutdown all backends gracefully"""
        logger.info("Shutting down backends...")

        for backend_name, backend in self.backends.items():
            try:
                logger.info(f"Stopping backend: {backend_name}")

                # Call cleanup if available
                if hasattr(backend, "cleanup"):
                    await asyncio.wait_for(backend.cleanup(), timeout=30.0)
                else:
                    await asyncio.wait_for(backend.stop(), timeout=30.0)

                logger.info(f"Backend {backend_name} stopped successfully")

            except asyncio.TimeoutError:
                logger.error(f"Timeout stopping backend {backend_name}")
            except Exception as e:
                logger.error(f"Error stopping backend {backend_name}: {e}")

        logger.info("All backends shutdown complete")

    async def shutdown_server(self):
        """Shutdown the gRPC server"""
        if self.server:
            logger.info("Shutting down gRPC server...")

            # Update health status
            if self.health_service:
                self.health_service.set("", self.health_service.NOT_SERVING)
                for backend_name in self.backends.keys():
                    self.health_service.set(
                        f"backend.{backend_name}", self.health_service.NOT_SERVING
                    )

            # Wait for server to close (graceful_exit handles this)
            logger.info("gRPC server shutdown complete")

    async def run(self):
        """Main run method"""
        try:
            # Setup logging
            await self.setup_logging()

            # Setup SSL
            ssl_context = await self.setup_ssl()

            # Initialize components
            await self.initialize_storage()
            await self.initialize_backends()
            await self.initialize_services()

            # Start server
            await self.start_server(ssl_context)

            # Wait for shutdown
            await self.wait_for_shutdown()

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Fatal error during execution: {e}", exc_info=True)
            raise
        finally:
            # Cleanup
            await self.shutdown_backends()
            await self.shutdown_server()
            logger.info("Marznode shutdown complete")


async def main():
    """Main entry point with graceful exit handling"""
    app = MarzNodeApp()

    # Setup graceful exit for the server
    with graceful_exit([app.server] if app.server else []):
        await app.run()


def entrypoint():
    """Entry point for the application"""
    try:
        # Run the async main function
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Application failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    entrypoint()
