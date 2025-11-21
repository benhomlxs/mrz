"""Storage backend for storing marznode data in memory"""

import asyncio
import logging
from typing import Optional
from .base import BaseStorage
from ..models import User, Inbound

logger = logging.getLogger(__name__)


class MemoryStorage(BaseStorage):
    """
    A storage backend for marznode.
    Note: This isn't fit for production use since data gets wiped on restarts.
    If Marzneshin is down, users are lost until it gets back up.
    """

    def __init__(self):
        self.storage = {"users": {}, "inbounds": {}}
        self._lock = asyncio.Lock()
        logger.info("MemoryStorage initialized")

    async def list_users(
        self, user_id: Optional[int] = None
    ) -> list[User] | User | None:
        """
        List users from storage

        Args:
            user_id: Optional user ID to fetch specific user

        Returns:
            Single user if user_id provided, list of all users otherwise
        """
        async with self._lock:
            if user_id is not None:
                return self.storage["users"].get(user_id)
            return list(self.storage["users"].values())

    async def list_inbounds(
        self, tag: Optional[list[str] | str] = None, include_users: bool = False
    ) -> list[Inbound] | Inbound | None:
        """
        List inbounds from storage

        Args:
            tag: Optional tag or list of tags to filter inbounds
            include_users: Whether to include user information (not used currently)

        Returns:
            Single inbound, list of inbounds, or None
        """
        async with self._lock:
            if tag is not None:
                if isinstance(tag, str):
                    # Return single inbound for string tag
                    return self.storage["inbounds"].get(tag)

                # Return list of inbounds for list of tags
                result = []
                for t in tag:
                    if t in self.storage["inbounds"]:
                        result.append(self.storage["inbounds"][t])
                    else:
                        logger.warning(f"Inbound tag '{t}' not found in storage")
                return result

            # Return all inbounds
            return list(self.storage["inbounds"].values())

    async def list_inbound_users(self, tag: str) -> list[User]:
        """
        List all users associated with a specific inbound

        Args:
            tag: Inbound tag to filter users

        Returns:
            List of users associated with the inbound
        """
        async with self._lock:
            users = []
            for user in self.storage["users"].values():
                # Check if user has this inbound
                for inbound in user.inbounds:
                    if inbound.tag == tag:
                        users.append(user)
                        break  # Found the inbound, no need to check other inbounds

            logger.debug(f"Found {len(users)} users for inbound '{tag}'")
            return users

    async def remove_user(self, user: User) -> None:
        """
        Remove a user from storage

        Args:
            user: User object to remove
        """
        async with self._lock:
            if user.id in self.storage["users"]:
                del self.storage["users"][user.id]
                logger.debug(
                    f"Removed user '{user.username}' (ID: {user.id}) from storage"
                )
            else:
                logger.warning(
                    f"Attempted to remove non-existent user '{user.username}' (ID: {user.id})"
                )

    async def update_user_inbounds(self, user: User, inbounds: list[Inbound]) -> None:
        """
        Update user's inbound associations

        Args:
            user: User object to update
            inbounds: New list of inbounds for the user
        """
        async with self._lock:
            # Create a copy of the user with updated inbounds
            user.inbounds = inbounds
            self.storage["users"][user.id] = user

            logger.debug(
                f"Updated user '{user.username}' (ID: {user.id}) with {len(inbounds)} inbounds: "
                f"{[i.tag for i in inbounds]}"
            )

    def register_inbound(self, inbound: Inbound) -> None:
        """
        Register an inbound in storage
        Note: This is called synchronously from config registration

        Args:
            inbound: Inbound object to register
        """
        self.storage["inbounds"][inbound.tag] = inbound
        logger.debug(
            f"Registered inbound '{inbound.tag}' with protocol '{inbound.protocol}'"
        )

    def remove_inbound(self, inbound: Inbound | str) -> None:
        """
        Remove an inbound from storage and clean up user associations
        Note: This is called synchronously from backend stop

        Args:
            inbound: Inbound object or tag string to remove
        """
        tag = inbound if isinstance(inbound, str) else inbound.tag

        # Remove the inbound
        if tag in self.storage["inbounds"]:
            self.storage["inbounds"].pop(tag)
            logger.debug(f"Removed inbound '{tag}' from storage")
        else:
            logger.warning(f"Attempted to remove non-existent inbound '{tag}'")

        # Clean up user associations
        removed_count = 0
        for user_id, user in list(self.storage["users"].items()):
            original_count = len(user.inbounds)
            user.inbounds = [inb for inb in user.inbounds if inb.tag != tag]

            if len(user.inbounds) < original_count:
                removed_count += 1

            # Remove user if they have no inbounds left
            if not user.inbounds:
                del self.storage["users"][user_id]
                logger.debug(f"Removed user '{user.username}' (no inbounds left)")

        if removed_count > 0:
            logger.debug(f"Cleaned up inbound '{tag}' from {removed_count} users")

    async def flush_users(self):
        """
        Remove all users from storage
        """
        async with self._lock:
            user_count = len(self.storage["users"])
            self.storage["users"] = {}
            logger.info(f"Flushed {user_count} users from storage")

    async def get_stats(self) -> dict:
        """
        Get storage statistics

        Returns:
            Dictionary with storage stats
        """
        async with self._lock:
            return {
                "total_users": len(self.storage["users"]),
                "total_inbounds": len(self.storage["inbounds"]),
                "inbound_tags": list(self.storage["inbounds"].keys()),
            }

    def __repr__(self) -> str:
        """String representation of storage"""
        return (
            f"MemoryStorage(users={len(self.storage['users'])}, "
            f"inbounds={len(self.storage['inbounds'])})"
        )
