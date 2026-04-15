"""
resolver/__init__.py — Clean exports for the entity resolution module.
"""

from maigret.resolver.linker import SplinkResolver
from maigret.resolver.config import ResolverConfig

__all__ = ["SplinkResolver", "ResolverConfig"]
