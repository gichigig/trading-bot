"""
Strategy Registry
==================

Central registry for strategy plugins with discovery and instantiation.
"""

from typing import Any, Callable, Dict, List, Optional, Type
import logging
import importlib
import pkgutil
from pathlib import Path

from trading_bot.strategies.base import BaseStrategy, StrategyMetadata

logger = logging.getLogger(__name__)


class StrategyRegistry:
    """
    Central registry for all available strategies.
    
    Supports:
    - Registration via decorator
    - Dynamic discovery from directories
    - Version tracking
    - Factory pattern for instantiation
    """
    
    _instance: Optional['StrategyRegistry'] = None
    
    def __new__(cls) -> 'StrategyRegistry':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._strategies: Dict[str, Type[BaseStrategy]] = {}
            cls._instance._metadata: Dict[str, StrategyMetadata] = {}
        return cls._instance
    
    def register(
        self,
        strategy_class: Type[BaseStrategy],
        name: Optional[str] = None,
    ) -> None:
        """Register a strategy class."""
        # Create temporary instance to get metadata
        temp_instance = strategy_class.__new__(strategy_class)
        temp_instance._parameters = {}
        metadata = temp_instance._define_metadata()
        
        strategy_name = name or metadata.name
        strategy_key = f"{strategy_name}:{metadata.version}"
        
        self._strategies[strategy_key] = strategy_class
        self._metadata[strategy_key] = metadata
        
        # Also register without version for latest
        self._strategies[strategy_name] = strategy_class
        self._metadata[strategy_name] = metadata
        
        logger.info(f"Registered strategy: {strategy_key}")
    
    def unregister(self, name: str, version: Optional[str] = None) -> bool:
        """Unregister a strategy."""
        key = f"{name}:{version}" if version else name
        if key in self._strategies:
            del self._strategies[key]
            del self._metadata[key]
            logger.info(f"Unregistered strategy: {key}")
            return True
        return False
    
    def get_strategy_class(
        self,
        name: str,
        version: Optional[str] = None,
    ) -> Optional[Type[BaseStrategy]]:
        """Get a strategy class by name and optional version."""
        key = f"{name}:{version}" if version else name
        return self._strategies.get(key)
    
    def create_strategy(
        self,
        name: str,
        version: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Optional[BaseStrategy]:
        """Factory method to create a strategy instance."""
        strategy_class = self.get_strategy_class(name, version)
        
        if strategy_class is None:
            logger.error(f"Strategy not found: {name} v{version}")
            return None
        
        try:
            instance = strategy_class(parameters=parameters, **kwargs)
            logger.info(f"Created strategy instance: {name} v{instance.version}")
            return instance
        except Exception as e:
            logger.error(f"Failed to create strategy {name}: {e}", exc_info=True)
            return None
    
    def get_all_strategies(self) -> List[str]:
        """Get list of all registered strategy names."""
        # Return unique names (without version suffix)
        names = set()
        for key in self._strategies.keys():
            name = key.split(":")[0]
            names.add(name)
        return sorted(names)
    
    def get_strategy_versions(self, name: str) -> List[str]:
        """Get all versions of a strategy."""
        versions = []
        for key in self._strategies.keys():
            if key.startswith(f"{name}:"):
                version = key.split(":")[1]
                versions.append(version)
        return sorted(versions)
    
    def get_strategy_metadata(
        self,
        name: str,
        version: Optional[str] = None,
    ) -> Optional[StrategyMetadata]:
        """Get metadata for a strategy."""
        key = f"{name}:{version}" if version else name
        return self._metadata.get(key)
    
    def get_strategies_by_category(self, category: str) -> List[str]:
        """Get strategies filtered by category."""
        results = []
        for name, metadata in self._metadata.items():
            if metadata.category == category and ":" not in name:
                results.append(name)
        return results
    
    def discover_strategies(self, path: str) -> int:
        """
        Discover and register strategies from a directory.
        Returns number of strategies discovered.
        """
        discovered = 0
        strategies_path = Path(path)
        
        if not strategies_path.exists():
            logger.warning(f"Strategy path does not exist: {path}")
            return 0
        
        # Import all Python files in directory
        for module_info in pkgutil.iter_modules([str(strategies_path)]):
            try:
                module = importlib.import_module(f"strategies.{module_info.name}")
                
                # Look for strategy classes in module
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (
                        isinstance(attr, type)
                        and issubclass(attr, BaseStrategy)
                        and attr is not BaseStrategy
                    ):
                        self.register(attr)
                        discovered += 1
                        
            except Exception as e:
                logger.error(f"Failed to load strategy module {module_info.name}: {e}")
        
        logger.info(f"Discovered {discovered} strategies from {path}")
        return discovered
    
    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics."""
        categories: Dict[str, int] = {}
        for metadata in self._metadata.values():
            cat = metadata.category or "uncategorized"
            categories[cat] = categories.get(cat, 0) + 1
        
        return {
            "total_strategies": len(self.get_all_strategies()),
            "total_versions": len(self._strategies),
            "categories": categories,
        }


# Global registry instance
_registry = StrategyRegistry()


def register_strategy(cls: Type[BaseStrategy]) -> Type[BaseStrategy]:
    """Decorator to register a strategy class."""
    _registry.register(cls)
    return cls


def get_strategy(
    name: str,
    version: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
) -> Optional[BaseStrategy]:
    """Get a strategy instance from the registry."""
    return _registry.create_strategy(name, version, parameters)


def list_strategies() -> List[str]:
    """List all registered strategy names."""
    return _registry.get_all_strategies()


def get_registry() -> StrategyRegistry:
    """Get the global strategy registry."""
    return _registry
