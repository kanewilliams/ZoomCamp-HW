"""
Configuration Management Module

LEARNING OBJECTIVES:
1. Understand configuration as code principles
2. Learn environment-specific configuration management
3. Practice YAML/JSON configuration patterns
4. Implement configuration validation

This module provides centralized configuration management for the entire project.
"""

from .settings import Settings, get_settings
from .schemas import ConfigSchema

__all__ = ["Settings", "get_settings", "ConfigSchema"]