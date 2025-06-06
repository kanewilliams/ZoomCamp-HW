"""
Configuration Settings Management

LEARNING OBJECTIVES:
1. Centralized configuration management
2. Environment-specific configurations
3. Configuration validation and type safety
4. Integration with external config files

TODO FOR STUDENT:
1. [ ] Add environment variable override support
2. [ ] Implement configuration caching
3. [ ] Add configuration change detection
4. [ ] Create configuration documentation generation
"""

import yaml
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
import logging
import os

logger = logging.getLogger(__name__)


@dataclass
class ScrapingSettings:
    """Scraping-specific configuration."""
    respect_robots_txt: bool = True
    default_delay: float = 2.0
    max_delay: float = 5.0
    timeout_seconds: int = 30
    max_retries: int = 3
    user_agent: str = "Mozilla/5.0 (compatible; HorseRacing-MLOps-Student/1.0)"


@dataclass
class TABSettings:
    """TAB.nz specific settings."""
    base_url: str = "https://www.tab.co.nz"
    delay_between_requests: float = 2.0
    max_delay: float = 4.0
    urls: Dict[str, str] = field(default_factory=dict)
    selectors: Dict[str, str] = field(default_factory=dict)
    rate_limit: Dict[str, int] = field(default_factory=dict)


@dataclass
class DataQualitySettings:
    """Data quality thresholds and validation rules."""
    min_races_per_day: int = 5
    max_races_per_day: int = 50
    required_fields: List[str] = field(default_factory=list)
    known_nz_tracks: List[str] = field(default_factory=list)
    max_data_age_hours: int = 25


@dataclass
class ExportSettings:
    """Data export configuration."""
    output_directory: str = "processed_data"
    default_formats: List[str] = field(default_factory=lambda: ["parquet", "csv"])
    compression: str = "snappy"
    retention_days: int = 30


class Settings:
    """
    Main settings class that loads configuration from YAML files.
    
    LEARNING SHOWCASE: How to build a robust configuration system
    
    Features demonstrated:
    - YAML configuration loading
    - Environment-specific overrides
    - Configuration validation
    - Default value handling
    - Type-safe configuration access
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        if config_dir is None:
            # Default to config directory relative to this file
            config_dir = Path(__file__).parent.parent.parent / "config"
        
        self.config_dir = Path(config_dir)
        self._load_configurations()
    
    def _load_configurations(self):
        """Load all configuration files."""
        try:
            # Load scraping configuration
            scraping_config = self._load_yaml_file("scraping.yaml")
            self._parse_scraping_config(scraping_config)
            
            # Load data sources configuration
            data_sources_config = self._load_yaml_file("data_sources.yaml")
            self._parse_data_sources_config(data_sources_config)
            
            logger.info("✅ Configuration loaded successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to load configuration: {e}")
            # Use default settings as fallback
            self._use_default_settings()
    
    def _load_yaml_file(self, filename: str) -> Dict[str, Any]:
        """Load a YAML configuration file."""
        file_path = self.config_dir / filename
        
        if not file_path.exists():
            logger.warning(f"Configuration file not found: {file_path}")
            return {}
        
        try:
            with open(file_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML file {filename}: {e}")
            return {}
    
    def _parse_scraping_config(self, config: Dict[str, Any]):
        """Parse scraping configuration into typed settings."""
        scraping_section = config.get('scraping', {})
        
        # Global scraping settings
        global_config = scraping_section.get('global', {})
        self.scraping = ScrapingSettings(
            respect_robots_txt=global_config.get('respect_robots_txt', True),
            default_delay=global_config.get('default_delay', 2.0),
            max_delay=global_config.get('max_delay', 5.0),
            timeout_seconds=global_config.get('timeout_seconds', 30),
            max_retries=global_config.get('max_retries', 3),
            user_agent=global_config.get('user_agent', "HorseRacing-MLOps-Student/1.0")
        )
        
        # TAB.nz specific settings
        tab_config = scraping_section.get('tab_nz', {})
        self.tab = TABSettings(
            base_url=tab_config.get('base_url', 'https://www.tab.co.nz'),
            delay_between_requests=tab_config.get('delay_between_requests', 2.0),
            max_delay=tab_config.get('max_delay', 4.0),
            urls=tab_config.get('urls', {}),
            selectors=tab_config.get('selectors', {}),
            rate_limit=tab_config.get('rate_limit', {})
        )
        
        # Data quality settings
        quality_config = config.get('data_quality', {})
        self.data_quality = DataQualitySettings(
            min_races_per_day=quality_config.get('min_races_per_day', 5),
            max_races_per_day=quality_config.get('max_races_per_day', 50),
            required_fields=quality_config.get('required_fields', []),
            known_nz_tracks=quality_config.get('known_nz_tracks', []),
            max_data_age_hours=quality_config.get('max_data_age_hours', 25)
        )
        
        # Source priority
        self.source_priority = config.get('source_priority', ['tab_nz'])
    
    def _parse_data_sources_config(self, config: Dict[str, Any]):
        """Parse data sources configuration."""
        self.public_apis = config.get('public_apis', {})
        self.commercial_apis = config.get('commercial_apis', {})
        self.feeds = config.get('feeds', {})
        self.bulk_data_sources = config.get('bulk_data_sources', {})
        
        # Export settings
        export_config = config.get('export_configs', {})
        self.export = ExportSettings(
            output_directory=export_config.get('output_directory', 'processed_data'),
            default_formats=export_config.get('default_formats', ['parquet', 'csv']),
            compression=export_config.get('compression', 'snappy'),
            retention_days=export_config.get('retention_days', 30)
        )
        
        # ML configuration
        self.ml_configs = config.get('ml_configs', {})
        
        # Integrations
        self.integrations = config.get('integrations', {})
    
    def _use_default_settings(self):
        """Use default settings as fallback."""
        logger.info("Using default configuration settings")
        
        self.scraping = ScrapingSettings()
        self.tab = TABSettings()
        self.data_quality = DataQualitySettings()
        self.export = ExportSettings()
        
        self.source_priority = ['tab_nz']
        self.public_apis = {}
        self.commercial_apis = {}
        self.feeds = {}
        self.bulk_data_sources = {}
        self.ml_configs = {}
        self.integrations = {}
    
    def get_scraper_config(self, source: str) -> Dict[str, Any]:
        """
        Get configuration for a specific scraper source.
        
        TODO FOR STUDENT: Extend this for other sources
        """
        if source == 'tab_nz':
            return {
                'base_url': self.tab.base_url,
                'delay': self.tab.delay_between_requests,
                'max_delay': self.tab.max_delay,
                'urls': self.tab.urls,
                'selectors': self.tab.selectors,
                'rate_limit': self.tab.rate_limit,
                'user_agent': self.scraping.user_agent,
                'timeout': self.scraping.timeout_seconds,
                'retries': self.scraping.max_retries
            }
        else:
            # Return default configuration for unknown sources
            return {
                'delay': self.scraping.default_delay,
                'max_delay': self.scraping.max_delay,
                'user_agent': self.scraping.user_agent,
                'timeout': self.scraping.timeout_seconds,
                'retries': self.scraping.max_retries
            }
    
    def validate_configuration(self) -> List[str]:
        """
        Validate configuration settings.
        
        TODO FOR STUDENT: Add comprehensive validation rules
        """
        errors = []
        
        # Validate TAB URLs and selectors
        if not self.tab.urls:
            errors.append("TAB URLs not configured - scraping will fail")
        
        if not self.tab.selectors:
            errors.append("TAB CSS selectors not configured - parsing will fail")
        
        # Validate data quality settings
        if self.data_quality.min_races_per_day <= 0:
            errors.append("Minimum races per day must be positive")
        
        if self.data_quality.max_races_per_day <= self.data_quality.min_races_per_day:
            errors.append("Maximum races per day must be greater than minimum")
        
        # Validate export settings
        if not self.export.default_formats:
            errors.append("No export formats configured")
        
        return errors
    
    def to_dict(self) -> Dict[str, Any]:
        """Export configuration as dictionary for debugging."""
        return {
            'scraping': self.scraping.__dict__,
            'tab': self.tab.__dict__,
            'data_quality': self.data_quality.__dict__,
            'export': self.export.__dict__,
            'source_priority': self.source_priority,
        }


# Global settings instance
_settings: Optional[Settings] = None


def get_settings(config_dir: Optional[Path] = None) -> Settings:
    """
    Get global settings instance (singleton pattern).
    
    LEARNING NOTE: Singleton pattern ensures configuration is loaded once
    and reused throughout the application.
    """
    global _settings
    
    if _settings is None:
        _settings = Settings(config_dir)
    
    return _settings


def reload_settings(config_dir: Optional[Path] = None) -> Settings:
    """
    Force reload of settings (useful for testing or config changes).
    
    TODO FOR STUDENT: Add configuration hot-reloading for development
    """
    global _settings
    _settings = Settings(config_dir)
    return _settings


# TODO FOR STUDENT - ENVIRONMENT VARIABLE OVERRIDES:
def get_env_override(key: str, default: Any = None) -> Any:
    """
    Get configuration value from environment variable.
    
    TODO FOR STUDENT: Implement environment variable override system
    Example:
    - RACING_SCRAPER_DELAY -> overrides scraping.default_delay
    - RACING_TAB_BASE_URL -> overrides tab.base_url
    """
    env_key = f"RACING_{key.upper().replace('.', '_')}"
    return os.getenv(env_key, default)


if __name__ == "__main__":
    """Test configuration loading."""
    
    print("🔧 Testing Configuration Loading")
    print("=" * 40)
    
    settings = get_settings()
    
    print("Configuration Summary:")
    print(f"  TAB Base URL: {settings.tab.base_url}")
    print(f"  Default Delay: {settings.scraping.default_delay}s")
    print(f"  Known Tracks: {len(settings.data_quality.known_nz_tracks)}")
    print(f"  Export Formats: {settings.export.default_formats}")
    
    print("\\nConfiguration Validation:")
    errors = settings.validate_configuration()
    if errors:
        print("❌ Configuration errors found:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("✅ Configuration is valid")
    
    print("\\n🎯 TODO: Update config files with real TAB.nz selectors!")