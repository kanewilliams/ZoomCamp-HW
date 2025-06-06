"""
Configuration Schema Definitions

LEARNING OBJECTIVES:
1. Understand schema validation for configuration
2. Learn about type safety in configuration management
3. Practice JSON Schema for validation
4. Implement configuration documentation

TODO FOR STUDENT: This is a foundation for configuration validation.
You can extend this with JSON Schema or Pydantic for more robust validation.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import json


@dataclass
class ConfigSchema:
    """
    Schema definitions for configuration validation.
    
    TODO FOR STUDENT: Expand this with comprehensive validation rules
    """
    
    @staticmethod
    def get_scraping_schema() -> Dict[str, Any]:
        """JSON Schema for scraping configuration."""
        return {
            "type": "object",
            "properties": {
                "scraping": {
                    "type": "object",
                    "properties": {
                        "global": {
                            "type": "object",
                            "properties": {
                                "respect_robots_txt": {"type": "boolean"},
                                "default_delay": {"type": "number", "minimum": 0.1},
                                "max_delay": {"type": "number", "minimum": 0.1},
                                "timeout_seconds": {"type": "integer", "minimum": 1},
                                "max_retries": {"type": "integer", "minimum": 0},
                                "user_agent": {"type": "string", "minLength": 1}
                            },
                            "required": ["respect_robots_txt", "default_delay"]
                        },
                        "tab_nz": {
                            "type": "object",
                            "properties": {
                                "base_url": {"type": "string", "format": "uri"},
                                "delay_between_requests": {"type": "number", "minimum": 0.1},
                                "urls": {"type": "object"},
                                "selectors": {"type": "object"},
                                "rate_limit": {"type": "object"}
                            },
                            "required": ["base_url"]
                        }
                    },
                    "required": ["global", "tab_nz"]
                },
                "data_quality": {
                    "type": "object",
                    "properties": {
                        "min_races_per_day": {"type": "integer", "minimum": 1},
                        "max_races_per_day": {"type": "integer", "minimum": 1},
                        "required_fields": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "known_nz_tracks": {
                            "type": "array", 
                            "items": {"type": "string"}
                        },
                        "max_data_age_hours": {"type": "integer", "minimum": 1}
                    }
                }
            },
            "required": ["scraping"]
        }
    
    @staticmethod
    def validate_config(config: Dict[str, Any]) -> List[str]:
        """
        Validate configuration against schema.
        
        TODO FOR STUDENT: Implement full JSON Schema validation
        This is a simplified version - consider using jsonschema library
        """
        errors = []
        
        # Basic validation examples
        scraping_config = config.get('scraping', {})
        
        if not scraping_config:
            errors.append("Missing 'scraping' section")
            return errors
        
        global_config = scraping_config.get('global', {})
        if 'default_delay' in global_config:
            if global_config['default_delay'] < 0.1:
                errors.append("default_delay must be at least 0.1 seconds")
        
        tab_config = scraping_config.get('tab_nz', {})
        if 'base_url' in tab_config:
            if not tab_config['base_url'].startswith('http'):
                errors.append("base_url must be a valid HTTP/HTTPS URL")
        
        # TODO FOR STUDENT: Add more validation rules
        
        return errors


# TODO FOR STUDENT - ADVANCED CONFIGURATION VALIDATION:
class ConfigValidator:
    """
    Advanced configuration validator.
    
    TODO FOR STUDENT: Implement using jsonschema or pydantic
    """
    
    @staticmethod
    def validate_with_jsonschema(config: Dict[str, Any]) -> List[str]:
        """
        Validate using JSON Schema library.
        
        TODO: pip install jsonschema, then implement:
        
        ```python
        import jsonschema
        
        schema = ConfigSchema.get_scraping_schema()
        try:
            jsonschema.validate(config, schema)
            return []
        except jsonschema.ValidationError as e:
            return [str(e)]
        ```
        """
        pass
    
    @staticmethod
    def validate_css_selectors(selectors: Dict[str, str]) -> List[str]:
        """
        Validate CSS selectors are properly formatted.
        
        TODO FOR STUDENT: Add CSS selector validation
        """
        errors = []
        
        for name, selector in selectors.items():
            if not selector or selector.strip() == "":
                errors.append(f"Empty CSS selector for {name}")
            
            # TODO: Add more CSS selector validation
            # - Check for valid CSS syntax
            # - Warn about overly specific selectors
            # - Suggest more robust selectors
        
        return errors
    
    @staticmethod
    def validate_track_names(tracks: List[str]) -> List[str]:
        """
        Validate track names against known NZ racing venues.
        
        TODO FOR STUDENT: Research and validate against official track list
        """
        errors = []
        
        # TODO: Load official track list and validate
        known_tracks = [
            "Ellerslie", "Trentham", "Riccarton", "Te Rapa", 
            "Awapuni", "Otaki", "Hastings", "New Plymouth"
            # Add more as you research them
        ]
        
        for track in tracks:
            if track not in known_tracks:
                errors.append(f"Unknown track: {track} - verify this is a valid NZ track")
        
        return errors


if __name__ == "__main__":
    """Test schema validation."""
    
    print("🔍 Testing Configuration Schema Validation")
    print("=" * 45)
    
    # Test with sample config
    sample_config = {
        "scraping": {
            "global": {
                "respect_robots_txt": True,
                "default_delay": 2.0,
                "max_delay": 5.0,
                "timeout_seconds": 30,
                "max_retries": 3,
                "user_agent": "HorseRacing-Student/1.0"
            },
            "tab_nz": {
                "base_url": "https://www.tab.co.nz",
                "delay_between_requests": 2.0
            }
        },
        "data_quality": {
            "min_races_per_day": 5,
            "max_races_per_day": 50,
            "known_nz_tracks": ["Ellerslie", "Trentham"]
        }
    }
    
    # Test validation
    errors = ConfigSchema.validate_config(sample_config)
    
    if errors:
        print("❌ Configuration validation errors:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("✅ Configuration validation passed")
    
    print("\\n📋 Schema structure:")
    schema = ConfigSchema.get_scraping_schema()
    print(json.dumps(schema, indent=2)[:500] + "...")
    
    print("\\n🎯 TODO: Implement full JSON Schema validation!")