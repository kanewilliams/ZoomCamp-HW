"""
Data Processing Module - Polars Learning Hub

This module teaches you modern data processing with Polars:
- Schema validation and type safety
- High-performance transformations
- Feature engineering for horse racing
- Data quality monitoring

Learning progression:
1. validators.py - Schema validation and data quality
2. transformers.py - Feature engineering and cleaning  
3. exporters.py - Output formats and optimization

Polars vs Pandas comparison examples included throughout!
"""

from .validators import RaceDataValidator, validate_scraped_data
from .transformers import RaceDataTransformer
from .exporters import DataExporter

__all__ = [
    "RaceDataValidator", 
    "validate_scraped_data",
    "RaceDataTransformer", 
    "DataExporter"
]