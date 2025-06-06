"""
Prefect Flows Module - Orchestration Learning Hub

This module teaches you workflow orchestration with Prefect:
- Task dependency management
- Error handling and retries
- Caching and performance optimization
- Monitoring and alerting
- Deployment strategies

Learning progression:
1. daily_scrape.py - Main data collection pipeline
2. backfill.py - Historical data processing
3. monitoring.py - Data quality and alerting

Prefect concepts covered:
- Flows and tasks
- Dependencies and parallelism  
- State management
- Deployments and scheduling
"""

from .daily_scrape import daily_race_scraping_flow
from .monitoring import data_quality_monitoring_flow

__all__ = [
    "daily_race_scraping_flow",
    "data_quality_monitoring_flow"
]