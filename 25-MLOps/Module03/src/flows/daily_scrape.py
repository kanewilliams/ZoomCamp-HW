"""
Daily Racing Data Scraping Flow - Prefect Learning Hub

LEARNING OBJECTIVES:
1. Understand Prefect flow and task concepts
2. Learn dependency management and parallelism
3. Practice error handling and retry strategies
4. Implement caching for performance
5. Set up monitoring and alerting

PREFECT CONCEPTS DEMONSTRATED:
- @flow and @task decorators
- Task dependencies and parallelism
- State management and caching
- Error handling with retries
- Resource management and cleanup
"""

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.context import get_run_context
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
import polars as pl
from pathlib import Path

# Import our custom modules
from ..scrapers import TABScraper
from ..data import validate_scraped_data, RaceDataTransformer, DataExporter


@task(
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1)
)
def scrape_tab_racing_data(target_date: date) -> List[Dict[str, Any]]:
    """
    Scrape racing data from TAB.nz for a specific date.
    
    LEARNING FOCUS: Task configuration options
    - retries: How many times to retry on failure
    - retry_delay_seconds: Wait time between retries
    - cache_key_fn: Cache based on input parameters
    - cache_expiration: How long to keep cached results
    
    TODO FOR STUDENT:
    1. [ ] Test retry behavior by introducing temporary failures
    2. [ ] Experiment with different cache strategies
    3. [ ] Add task tags for better organization
    4. [ ] Implement custom result serializers
    """
    logger = get_run_logger()
    logger.info(f"🏇 Starting TAB.nz scraping for {target_date}")
    
    try:
        with TABScraper() as scraper:
            races_data = scraper.scrape_daily_races(target_date)
            
            if not races_data:
                logger.warning(f"No racing data found for {target_date}")
                return []
            
            logger.info(f"✅ Scraped {len(races_data)} races from TAB.nz")
            return races_data
            
    except Exception as e:
        logger.error(f"❌ TAB scraping failed: {e}")
        raise  # Re-raise to trigger Prefect retry logic


@task(
    retries=2,
    retry_delay_seconds=30
)
def validate_racing_data(raw_data: List[Dict[str, Any]]) -> bool:
    """
    Validate scraped data quality.
    
    LEARNING FOCUS: Data quality as a first-class citizen in pipelines
    """
    logger = get_run_logger()
    logger.info(f"🔍 Validating {len(raw_data)} race records")
    
    try:
        validation_result = validate_scraped_data(raw_data)
        
        if not validation_result.is_valid:
            logger.error(f"❌ Data validation failed:")
            for error in validation_result.errors:
                logger.error(f"  - {error}")
            return False
        
        if validation_result.warnings:
            logger.warning("⚠️ Data quality warnings:")
            for warning in validation_result.warnings:
                logger.warning(f"  - {warning}")
        
        logger.info(f"✅ Data validation passed with quality score: {validation_result.stats.get('data_quality_score', 'N/A')}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Validation task failed: {e}")
        raise


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=6)
)
def transform_racing_data(raw_data: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    Transform and engineer features for racing data.
    
    LEARNING FOCUS: Data transformation as cacheable operations
    """
    logger = get_run_logger()
    logger.info(f"🔄 Transforming {len(raw_data)} race records")
    
    try:
        # Convert to Polars DataFrame
        df = pl.DataFrame(raw_data)
        
        # Apply transformations
        transformer = RaceDataTransformer()
        cleaned_df = transformer.clean_scraped_data(df)
        featured_df = transformer.engineer_basic_features(cleaned_df)
        betting_df = transformer.create_betting_features(featured_df)
        
        logger.info(f"✅ Transformed data: {len(betting_df)} records, {len(betting_df.columns)} features")
        return betting_df
        
    except Exception as e:
        logger.error(f"❌ Data transformation failed: {e}")
        raise


@task(retries=2)
def export_processed_data(
    df: pl.DataFrame, 
    date_str: str,
    export_formats: List[str] = ['parquet', 'csv']
) -> Dict[str, str]:
    """
    Export processed data in multiple formats.
    
    LEARNING FOCUS: Multiple output formats for different consumers
    """
    logger = get_run_logger()
    logger.info(f"💾 Exporting {len(df)} records for {date_str}")
    
    try:
        exporter = DataExporter()
        export_paths = {}
        
        base_filename = f"racing_data_{date_str}"
        
        if 'parquet' in export_formats:
            path = exporter.export_parquet(df, base_filename)
            export_paths['parquet'] = str(path)
        
        if 'csv' in export_formats:
            path = exporter.export_csv(df, base_filename)
            export_paths['csv'] = str(path)
        
        if 'summary' in export_formats:
            path = exporter.export_daily_summary_report(df, date_str)
            export_paths['summary'] = str(path)
        
        logger.info(f"✅ Data exported to: {list(export_paths.keys())}")
        return export_paths
        
    except Exception as e:
        logger.error(f"❌ Data export failed: {e}")
        raise


@task
def create_data_quality_metrics(df: pl.DataFrame) -> Dict[str, Any]:
    """
    Generate data quality metrics for monitoring.
    
    TODO FOR STUDENT:
    1. [ ] Add more comprehensive quality metrics
    2. [ ] Implement alerting thresholds
    3. [ ] Create time-series quality tracking
    4. [ ] Add statistical anomaly detection
    """
    logger = get_run_logger()
    
    try:
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'unique_tracks': df.select('track_clean').n_unique(),
            'null_percentages': {},
            'data_types': dict(zip(df.columns, [str(dtype) for dtype in df.dtypes])),
        }
        
        # Calculate null percentages
        for col in df.columns:
            null_pct = (df.select(pl.col(col).is_null().sum()).item() / len(df)) * 100
            metrics['null_percentages'][col] = round(null_pct, 2)
        
        logger.info(f"📊 Generated quality metrics: {metrics['total_records']} records, {metrics['unique_tracks']} tracks")
        return metrics
        
    except Exception as e:
        logger.error(f"❌ Quality metrics generation failed: {e}")
        raise


@flow(
    name="daily-race-scraping", 
    description="Daily horse racing data collection and processing pipeline",
    version="1.0",
    retries=1,
    retry_delay_seconds=300
)
def daily_race_scraping_flow(
    target_date: Optional[date] = None,
    export_formats: List[str] = ['parquet', 'csv', 'summary']
) -> Dict[str, Any]:
    """
    Main daily racing data pipeline.
    
    LEARNING OBJECTIVES:
    1. Understand flow composition from tasks
    2. Learn about task dependencies (automatic from data flow)
    3. Practice error handling at flow level
    4. Implement monitoring and metrics collection
    
    FLOW FEATURES DEMONSTRATED:
    - Automatic dependency resolution
    - Parallel task execution where possible
    - State management and checkpointing
    - Comprehensive logging and monitoring
    
    TODO FOR STUDENT:
    1. [ ] Add more data sources (Racing.com, Trackside.co.nz)
    2. [ ] Implement parallel scraping of multiple sources
    3. [ ] Add data quality gates (fail pipeline if quality too low)
    4. [ ] Create alerting for pipeline failures
    5. [ ] Add performance monitoring and SLA tracking
    """
    logger = get_run_logger()
    
    # Default to today if no date specified
    if target_date is None:
        target_date = date.today()
    
    date_str = target_date.isoformat()
    logger.info(f"🏇 Starting daily racing pipeline for {date_str}")
    
    pipeline_stats = {
        'start_time': datetime.now().isoformat(),
        'target_date': date_str,
        'flow_run_id': get_run_context().flow_run.id,
    }
    
    try:
        # Step 1: Scrape racing data
        raw_data = scrape_tab_racing_data(target_date)
        
        if not raw_data:
            logger.warning(f"⚠️ No data scraped for {date_str} - ending pipeline")
            pipeline_stats['status'] = 'no_data'
            pipeline_stats['end_time'] = datetime.now().isoformat()
            return pipeline_stats
        
        # Step 2: Validate data quality
        is_valid = validate_racing_data(raw_data)
        
        if not is_valid:
            logger.error(f"❌ Data validation failed for {date_str}")
            pipeline_stats['status'] = 'validation_failed'
            pipeline_stats['end_time'] = datetime.now().isoformat()
            return pipeline_stats
        
        # Step 3: Transform and engineer features
        processed_df = transform_racing_data(raw_data)
        
        # Step 4: Export data (these can run in parallel)
        export_paths = export_processed_data(processed_df, date_str, export_formats)
        
        # Step 5: Generate quality metrics
        quality_metrics = create_data_quality_metrics(processed_df)
        
        # Success!
        pipeline_stats.update({
            'status': 'success',
            'records_processed': len(processed_df),
            'export_paths': export_paths,
            'quality_metrics': quality_metrics,
            'end_time': datetime.now().isoformat()
        })
        
        logger.info(f"✅ Pipeline completed successfully for {date_str}")
        logger.info(f"📊 Processed {pipeline_stats['records_processed']} records")
        
        return pipeline_stats
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed for {date_str}: {e}")
        pipeline_stats.update({
            'status': 'failed',
            'error': str(e),
            'end_time': datetime.now().isoformat()
        })
        raise


@flow(
    name="backfill-racing-data",
    description="Backfill historical racing data for a date range"
)
def backfill_racing_data_flow(
    start_date: date,
    end_date: date,
    max_parallel: int = 3
) -> List[Dict[str, Any]]:
    """
    Backfill racing data for a range of dates.
    
    LEARNING FOCUS: 
    - Flow composition (calling other flows)
    - Parallel execution with concurrency limits
    - Bulk data processing patterns
    
    TODO FOR STUDENT:
    1. [ ] Implement proper concurrency control
    2. [ ] Add progress tracking and resumability
    3. [ ] Handle rate limiting across parallel runs
    4. [ ] Add bulk export optimization
    """
    logger = get_run_logger()
    logger.info(f"🔄 Starting backfill from {start_date} to {end_date}")
    
    results = []
    current_date = start_date
    
    while current_date <= end_date:
        try:
            # TODO FOR STUDENT: Implement parallel execution with Prefect
            # Use submit() for parallel execution with concurrency limits
            result = daily_race_scraping_flow(target_date=current_date)
            results.append(result)
            
        except Exception as e:
            logger.error(f"❌ Backfill failed for {current_date}: {e}")
            results.append({
                'target_date': current_date.isoformat(),
                'status': 'failed',
                'error': str(e)
            })
        
        current_date += timedelta(days=1)
    
    logger.info(f"✅ Backfill completed: {len(results)} dates processed")
    return results


# TODO FOR STUDENT - TESTING AND DEPLOYMENT:
if __name__ == "__main__":
    """
    Test the daily scraping flow locally.
    
    LEARNING STEPS:
    1. Run this script to test the flow
    2. Check the Prefect UI for flow runs
    3. Examine logs and state transitions
    4. Test error handling by introducing failures
    """
    
    print("🏇 Testing Daily Racing Scraping Flow")
    print("=" * 45)
    
    # Run for yesterday (more likely to have complete data)
    yesterday = date.today() - timedelta(days=1)
    
    print(f"Running pipeline for {yesterday}")
    print("Check Prefect UI at http://localhost:4200 for monitoring")
    
    # Execute the flow
    result = daily_race_scraping_flow(target_date=yesterday)
    
    print("\\nPipeline Result:")
    print(f"Status: {result.get('status')}")
    print(f"Records: {result.get('records_processed', 'N/A')}")
    print(f"Duration: {result.get('end_time')} - {result.get('start_time')}")
    
    if result.get('export_paths'):
        print("\\nExported files:")
        for format_type, path in result['export_paths'].items():
            print(f"  {format_type}: {path}")
    
    print("\\n🎯 Next steps:")
    print("1. Deploy this flow to run daily")
    print("2. Add more data sources")  
    print("3. Implement monitoring and alerting")
    print("4. Scale with parallel processing")