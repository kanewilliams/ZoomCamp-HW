"""
Data Quality Monitoring Flow - Learning Production Monitoring

LEARNING OBJECTIVES:
1. Implement data quality monitoring in production
2. Learn about SLA tracking and alerting
3. Practice anomaly detection on data pipelines
4. Understand observability patterns

TODO FOR STUDENT - COMPREHENSIVE MONITORING:
This file shows you the foundation for production monitoring.
You'll need to implement the actual monitoring logic based on your requirements.
"""

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional
import polars as pl
from pathlib import Path
import json

from ..data import DataExporter


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=30)
)
def load_recent_data(days_back: int = 7) -> pl.DataFrame:
    """
    Load recent data for quality monitoring.
    
    TODO FOR STUDENT:
    1. [ ] Implement efficient data loading from your storage
    2. [ ] Add incremental loading for large datasets
    3. [ ] Handle missing files gracefully
    """
    logger = get_run_logger()
    logger.info(f"📊 Loading recent {days_back} days of data for monitoring")
    
    try:
        # TODO: Replace with actual data loading from your export directory
        processed_data_dir = Path("processed_data")
        
        if not processed_data_dir.exists():
            logger.warning("No processed data directory found")
            return pl.DataFrame()
        
        # Load recent parquet files
        recent_files = []
        for i in range(days_back):
            target_date = date.today() - timedelta(days=i)
            file_path = processed_data_dir / f"racing_data_{target_date.isoformat()}.parquet"
            
            if file_path.exists():
                recent_files.append(file_path)
        
        if not recent_files:
            logger.warning("No recent data files found")
            return pl.DataFrame()
        
        # Combine all recent files
        dataframes = []
        for file_path in recent_files:
            try:
                df = pl.read_parquet(file_path)
                dataframes.append(df)
            except Exception as e:
                logger.warning(f"Failed to load {file_path}: {e}")
        
        if dataframes:
            combined_df = pl.concat(dataframes)
            logger.info(f"✅ Loaded {len(combined_df)} records from {len(dataframes)} files")
            return combined_df
        else:
            return pl.DataFrame()
            
    except Exception as e:
        logger.error(f"❌ Failed to load recent data: {e}")
        raise


@task
def calculate_data_freshness_metrics(df: pl.DataFrame) -> Dict[str, Any]:
    """
    Calculate data freshness and completeness metrics.
    
    TODO FOR STUDENT:
    1. [ ] Define acceptable data age thresholds
    2. [ ] Implement completeness checking
    3. [ ] Add seasonality-aware anomaly detection
    """
    logger = get_run_logger()
    
    try:
        if len(df) == 0:
            return {'status': 'no_data', 'metrics': {}}
        
        metrics = {}
        
        # Data age metrics
        if 'scraped_at' in df.columns:
            latest_scrape = df.select(pl.col('scraped_at').max()).item()
            if latest_scrape:
                # Convert to datetime if it's a string
                if isinstance(latest_scrape, str):
                    latest_scrape = datetime.fromisoformat(latest_scrape.replace('Z', '+00:00'))
                
                age_hours = (datetime.now() - latest_scrape).total_seconds() / 3600
                metrics['data_age_hours'] = age_hours
                metrics['data_freshness_status'] = 'stale' if age_hours > 25 else 'fresh'
        
        # Completeness metrics
        if 'race_date' in df.columns:
            date_range = df.select([
                pl.col('race_date').min().alias('min_date'),
                pl.col('race_date').max().alias('max_date'),
                pl.col('race_date').n_unique().alias('unique_dates')
            ]).to_dicts()[0]
            
            metrics.update(date_range)
        
        # Volume metrics
        daily_counts = df.group_by('race_date').agg(pl.len().alias('daily_count'))
        
        metrics.update({
            'total_records': len(df),
            'avg_daily_records': daily_counts.select(pl.col('daily_count').mean()).item(),
            'min_daily_records': daily_counts.select(pl.col('daily_count').min()).item(),
            'max_daily_records': daily_counts.select(pl.col('daily_count').max()).item(),
        })
        
        logger.info(f"📊 Calculated freshness metrics: {metrics.get('data_freshness_status', 'unknown')} data")
        return {'status': 'success', 'metrics': metrics}
        
    except Exception as e:
        logger.error(f"❌ Freshness calculation failed: {e}")
        return {'status': 'error', 'error': str(e)}


@task
def detect_data_anomalies(df: pl.DataFrame) -> Dict[str, Any]:
    """
    Detect anomalies in racing data patterns.
    
    TODO FOR STUDENT - ANOMALY DETECTION:
    1. [ ] Implement statistical anomaly detection
    2. [ ] Add domain-specific rules (e.g., unusual track counts)
    3. [ ] Create alerting thresholds
    4. [ ] Handle seasonal patterns (more racing on weekends)
    """
    logger = get_run_logger()
    
    try:
        if len(df) == 0:
            return {'status': 'no_data', 'anomalies': []}
        
        anomalies = []
        
        # Check for unusual daily volumes
        daily_stats = df.group_by('race_date').agg([
            pl.len().alias('race_count'),
            pl.col('track_clean').n_unique().alias('track_count')
        ])
        
        avg_races = daily_stats.select(pl.col('race_count').mean()).item()
        std_races = daily_stats.select(pl.col('race_count').std()).item()
        
        # Flag days with unusually low race counts (more than 2 std devs below mean)
        if std_races and std_races > 0:
            threshold = avg_races - (2 * std_races)
            low_volume_days = daily_stats.filter(pl.col('race_count') < threshold)
            
            if len(low_volume_days) > 0:
                anomalies.append({
                    'type': 'low_volume',
                    'description': f'Found {len(low_volume_days)} days with unusually low race counts',
                    'threshold': threshold,
                    'affected_dates': low_volume_days.select('race_date').to_series().to_list()
                })
        
        # Check for missing expected tracks
        # TODO FOR STUDENT: Define expected tracks for different days
        expected_major_tracks = ['ELLERSLIE', 'TRENTHAM', 'RICCARTON']
        track_appearances = df.group_by('track_clean').agg(pl.len().alias('appearances'))
        
        for track in expected_major_tracks:
            appearances = track_appearances.filter(pl.col('track_clean') == track)
            if len(appearances) == 0:
                anomalies.append({
                    'type': 'missing_track',
                    'description': f'Major track {track} not found in recent data',
                    'track': track
                })
        
        # Check for data quality degradation
        if 'track_clean' in df.columns:
            unknown_tracks = df.filter(pl.col('track_clean').is_in(['UNKNOWN', '', None])).height
            if unknown_tracks > 0:
                unknown_pct = (unknown_tracks / len(df)) * 100
                if unknown_pct > 5:  # More than 5% unknown tracks
                    anomalies.append({
                        'type': 'data_quality',
                        'description': f'{unknown_pct:.1f}% of records have unknown track names',
                        'percentage': unknown_pct
                    })
        
        logger.info(f"🔍 Detected {len(anomalies)} potential anomalies")
        return {'status': 'success', 'anomalies': anomalies}
        
    except Exception as e:
        logger.error(f"❌ Anomaly detection failed: {e}")
        return {'status': 'error', 'error': str(e)}


@task
def generate_monitoring_report(
    freshness_metrics: Dict[str, Any],
    anomaly_results: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate comprehensive monitoring report.
    
    TODO FOR STUDENT:
    1. [ ] Add trend analysis
    2. [ ] Create actionable recommendations
    3. [ ] Implement report distribution (email, Slack, etc.)
    """
    logger = get_run_logger()
    
    try:
        report = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'healthy',
            'freshness': freshness_metrics,
            'anomalies': anomaly_results,
            'recommendations': []
        }
        
        # Determine overall health status
        issues = []
        
        if freshness_metrics.get('status') == 'error':
            issues.append('freshness_check_failed')
        elif freshness_metrics.get('metrics', {}).get('data_freshness_status') == 'stale':
            issues.append('stale_data')
        
        if anomaly_results.get('status') == 'error':
            issues.append('anomaly_detection_failed')
        elif len(anomaly_results.get('anomalies', [])) > 0:
            issues.append('anomalies_detected')
        
        if issues:
            report['overall_status'] = 'warning' if len(issues) == 1 else 'critical'
            report['issues'] = issues
        
        # Generate recommendations
        recommendations = []
        
        if 'stale_data' in issues:
            recommendations.append("Data is stale - check scraping pipeline status")
        
        if 'anomalies_detected' in issues:
            recommendations.append("Anomalies detected - review data quality and source systems")
        
        if not issues:
            recommendations.append("All systems healthy - no action required")
        
        report['recommendations'] = recommendations
        
        logger.info(f"📋 Generated monitoring report: {report['overall_status']} status")
        return report
        
    except Exception as e:
        logger.error(f"❌ Report generation failed: {e}")
        raise


@task
def save_monitoring_report(report: Dict[str, Any]) -> str:
    """Save monitoring report for historical tracking."""
    logger = get_run_logger()
    
    try:
        reports_dir = Path("monitoring_reports")
        reports_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = reports_dir / f"monitoring_report_{timestamp}.json"
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"💾 Saved monitoring report to {report_path}")
        return str(report_path)
        
    except Exception as e:
        logger.error(f"❌ Failed to save monitoring report: {e}")
        raise


@flow(
    name="data-quality-monitoring",
    description="Monitor data quality and pipeline health",
    version="1.0"
)
def data_quality_monitoring_flow(
    days_to_analyze: int = 7,
    alert_on_issues: bool = True
) -> Dict[str, Any]:
    """
    Main data quality monitoring flow.
    
    LEARNING OBJECTIVES:
    1. Understand monitoring as code
    2. Learn about data quality SLAs
    3. Practice alerting and escalation
    4. Implement observability patterns
    
    TODO FOR STUDENT:
    1. [ ] Add integration with alerting systems (Slack, PagerDuty)
    2. [ ] Implement SLA tracking and reporting
    3. [ ] Add performance monitoring (pipeline execution times)
    4. [ ] Create dashboards for monitoring data
    5. [ ] Implement automated remediation for common issues
    """
    logger = get_run_logger()
    logger.info(f"🔍 Starting data quality monitoring for last {days_to_analyze} days")
    
    try:
        # Load recent data
        recent_data = load_recent_data(days_to_analyze)
        
        # Calculate metrics
        freshness_metrics = calculate_data_freshness_metrics(recent_data)
        anomaly_results = detect_data_anomalies(recent_data)
        
        # Generate report
        monitoring_report = generate_monitoring_report(freshness_metrics, anomaly_results)
        
        # Save report
        report_path = save_monitoring_report(monitoring_report)
        
        # TODO FOR STUDENT: Add alerting logic
        if alert_on_issues and monitoring_report['overall_status'] != 'healthy':
            logger.warning(f"⚠️ Data quality issues detected: {monitoring_report['issues']}")
            # TODO: Implement actual alerting (email, Slack, etc.)
        
        logger.info(f"✅ Monitoring completed: {monitoring_report['overall_status']} status")
        
        return {
            'status': 'success',
            'monitoring_report': monitoring_report,
            'report_path': report_path,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Data quality monitoring failed: {e}")
        raise


# TODO FOR STUDENT - ALERTING INTEGRATION:
@task
def send_alert_notification(report: Dict[str, Any]) -> bool:
    """
    Send alert notifications for data quality issues.
    
    TODO FOR STUDENT - ALERTING IMPLEMENTATION:
    1. [ ] Integrate with Slack API for notifications
    2. [ ] Add email alerting for critical issues
    3. [ ] Implement escalation logic (retry alerts)
    4. [ ] Add webhook integration for external systems
    5. [ ] Create alert templates for different issue types
    """
    logger = get_run_logger()
    
    # Placeholder implementation
    if report['overall_status'] != 'healthy':
        logger.warning("🚨 ALERT: Data quality issues detected!")
        logger.warning(f"Issues: {report.get('issues', [])}")
        logger.warning(f"Recommendations: {report.get('recommendations', [])}")
        
        # TODO: Implement actual alerting
        # - Slack webhook
        # - Email via SMTP
        # - PagerDuty API
        # - Custom webhook
        
        return True
    
    return False


if __name__ == "__main__":
    """
    Test the monitoring flow locally.
    
    LEARNING EXERCISE:
    1. Run monitoring on your current data
    2. Introduce data quality issues and see if they're detected
    3. Test alerting logic with different scenarios
    """
    
    print("🔍 Testing Data Quality Monitoring")
    print("=" * 40)
    
    result = data_quality_monitoring_flow(days_to_analyze=3)
    
    print("\\nMonitoring Result:")
    print(f"Status: {result['status']}")
    print(f"Overall Health: {result['monitoring_report']['overall_status']}")
    
    if result['monitoring_report'].get('issues'):
        print(f"Issues: {result['monitoring_report']['issues']}")
        print("Recommendations:")
        for rec in result['monitoring_report']['recommendations']:
            print(f"  - {rec}")
    
    print(f"\\nReport saved to: {result['report_path']}")
    print("\\n🎯 Next: Set up automated scheduling and alerting!")