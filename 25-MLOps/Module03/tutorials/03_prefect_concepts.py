"""
Prefect Learning Tutorial #3: Workflow Orchestration Concepts

LEARNING MISSION: Master Prefect fundamentals through racing pipeline examples

This tutorial covers:
1. Tasks vs Flows concepts
2. Dependencies and execution order
3. State management and retry logic
4. Caching and performance optimization
5. Monitoring and observability

RUN THIS: python tutorials/03_prefect_concepts.py
"""

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.context import get_run_context
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional
import time
import random
import json

print("🏇 Prefect Learning Tutorial #3: Workflow Orchestration")
print("=" * 60)


# =============================================================================
# TUTORIAL 1: Understanding Tasks vs Flows
# =============================================================================

@task(name="fetch-race-data")
def fetch_sample_race_data(race_date: str, track: str) -> Dict[str, Any]:
    """
    🎯 LEARNING: This is a Prefect TASK
    
    Tasks are the atomic units of work in Prefect:
    - They do one specific thing
    - They can be retried independently
    - They can be cached
    - They have their own state management
    """
    logger = get_run_logger()
    logger.info(f"🏇 Fetching race data for {track} on {race_date}")
    
    # Simulate web scraping delay
    time.sleep(random.uniform(0.5, 2.0))
    
    # Simulate occasional failures for learning
    if random.random() < 0.1:  # 10% chance of failure
        raise Exception(f"Connection timeout for {track}")
    
    # Return sample data
    sample_data = {
        "race_date": race_date,
        "track": track,
        "races": [
            {
                "race_number": i,
                "race_time": f"{13 + i}:30",
                "horses": [f"Horse_{j}" for j in range(1, 9)]
            }
            for i in range(1, random.randint(6, 12))
        ],
        "scraped_at": datetime.now().isoformat()
    }
    
    logger.info(f"✅ Fetched {len(sample_data['races'])} races from {track}")
    return sample_data


@task(name="validate-data", retries=2, retry_delay_seconds=30)
def validate_race_data(data: Dict[str, Any]) -> bool:
    """
    🎯 LEARNING: Task with retry configuration
    
    This task shows:
    - How to configure retries at task level
    - Data validation as a separate concern
    - Boolean return for pipeline control
    """
    logger = get_run_logger()
    logger.info(f"🔍 Validating data for {data['track']}")
    
    # Simulate validation logic
    if len(data['races']) < 3:
        logger.error("Too few races found - data quality issue")
        return False
    
    if not all('horses' in race for race in data['races']):
        logger.error("Missing horse data in some races")
        return False
    
    logger.info("✅ Data validation passed")
    return True


@task(name="process-data", cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def process_race_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    🎯 LEARNING: Task with caching
    
    Caching concepts:
    - cache_key_fn determines what makes results unique
    - task_input_hash means same inputs = same cache key
    - cache_expiration sets how long to keep cached results
    """
    logger = get_run_logger()
    logger.info(f"🔄 Processing data for {data['track']}")
    
    # Simulate expensive processing
    time.sleep(1.0)
    
    processed = {
        "track": data["track"],
        "race_date": data["race_date"],
        "total_races": len(data["races"]),
        "total_horses": sum(len(race["horses"]) for race in data["races"]),
        "first_race": data["races"][0]["race_time"] if data["races"] else None,
        "last_race": data["races"][-1]["race_time"] if data["races"] else None,
        "processed_at": datetime.now().isoformat()
    }
    
    logger.info(f"✅ Processed {processed['total_races']} races with {processed['total_horses']} horses")
    return processed


@flow(name="single-track-pipeline", description="Process racing data for one track")
def single_track_pipeline(race_date: str, track: str) -> Optional[Dict[str, Any]]:
    """
    🎯 LEARNING: This is a Prefect FLOW
    
    Flows orchestrate tasks:
    - They define the execution order
    - They handle task dependencies automatically
    - They can contain other flows (subflows)
    - They provide observability and monitoring
    """
    logger = get_run_logger()
    logger.info(f"🚀 Starting pipeline for {track} on {race_date}")
    
    # Step 1: Fetch data
    raw_data = fetch_sample_race_data(race_date, track)
    
    # Step 2: Validate data  
    is_valid = validate_race_data(raw_data)
    
    # Step 3: Conditional processing based on validation
    if not is_valid:
        logger.error(f"❌ Data validation failed for {track} - skipping processing")
        return None
    
    # Step 4: Process data (only if validation passed)
    processed_data = process_race_data(raw_data)
    
    logger.info(f"✅ Pipeline completed for {track}")
    return processed_data


# =============================================================================
# TUTORIAL 2: Dependencies and Parallelism
# =============================================================================

@task(name="parallel-fetch")
def parallel_fetch_track_data(tracks: List[str], race_date: str) -> List[Dict[str, Any]]:
    """
    🎯 LEARNING: Task that handles multiple inputs
    
    This shows how to process multiple items within a single task.
    Alternative: Use Prefect's .map() for parallel task execution.
    """
    logger = get_run_logger()
    logger.info(f"📡 Fetching data for {len(tracks)} tracks in parallel")
    
    results = []
    for track in tracks:
        try:
            data = fetch_sample_race_data(race_date, track).result()  # Get actual result
            results.append(data)
        except Exception as e:
            logger.warning(f"Failed to fetch data for {track}: {e}")
            # Continue with other tracks
    
    logger.info(f"✅ Successfully fetched data for {len(results)}/{len(tracks)} tracks")
    return results


@task(name="aggregate-results")
def aggregate_track_results(track_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    🎯 LEARNING: Aggregation task that depends on multiple inputs
    
    This task shows:
    - How to combine results from multiple sources
    - Aggregation patterns in data pipelines
    - Summary statistics generation
    """
    logger = get_run_logger()
    logger.info(f"📊 Aggregating results from {len(track_results)} tracks")
    
    if not track_results:
        return {"status": "no_data", "total_tracks": 0}
    
    total_races = sum(len(data["races"]) for data in track_results)
    total_horses = sum(
        sum(len(race["horses"]) for race in data["races"]) 
        for data in track_results
    )
    
    aggregated = {
        "date": track_results[0]["race_date"],
        "total_tracks": len(track_results),
        "total_races": total_races,
        "total_horses": total_horses,
        "tracks_processed": [data["track"] for data in track_results],
        "aggregated_at": datetime.now().isoformat()
    }
    
    logger.info(f"✅ Aggregated: {aggregated['total_tracks']} tracks, {aggregated['total_races']} races")
    return aggregated


@flow(name="multi-track-pipeline", description="Process multiple tracks in parallel")
def multi_track_pipeline(race_date: str, tracks: List[str]) -> Dict[str, Any]:
    """
    🎯 LEARNING: Flow that demonstrates parallelism and dependencies
    
    Execution flow:
    1. Fetch data from all tracks (can run in parallel)
    2. Validate each track's data (parallel)
    3. Process valid data (parallel) 
    4. Aggregate all results (depends on all processing)
    """
    logger = get_run_logger()
    logger.info(f"🚀 Starting multi-track pipeline for {len(tracks)} tracks")
    
    # Method 1: Sequential processing of tracks
    processed_tracks = []
    
    for track in tracks:
        try:
            # Each track pipeline can run independently
            result = single_track_pipeline(race_date, track)
            if result:
                processed_tracks.append(result)
        except Exception as e:
            logger.error(f"Track {track} pipeline failed: {e}")
            # Continue with other tracks
    
    # Aggregate all successful results
    if processed_tracks:
        summary = aggregate_track_results(processed_tracks)
    else:
        summary = {"status": "all_failed", "total_tracks": 0}
    
    logger.info(f"✅ Multi-track pipeline completed: {summary.get('total_tracks', 0)} tracks processed")
    return summary


# =============================================================================
# TUTORIAL 3: State Management and Error Handling
# =============================================================================

@task(name="unreliable-task", retries=3, retry_delay_seconds=5)
def unreliable_external_service(service_name: str) -> str:
    """
    🎯 LEARNING: Task that demonstrates state management
    
    This task fails randomly to show:
    - How Prefect handles retries
    - State transitions (Pending → Running → Failed → Retrying)
    - Exponential backoff (can be configured)
    """
    logger = get_run_logger()
    logger.info(f"📞 Calling external service: {service_name}")
    
    # Get current task run context to see retry information
    context = get_run_context()
    task_run = context.task_run
    
    if task_run:
        logger.info(f"Task run ID: {task_run.id}")
        logger.info(f"Task run attempt: {task_run.run_count}")
    
    # Simulate service unreliability
    failure_rate = 0.6  # 60% chance of failure
    if random.random() < failure_rate:
        error_msg = f"{service_name} returned HTTP 500 - Service Unavailable"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    
    logger.info(f"✅ Successfully called {service_name}")
    return f"Data from {service_name}"


@task(name="fallback-service")
def fallback_data_source() -> str:
    """
    🎯 LEARNING: Fallback pattern for resilience
    
    This shows how to implement backup data sources
    when primary sources fail.
    """
    logger = get_run_logger()
    logger.info("🔄 Using fallback data source")
    
    # Simulate a more reliable backup service
    time.sleep(0.5)
    
    return "Fallback racing data (cached/historical)"


@flow(name="resilient-pipeline", description="Pipeline with error handling and fallbacks")
def resilient_data_pipeline(primary_service: str, enable_fallback: bool = True) -> str:
    """
    🎯 LEARNING: Flow that demonstrates error handling patterns
    
    Error handling strategies:
    1. Task-level retries (configured on tasks)
    2. Flow-level try/catch (Python exception handling)
    3. Fallback services (alternative data sources)
    4. Graceful degradation (partial results)
    """
    logger = get_run_logger()
    logger.info(f"🛡️ Starting resilient pipeline with primary service: {primary_service}")
    
    try:
        # Try primary service first
        result = unreliable_external_service(primary_service)
        logger.info("✅ Primary service succeeded")
        return result
        
    except Exception as e:
        logger.error(f"❌ Primary service failed after retries: {e}")
        
        if enable_fallback:
            logger.info("🔄 Switching to fallback service")
            try:
                fallback_result = fallback_data_source()
                logger.info("✅ Fallback service succeeded")
                return fallback_result
            except Exception as fallback_error:
                logger.error(f"❌ Fallback service also failed: {fallback_error}")
                raise Exception("Both primary and fallback services failed")
        else:
            logger.error("🚫 Fallback disabled - pipeline failed")
            raise


# =============================================================================
# TUTORIAL 4: Advanced Flow Patterns
# =============================================================================

@task(name="conditional-task")
def conditional_processing_task(data: Dict[str, Any], condition: str) -> Optional[Dict[str, Any]]:
    """
    🎯 LEARNING: Conditional task execution
    
    This shows how to implement conditional logic within tasks.
    """
    logger = get_run_logger()
    
    if condition == "weekend" and datetime.fromisoformat(data["race_date"]).weekday() >= 5:
        logger.info("🏁 Weekend racing - applying special processing")
        data["weekend_bonus"] = True
        time.sleep(1)  # Simulate extra processing
        
    elif condition == "major_track" and data["track"] in ["Ellerslie", "Trentham", "Riccarton"]:
        logger.info("🏆 Major track - enhanced data collection")
        data["major_track"] = True
        
    else:
        logger.info("📅 Standard processing")
    
    return data


@flow(name="conditional-flow", description="Flow with conditional execution paths")
def conditional_execution_flow(race_date: str, track: str) -> Dict[str, Any]:
    """
    🎯 LEARNING: Conditional flow execution
    
    This demonstrates:
    - How to implement different execution paths
    - Conditional task execution based on data/context
    - Dynamic workflow behavior
    """
    logger = get_run_logger()
    
    # Base data collection
    base_data = fetch_sample_race_data(race_date, track)
    
    # Determine processing path based on conditions
    race_datetime = datetime.fromisoformat(race_date)
    is_weekend = race_datetime.weekday() >= 5
    is_major_track = track in ["Ellerslie", "Trentham", "Riccarton"]
    
    logger.info(f"Conditions: weekend={is_weekend}, major_track={is_major_track}")
    
    # Apply conditional processing
    if is_weekend:
        processed_data = conditional_processing_task(base_data, "weekend")
    elif is_major_track:
        processed_data = conditional_processing_task(base_data, "major_track")
    else:
        processed_data = conditional_processing_task(base_data, "standard")
    
    # Add metadata about the execution path
    result = {
        "data": processed_data,
        "execution_path": {
            "is_weekend": is_weekend,
            "is_major_track": is_major_track,
            "processing_type": "weekend" if is_weekend else ("major_track" if is_major_track else "standard")
        },
        "completed_at": datetime.now().isoformat()
    }
    
    return result


# =============================================================================
# TUTORIAL 5: Monitoring and Observability
# =============================================================================

@task(name="monitored-task")
def monitored_data_processing(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    🎯 LEARNING: Task with comprehensive monitoring
    
    This shows how to add observability to your tasks:
    - Structured logging
    - Performance metrics
    - Custom tags and metadata
    """
    logger = get_run_logger()
    start_time = time.time()
    
    # Log structured information
    logger.info("📊 Starting monitored processing", extra={
        "track": data["track"],
        "race_count": len(data["races"]),
        "data_size": len(str(data))
    })
    
    # Simulate processing with progress logging
    total_races = len(data["races"])
    processed_races = []
    
    for i, race in enumerate(data["races"]):
        time.sleep(0.1)  # Simulate processing time
        
        processed_race = {
            **race,
            "processed": True,
            "horse_count": len(race["horses"])
        }
        processed_races.append(processed_race)
        
        # Log progress every 25%
        if (i + 1) % max(1, total_races // 4) == 0:
            progress = (i + 1) / total_races * 100
            logger.info(f"Progress: {progress:.0f}% ({i + 1}/{total_races} races)")
    
    # Calculate performance metrics
    end_time = time.time()
    processing_time = end_time - start_time
    races_per_second = total_races / processing_time if processing_time > 0 else 0
    
    result = {
        "track": data["track"],
        "races": processed_races,
        "metadata": {
            "processing_time_seconds": round(processing_time, 2),
            "races_per_second": round(races_per_second, 2),
            "total_races": total_races,
            "start_time": datetime.fromtimestamp(start_time).isoformat(),
            "end_time": datetime.fromtimestamp(end_time).isoformat()
        }
    }
    
    logger.info(f"✅ Processing completed in {processing_time:.2f}s ({races_per_second:.1f} races/sec)")
    return result


@flow(name="observable-pipeline", description="Pipeline with comprehensive monitoring")
def observable_racing_pipeline(race_date: str, tracks: List[str]) -> Dict[str, Any]:
    """
    🎯 LEARNING: Flow with comprehensive observability
    
    Observability features:
    - Structured logging throughout
    - Performance tracking
    - Error context preservation
    - Pipeline-level metrics
    """
    logger = get_run_logger()
    pipeline_start = time.time()
    
    logger.info(f"🔍 Starting observable pipeline", extra={
        "race_date": race_date,
        "track_count": len(tracks),
        "tracks": tracks
    })
    
    results = []
    errors = []
    
    for track in tracks:
        track_start = time.time()
        
        try:
            # Fetch and process data for each track
            raw_data = fetch_sample_race_data(race_date, track)
            processed_data = monitored_data_processing(raw_data)
            
            track_time = time.time() - track_start
            processed_data["track_processing_time"] = round(track_time, 2)
            
            results.append(processed_data)
            logger.info(f"✅ Track {track} completed in {track_time:.2f}s")
            
        except Exception as e:
            track_time = time.time() - track_start
            error_info = {
                "track": track,
                "error": str(e),
                "processing_time": round(track_time, 2),
                "timestamp": datetime.now().isoformat()
            }
            errors.append(error_info)
            logger.error(f"❌ Track {track} failed after {track_time:.2f}s: {e}")
    
    # Calculate pipeline-level metrics
    pipeline_time = time.time() - pipeline_start
    success_rate = len(results) / len(tracks) * 100 if tracks else 0
    total_races = sum(len(result["races"]) for result in results)
    
    pipeline_summary = {
        "race_date": race_date,
        "results": results,
        "errors": errors,
        "metrics": {
            "total_tracks": len(tracks),
            "successful_tracks": len(results),
            "failed_tracks": len(errors),
            "success_rate_percent": round(success_rate, 1),
            "total_races_processed": total_races,
            "pipeline_duration_seconds": round(pipeline_time, 2),
            "average_track_time": round(pipeline_time / len(tracks), 2) if tracks else 0
        },
        "completed_at": datetime.now().isoformat()
    }
    
    logger.info(f"🏁 Pipeline completed", extra=pipeline_summary["metrics"])
    
    return pipeline_summary


# =============================================================================
# MAIN TUTORIAL RUNNER
# =============================================================================

def run_tutorial_1():
    """Run basic tasks and flows tutorial."""
    print("\\n" + "="*50)
    print("📚 TUTORIAL 1: Tasks vs Flows")
    print("="*50)
    
    # Run single track pipeline
    result = single_track_pipeline("2024-01-15", "Ellerslie")
    print(f"\\n✅ Single track result: {result}")


def run_tutorial_2():
    """Run parallelism and dependencies tutorial."""
    print("\\n" + "="*50)
    print("📚 TUTORIAL 2: Parallelism & Dependencies")
    print("="*50)
    
    tracks = ["Ellerslie", "Trentham", "Riccarton"]
    result = multi_track_pipeline("2024-01-15", tracks)
    print(f"\\n✅ Multi-track result: {result}")


def run_tutorial_3():
    """Run error handling tutorial."""
    print("\\n" + "="*50)
    print("📚 TUTORIAL 3: Error Handling & Resilience")
    print("="*50)
    
    # Test with unreliable service
    try:
        result = resilient_data_pipeline("UnreliableRacingAPI", enable_fallback=True)
        print(f"\\n✅ Resilient pipeline result: {result}")
    except Exception as e:
        print(f"\\n❌ Pipeline failed: {e}")


def run_tutorial_4():
    """Run conditional execution tutorial."""
    print("\\n" + "="*50)
    print("📚 TUTORIAL 4: Conditional Execution")
    print("="*50)
    
    # Test weekend vs weekday processing
    weekend_date = "2024-01-14"  # Sunday
    weekday_date = "2024-01-15"  # Monday
    
    weekend_result = conditional_execution_flow(weekend_date, "Ellerslie")
    weekday_result = conditional_execution_flow(weekday_date, "Te Rapa")
    
    print(f"\\n✅ Weekend result: {weekend_result['execution_path']}")
    print(f"✅ Weekday result: {weekday_result['execution_path']}")


def run_tutorial_5():
    """Run monitoring and observability tutorial."""
    print("\\n" + "="*50)
    print("📚 TUTORIAL 5: Monitoring & Observability")
    print("="*50)
    
    tracks = ["Ellerslie", "Trentham"]
    result = observable_racing_pipeline("2024-01-15", tracks)
    
    print(f"\\n📊 Pipeline Metrics:")
    for key, value in result["metrics"].items():
        print(f"  {key}: {value}")


def main():
    """Run all Prefect tutorials."""
    
    print("🎯 Starting Prefect Concepts Tutorial")
    print("Make sure Prefect server is running: prefect server start")
    print("Then visit http://localhost:4200 to see the UI\\n")
    
    try:
        run_tutorial_1()
        run_tutorial_2()
        run_tutorial_3()
        run_tutorial_4()
        run_tutorial_5()
        
        print("\\n" + "="*60)
        print("🎉 CONGRATULATIONS!")
        print("You've completed Prefect Tutorial #3")
        print("="*60)
        
        print("\\n🎯 KEY CONCEPTS LEARNED:")
        print("✅ Tasks vs Flows: Atomic units vs orchestration")
        print("✅ Dependencies: Automatic from data flow")
        print("✅ Retries: Task-level configuration")
        print("✅ Caching: Performance optimization")
        print("✅ State Management: Prefect handles execution state")
        print("✅ Error Handling: Try/catch + fallback patterns")
        print("✅ Parallelism: Map operations and independent tasks")
        print("✅ Monitoring: Structured logging and metrics")
        
        print("\\n🚀 NEXT STEPS:")
        print("1. Check Prefect UI for flow run history")
        print("2. Experiment with different retry configurations")
        print("3. Try deploying flows for scheduling")
        print("4. Implement your own racing data pipeline")
        
        print("\\n📚 ADVANCED LEARNING:")
        print("- Prefect Deployments: Scheduling and automation")
        print("- Subflows: Composing complex workflows")
        print("- Custom task runners: Dask, Ray integration")
        print("- Prefect Cloud: Hosted orchestration platform")
        
    except KeyboardInterrupt:
        print("\\n\\n⏹️ Tutorial interrupted by user")
    except Exception as e:
        print(f"\\n\\n❌ Tutorial failed: {e}")
        print("Make sure Prefect is installed: uv add prefect")


if __name__ == "__main__":
    main()