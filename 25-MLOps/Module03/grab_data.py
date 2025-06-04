import polars as pl
import requests
from datetime import datetime, timedelta
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
import time


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def fetch_sample_data() -> pl.DataFrame:
    """
    Fetch sample horse racing data.
    For now, creates synthetic data - you can replace with actual scraping.
    """
    # Sample data structure based on typical horse racing data
    sample_data = {
        "race_date": ["2024-01-15", "2024-01-15", "2024-01-15", "2024-01-16"],
        "track": ["Ellerslie", "Trentham", "Riccarton", "Ellerslie"],
        "race_number": [1, 2, 3, 1],
        "horse_name": ["Thunder Bay", "Lightning Strike", "Midnight Runner", "Storm Chaser"],
        "jockey": ["J. McDonald", "S. Weatherley", "D. Johnson", "L. Innes"],
        "trainer": ["M. Baker", "T. Pike", "R. Patterson", "J. Bridgman"],
        "barrier": [3, 7, 1, 5],
        "weight": [57.0, 56.5, 58.0, 57.5],
        "odds": [3.2, 8.5, 2.1, 15.0],
        "finish_position": [1, 3, 2, 4],
        "margin": [0.0, 2.5, 1.2, 8.0],
        "race_time": ["1:23.45", "1:45.23", "2:01.12", "1:25.67"],
        "prize_money": [50000, 35000, 25000, 40000]
    }
    
    df = pl.DataFrame(sample_data)
    return df


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def fetch_trackside_data() -> pl.DataFrame:
    """
    Placeholder for Trackside NZ data scraping.
    This would involve web scraping trackside.co.nz
    """
    # For now, return sample data with different structure
    trackside_data = {
        "source": ["trackside"] * 3,
        "event_date": ["2024-01-15", "2024-01-15", "2024-01-16"],
        "venue": ["Auckland", "Wellington", "Christchurch"],
        "race_id": ["AUK_R1", "WEL_R2", "CHC_R1"],
        "runner_name": ["Fast Forward", "Quick Step", "Speed Demon"],
        "starting_price": [4.5, 6.2, 2.8],
        "result": ["1st", "2nd", "1st"]
    }
    
    df = pl.DataFrame(trackside_data)
    return df


@task
def combine_data_sources(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    """
    Combine data from multiple sources into a unified format.
    """
    # Add source column to first dataframe
    df1_with_source = df1.with_columns(pl.lit("loveracing").alias("data_source"))
    
    # For now, just return the first dataframe since structures are different
    # In a real implementation, you'd harmonize the schemas
    return df1_with_source


@task
def clean_and_validate_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean and validate the horse racing data.
    """
    cleaned_df = (
        df
        .with_columns([
            pl.col("race_date").str.to_date().alias("race_date"),
            pl.col("odds").cast(pl.Float64),
            pl.col("weight").cast(pl.Float64),
            pl.col("barrier").cast(pl.Int32),
            pl.col("finish_position").cast(pl.Int32)
        ])
        .filter(pl.col("odds") > 0)  # Remove invalid odds
        .filter(pl.col("finish_position") > 0)  # Remove scratched horses
    )
    
    return cleaned_df


@task
def save_to_file(df: pl.DataFrame, file_path: str) -> str:
    """
    Save dataframe to parquet file in raw_data directory.
    """
    output_path = Path("raw_data") / file_path
    output_path.parent.mkdir(exist_ok=True)
    
    df.write_parquet(output_path)
    
    return str(output_path)


@task
def log_data_summary(df: pl.DataFrame, file_path: str) -> dict:
    """
    Log summary statistics about the collected data.
    """
    summary = {
        "timestamp": datetime.now().isoformat(),
        "file_path": file_path,
        "total_records": len(df),
        "unique_horses": df.select("horse_name").n_unique(),
        "unique_tracks": df.select("track").n_unique(),
        "date_range": {
            "min_date": str(df.select("race_date").min().item()),
            "max_date": str(df.select("race_date").max().item())
        }
    }
    
    print(f"Data collection summary: {summary}")
    return summary


@flow(name="horse-racing-data-pipeline", log_prints=True)
def grab_horse_racing_data():
    """
    Main flow to grab horse racing data from multiple sources.
    Runs daily to collect latest race results.
    """
    print("🏇 Starting horse racing data collection...")
    
    # Fetch data from multiple sources
    loveracing_data = fetch_sample_data()
    trackside_data = fetch_trackside_data()
    
    # Combine data sources
    combined_data = combine_data_sources(loveracing_data, trackside_data)
    
    # Clean and validate
    clean_data = clean_and_validate_data(combined_data)
    
    # Generate filename with current date
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"horse_racing_data_{today}.parquet"
    
    # Save to file
    saved_path = save_to_file(clean_data, filename)
    
    # Log summary
    summary = log_data_summary(clean_data, saved_path)
    
    print(f"✅ Data collection complete! Saved to: {saved_path}")
    return summary


if __name__ == "__main__":
    # Run the flow
    result = grab_horse_racing_data()
    print(f"Pipeline completed successfully: {result}")