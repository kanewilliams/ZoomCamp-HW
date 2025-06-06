"""
Data Transformation with Polars - Feature Engineering for Horse Racing

LEARNING OBJECTIVES:
1. Master Polars expressions vs Pandas operations
2. Learn lazy evaluation for performance
3. Implement domain-specific feature engineering
4. Practice window functions and aggregations

POLARS PERFORMANCE SHOWCASE:
- Lazy evaluation: Build complex queries without execution
- Columnar operations: Process millions of rows efficiently  
- Memory efficiency: Minimal copying vs Pandas
- Parallel processing: Automatic multi-threading
"""

import polars as pl
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class RaceDataTransformer:
    """
    Transform and engineer features for horse racing data.
    
    LEARNING COMPARISON - Polars vs Pandas Performance:
    
    Pandas approach:
    ```python
    df['win_rate'] = df.groupby('jockey')['finish_position'].apply(
        lambda x: (x == 1).sum() / len(x)
    )  # Slow, lots of copying
    ```
    
    Polars approach:
    ```python
    df = df.with_columns(
        pl.col('finish_position').eq(1).mean().over('jockey').alias('jockey_win_rate')
    )  # Fast, zero-copy, parallel
    ```
    
    TODO FOR STUDENT:
    1. [ ] Benchmark transformation speed vs equivalent Pandas code
    2. [ ] Implement lazy evaluation examples
    3. [ ] Add more horse racing domain features
    4. [ ] Create feature importance analysis
    """
    
    @staticmethod
    def clean_scraped_data(df: pl.DataFrame) -> pl.DataFrame:
        """
        Clean and standardize scraped racing data.
        
        LEARNING FOCUS: Polars string operations and data cleaning
        """
        try:
            cleaned_df = df.with_columns([
                # Date standardization
                pl.col('date').str.to_date('%Y-%m-%d').alias('race_date'),
                
                # Text cleaning - see how clean Polars syntax is!
                pl.col('track').str.strip_chars().str.to_uppercase().alias('track_clean'),
                pl.col('source').str.strip_chars().str.to_lowercase(),
                
                # Handle race times - convert to minutes from start of day
                pl.col('race_time').map_elements(
                    RaceDataTransformer._parse_race_time,
                    return_dtype=pl.Int32
                ).alias('race_minutes'),
                
                # TODO FOR STUDENT: Add more cleaning operations
                # pl.col('distance').str.extract(r'(\d+)').cast(pl.Int32).alias('distance_meters'),
                # pl.col('track_condition').str.to_lowercase().alias('going'),
            ])
            
            logger.info(f"Cleaned {len(cleaned_df)} race records")
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Data cleaning failed: {e}")
            raise
    
    @staticmethod
    def _parse_race_time(time_str: str) -> int:
        """
        Convert race time string to minutes from midnight.
        
        TODO FOR STUDENT: Handle different time formats you find on TAB.nz
        """
        try:
            if ':' in time_str:
                hours, minutes = time_str.split(':')
                return int(hours) * 60 + int(minutes)
            return 0
        except:
            return 0
    
    @staticmethod
    def engineer_basic_features(df: pl.DataFrame) -> pl.DataFrame:
        """
        Create basic features for horse racing analysis.
        
        LEARNING SHOWCASE: Window functions and aggregations in Polars
        """
        try:
            featured_df = df.with_columns([
                # Time-based features
                pl.col('race_date').dt.weekday().alias('day_of_week'),
                pl.col('race_date').dt.month().alias('month'),
                pl.col('race_minutes').floordiv(60).alias('race_hour'),
                
                # Track features
                pl.col('track_clean').value_counts().struct.field('counts').alias('track_race_count'),
                
                # TODO FOR STUDENT: Add horse-specific features when you have horse data
                # These would require nested data or separate horse DataFrame
                
                # Metadata features
                pl.col('scraped_at').str.to_datetime().alias('scraped_timestamp'),
                pl.lit(datetime.now().isoformat()).alias('processed_at'),
            ])
            
            logger.info(f"Engineered features for {len(featured_df)} records")
            return featured_df
            
        except Exception as e:
            logger.error(f"Feature engineering failed: {e}")
            raise
    
    @staticmethod 
    def engineer_horse_features(df: pl.DataFrame) -> pl.DataFrame:
        """
        Create horse and jockey performance features.
        
        TODO FOR STUDENT - MAJOR IMPLEMENTATION NEEDED:
        This requires horse-level data which you'll get from your TAB scraper.
        
        Features to implement:
        1. [ ] Jockey win rate (last 30 days)
        2. [ ] Trainer strike rate 
        3. [ ] Horse form (last 5 runs)
        4. [ ] Track/distance preference
        5. [ ] Weight-carrying ability
        6. [ ] Barrier performance
        7. [ ] Class adjustments
        8. [ ] Speed ratings
        
        POLARS POWER: Window functions make this easy!
        ```python
        df.with_columns([
            # Last 5 runs average finish position
            pl.col('finish_position').mean().over(
                pl.col('horse_name'), 
                order_by='race_date'
            ).slice(-5).alias('recent_form'),
            
            # Jockey win rate at this track
            pl.col('finish_position').eq(1).mean().over(
                ['jockey', 'track']
            ).alias('jockey_track_win_rate')
        ])
        ```
        """
        logger.warning("Horse feature engineering not implemented - needs horse-level data from scraper")
        
        # Placeholder for when you have horse data
        # TODO FOR STUDENT: Implement when your scraper returns horse information
        
        return df
    
    @staticmethod
    def create_betting_features(df: pl.DataFrame) -> pl.DataFrame:
        """
        Create features specifically for betting analysis.
        
        TODO FOR STUDENT - BETTING DOMAIN KNOWLEDGE:
        Research these betting concepts and implement:
        1. [ ] Overlay betting (finding value odds)
        2. [ ] Market movement analysis
        3. [ ] Public vs smart money indicators
        4. [ ] Form cycles and class adjustments
        5. [ ] Track bias calculations
        
        IMPORTANT: This is for LEARNING only - gambling can be addictive!
        """
        logger.info("Creating betting analysis features...")
        
        try:
            betting_df = df.with_columns([
                # Time to post features
                pl.col('race_minutes').alias('minutes_to_post'),  # Will need current time
                
                # Track characteristics
                pl.col('track_clean').map_elements(
                    RaceDataTransformer._get_track_characteristics,
                    return_dtype=pl.Utf8
                ).alias('track_type'),
                
                # TODO FOR STUDENT: Add odds-based features when available
                # pl.col('odds').log().alias('log_odds'),  # Odds probability
                # pl.col('odds').rank().alias('market_rank'),  # Market position
                
                # Race competitive features  
                pl.len().alias('field_size'),  # Number of runners
                
                # TODO: Weather and track condition features
                # pl.lit('Good').alias('track_condition'),  # From weather API
            ])
            
            return betting_df
            
        except Exception as e:
            logger.error(f"Betting feature engineering failed: {e}")
            raise
    
    @staticmethod
    def _get_track_characteristics(track_name: str) -> str:
        """
        Map track names to characteristics.
        
        TODO FOR STUDENT: Research NZ track characteristics:
        - Left/right handed
        - Flat/jumping
        - Track circumference  
        - Typical going conditions
        """
        track_info = {
            'ELLERSLIE': 'flat_left_2000m',
            'TRENTHAM': 'flat_right_2200m', 
            'RICCARTON': 'flat_right_1600m',
            'TE RAPA': 'flat_right_1600m',
            # TODO: Add more tracks as you research them
        }
        
        return track_info.get(track_name, 'unknown')
    
    @staticmethod
    def aggregate_daily_summary(df: pl.DataFrame) -> pl.DataFrame:
        """
        Create daily racing summary statistics.
        
        LEARNING FOCUS: Polars aggregation syntax vs Pandas groupby
        """
        try:
            daily_summary = df.group_by('race_date').agg([
                pl.len().alias('total_races'),
                pl.col('track_clean').n_unique().alias('unique_tracks'),
                pl.col('race_minutes').min().alias('first_race_time'),
                pl.col('race_minutes').max().alias('last_race_time'),
                pl.col('scraped_at').max().alias('latest_scrape'),
                
                # TODO FOR STUDENT: Add more aggregations
                # pl.col('total_runners').sum().alias('total_runners'),
                # pl.col('average_odds').mean().alias('avg_market_odds'),
            ])
            
            logger.info(f"Created daily summary for {len(daily_summary)} days")
            return daily_summary
            
        except Exception as e:
            logger.error(f"Daily aggregation failed: {e}")
            raise
    
    @staticmethod
    def lazy_transformation_example(df: pl.LazyFrame) -> pl.LazyFrame:
        """
        Demonstrate lazy evaluation for performance.
        
        LEARNING SHOWCASE: Polars lazy evaluation vs Pandas immediate execution
        
        Polars builds an execution plan and optimizes it before running.
        This can dramatically improve performance on large datasets!
        """
        logger.info("Building lazy transformation pipeline...")
        
        # Build complex transformation pipeline without execution
        lazy_result = (
            df
            .filter(pl.col('race_date') > datetime(2024, 1, 1))  # Filter first
            .with_columns([
                pl.col('track_clean').str.to_uppercase(),
                pl.col('race_minutes').cast(pl.Int32),
            ])
            .group_by(['race_date', 'track_clean'])
            .agg([
                pl.len().alias('races_count'),
                pl.col('race_minutes').mean().alias('avg_race_time'),
            ])
            .sort(['race_date', 'track_clean'])
        )
        
        # TODO FOR STUDENT: Call .collect() to execute and compare performance
        # result = lazy_result.collect()
        
        logger.info("Lazy pipeline built - call .collect() to execute")
        return lazy_result


# TODO FOR STUDENT - PERFORMANCE TESTING:
if __name__ == "__main__":
    """
    Test transformations and compare with Pandas equivalents.
    
    LEARNING EXERCISE:
    1. Create sample data
    2. Time Polars vs Pandas operations
    3. Check memory usage differences
    4. Explore lazy evaluation benefits
    """
    
    # Create sample data for testing
    sample_data = pl.DataFrame({
        'date': ['2024-01-15', '2024-01-15', '2024-01-16'] * 100,
        'race_time': ['14:30', '15:05', '16:20'] * 100,
        'track': ['Ellerslie', 'Trentham', 'Riccarton'] * 100,
        'race_number': [1, 2, 3] * 100,
        'source': ['tab_nz'] * 300,
        'scraped_at': [datetime.now().isoformat()] * 300,
    })
    
    print("🏇 Testing Data Transformations")
    print("=" * 40)
    
    transformer = RaceDataTransformer()
    
    print("1. Testing data cleaning...")
    cleaned = transformer.clean_scraped_data(sample_data)
    print(f"Cleaned data shape: {cleaned.shape}")
    print(cleaned.head())
    
    print("\\n2. Testing feature engineering...")
    featured = transformer.engineer_basic_features(cleaned)
    print(f"Featured data columns: {featured.columns}")
    
    print("\\n3. Testing aggregation...")
    summary = transformer.aggregate_daily_summary(featured)
    print(summary)
    
    print("\\n4. Testing lazy evaluation...")
    lazy_df = sample_data.lazy()
    lazy_result = transformer.lazy_transformation_example(lazy_df)
    print("Lazy pipeline created - execute with .collect()")
    
    print("\\n🎯 TODO: Implement horse-level features and benchmark vs Pandas!")