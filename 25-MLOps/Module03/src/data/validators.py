"""
Data Validation with Polars - Learn Type Safety & Quality Checks

LEARNING OBJECTIVES:
1. Understand Polars schema validation vs Pandas loose typing
2. Learn data quality metrics and monitoring
3. Practice error handling for bad data
4. Implement domain-specific validation rules

POLARS POWER: 
- Strict schemas prevent silent errors
- Fast validation on large datasets  
- Built-in null handling
- Expressive data quality checks
"""

import polars as pl
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Container for validation results - learn about dataclasses!"""
    is_valid: bool
    errors: List[str]
    warnings: List[str] 
    stats: Dict[str, Any]


class RaceDataValidator:
    """
    Validator for horse racing data using Polars schemas.
    
    LEARNING COMPARISON - Polars vs Pandas:
    
    Pandas (loose):
    ```python
    df['odds'] = pd.to_numeric(df['odds'], errors='coerce')  # Silent NaNs!
    ```
    
    Polars (strict):
    ```python
    df = df.with_columns(pl.col('odds').cast(pl.Float64, strict=True))  # Fails loudly!
    ```
    
    TODO FOR STUDENT:
    1. [ ] Compare schema validation speed vs Pandas dtype checking
    2. [ ] Add custom validation rules for horse racing domain
    3. [ ] Implement data quality scoring
    4. [ ] Create validation reports for monitoring
    """
    
    # Define expected schema for race data
    RACE_SCHEMA = {
        'date': pl.Date,
        'race_time': pl.Utf8,
        'race_number': pl.Int32,
        'track': pl.Utf8,
        'distance': pl.Int32,  # Distance in meters
        'source': pl.Utf8,
        'scraped_at': pl.Datetime
    }
    
    # Schema for horse data within races
    HORSE_SCHEMA = {
        'name': pl.Utf8,
        'jockey': pl.Utf8,
        'trainer': pl.Utf8,
        'barrier': pl.Int32,
        'weight': pl.Float64,  # Weight in kg
        'odds': pl.Float64,
        'finish_position': pl.Int32,  # 0 if race not finished
    }
    
    @staticmethod
    def validate_scraped_data(raw_data: List[Dict[str, Any]]) -> ValidationResult:
        """
        Validate raw scraped data before processing.
        
        LEARNING NOTE: This shows you how to catch data quality issues
        early in your pipeline, before they cause problems downstream.
        
        TODO FOR STUDENT:
        1. Add more domain-specific validation rules
        2. Create validation metrics for monitoring
        3. Handle edge cases (scratched horses, abandoned races)
        """
        errors = []
        warnings = []
        stats = {
            'total_records': len(raw_data),
            'races_processed': 0,
            'horses_processed': 0,
            'validation_timestamp': datetime.now().isoformat()
        }
        
        if not raw_data:
            errors.append("No data provided for validation")
            return ValidationResult(False, errors, warnings, stats)
        
        # Convert to DataFrame for validation
        try:
            df = pl.DataFrame(raw_data)
            stats['races_processed'] = len(df)
        except Exception as e:
            errors.append(f"Failed to create DataFrame: {e}")
            return ValidationResult(False, errors, warnings, stats)
        
        # Required fields check
        required_fields = ['date', 'race_number', 'track', 'source']
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            errors.append(f"Missing required fields: {missing_fields}")
        
        # Data quality checks
        validation_checks = [
            RaceDataValidator._check_date_validity(df),
            RaceDataValidator._check_race_numbers(df),
            RaceDataValidator._check_track_names(df),
            RaceDataValidator._check_horse_data_quality(df),
        ]
        
        for check_errors, check_warnings in validation_checks:
            errors.extend(check_errors)
            warnings.extend(check_warnings)
        
        # Calculate data quality score
        total_checks = len(validation_checks) * 4  # Assume 4 checks per validation
        failed_checks = len(errors)
        stats['data_quality_score'] = max(0, (total_checks - failed_checks) / total_checks)
        
        is_valid = len(errors) == 0
        return ValidationResult(is_valid, errors, warnings, stats)
    
    @staticmethod
    def _check_date_validity(df: pl.DataFrame) -> Tuple[List[str], List[str]]:
        """
        Validate date fields.
        
        TODO FOR STUDENT: Learn about Polars date handling vs Pandas
        """
        errors = []
        warnings = []
        
        if 'date' not in df.columns:
            errors.append("Date column missing")
            return errors, warnings
        
        try:
            # TODO FOR STUDENT: Compare this with Pandas date parsing
            date_col = df.select(pl.col('date').str.to_date())
            
            # Check for null dates
            null_dates = df.filter(pl.col('date').is_null()).height
            if null_dates > 0:
                errors.append(f"{null_dates} records have null dates")
            
            # Check for future dates (suspicious)
            today = date.today()
            future_dates = df.filter(pl.col('date').str.to_date() > today).height
            if future_dates > 0:
                warnings.append(f"{future_dates} records have future dates")
                
        except Exception as e:
            errors.append(f"Date validation failed: {e}")
        
        return errors, warnings
    
    @staticmethod  
    def _check_race_numbers(df: pl.DataFrame) -> Tuple[List[str], List[str]]:
        """
        Validate race numbers are reasonable.
        
        LEARNING NOTE: Domain knowledge is crucial for validation!
        Most tracks run 8-12 races per day.
        """
        errors = []
        warnings = []
        
        if 'race_number' not in df.columns:
            return errors, warnings
        
        try:
            # Race numbers should be positive and reasonable (1-15 typically)
            invalid_races = df.filter(
                (pl.col('race_number') < 1) | (pl.col('race_number') > 15)
            ).height
            
            if invalid_races > 0:
                warnings.append(f"{invalid_races} records have unusual race numbers")
                
        except Exception as e:
            errors.append(f"Race number validation failed: {e}")
        
        return errors, warnings
    
    @staticmethod
    def _check_track_names(df: pl.DataFrame) -> Tuple[List[str], List[str]]:
        """
        Validate track names and identify potential data quality issues.
        
        TODO FOR STUDENT: Build a reference list of valid NZ track names
        """
        errors = []
        warnings = []
        
        if 'track' not in df.columns:
            return errors, warnings
        
        try:
            # Check for empty track names
            empty_tracks = df.filter(pl.col('track').is_null() | (pl.col('track') == "")).height
            if empty_tracks > 0:
                errors.append(f"{empty_tracks} records have missing track names")
            
            # TODO FOR STUDENT: Add valid track name checking
            # known_tracks = ['Ellerslie', 'Trentham', 'Riccarton', 'Te Rapa', ...]
            # unknown_tracks = df.filter(~pl.col('track').is_in(known_tracks))
            
        except Exception as e:
            errors.append(f"Track validation failed: {e}")
            
        return errors, warnings
    
    @staticmethod
    def _check_horse_data_quality(df: pl.DataFrame) -> Tuple[List[str], List[str]]:
        """
        Validate horse-specific data if present.
        
        TODO FOR STUDENT: 
        1. Check odds are positive and reasonable (0.1 to 1000)
        2. Validate barrier numbers (1-20 typically)
        3. Check weight ranges (50-65kg typically)
        4. Ensure jockey/trainer names aren't empty
        """
        errors = []
        warnings = []
        
        # This is a placeholder - horse data might be nested
        # TODO: Implement horse data validation when you understand the structure
        
        if 'horses' in df.columns:
            warnings.append("Horse data validation not yet implemented - TODO for student")
        
        return errors, warnings
    
    @staticmethod
    def create_polars_schema(race_data: pl.DataFrame) -> pl.DataFrame:
        """
        Apply strict Polars schema to ensure data quality.
        
        LEARNING SHOWCASE: See how Polars enforces types vs Pandas silent coercion
        
        TODO FOR STUDENT:
        1. Compare performance with equivalent Pandas operations
        2. Add schema evolution handling for new fields
        3. Implement custom validation rules
        """
        try:
            # Apply schema with strict type checking
            validated_df = race_data.with_columns([
                # Date handling - Polars is very strict about date formats
                pl.col('date').str.to_date('%Y-%m-%d').alias('race_date'),
                
                # Numeric validations with strict casting
                pl.col('race_number').cast(pl.Int32, strict=True),
                
                # String cleaning and validation
                pl.col('track').str.strip_chars().str.to_uppercase(),
                pl.col('source').str.strip_chars().str.to_lowercase(),
                
                # TODO FOR STUDENT: Add more schema rules
                # pl.col('distance').cast(pl.Int32, strict=True),
                # pl.col('odds').cast(pl.Float64, strict=True),
            ])
            
            logger.info(f"Schema validation successful for {len(validated_df)} records")
            return validated_df
            
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            raise


# Convenience function for quick validation
def validate_scraped_data(raw_data: List[Dict[str, Any]]) -> ValidationResult:
    """Quick validation function for use in Prefect flows."""
    return RaceDataValidator.validate_scraped_data(raw_data)


# TODO FOR STUDENT - TESTING AND LEARNING:
if __name__ == "__main__":
    """
    Test validation with sample data.
    
    LEARNING EXERCISE: 
    1. Create good and bad test data
    2. See how validation catches issues
    3. Compare with Pandas validation approaches
    """
    
    # Sample good data
    good_data = [
        {
            'date': '2024-01-15',
            'race_time': '14:30',
            'race_number': 1,
            'track': 'Ellerslie',
            'distance': 1200,
            'source': 'tab_nz',
            'scraped_at': datetime.now().isoformat()
        }
    ]
    
    # Sample bad data for testing
    bad_data = [
        {
            'date': None,  # Missing date
            'race_number': -1,  # Invalid race number
            'track': '',  # Empty track
            'source': 'unknown'
        }
    ]
    
    print("🔍 Testing Data Validation")
    print("=" * 40)
    
    print("1. Testing good data:")
    result = validate_scraped_data(good_data)
    print(f"Valid: {result.is_valid}")
    print(f"Errors: {result.errors}")
    print(f"Stats: {result.stats}")
    
    print("\\n2. Testing bad data:")
    result = validate_scraped_data(bad_data)
    print(f"Valid: {result.is_valid}")
    print(f"Errors: {result.errors}")
    print(f"Warnings: {result.warnings}")
    
    print("\\n🎯 TODO: Add more validation rules and test edge cases!")