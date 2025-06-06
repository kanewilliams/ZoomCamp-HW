"""
Data Export Module - Polars Output Optimization

LEARNING OBJECTIVES:
1. Compare different output formats (Parquet, CSV, JSON)
2. Understand compression and performance trade-offs
3. Learn partitioning strategies for large datasets
4. Practice file format optimization

POLARS EXPORT ADVANTAGES:
- Native Parquet support (faster than Pandas)
- Streaming writes for large datasets
- Automatic compression optimization
- Schema preservation across formats
"""

import polars as pl
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class DataExporter:
    """
    Export racing data in various optimized formats.
    
    LEARNING COMPARISON - Export Performance:
    
    Pandas CSV export:
    ```python
    df.to_csv('data.csv', index=False)  # Single-threaded, slow
    ```
    
    Polars CSV export:
    ```python
    df.write_csv('data.csv')  # Multi-threaded, faster
    ```
    
    Polars Parquet (recommended):
    ```python
    df.write_parquet('data.parquet')  # Columnar, compressed, super fast
    ```
    
    TODO FOR STUDENT:
    1. [ ] Benchmark export speeds across formats
    2. [ ] Test compression ratios
    3. [ ] Implement partitioned writes for large datasets
    4. [ ] Add schema evolution handling
    """
    
    def __init__(self, output_dir: str = "processed_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Track export statistics
        self.export_stats = {
            'total_exports': 0,
            'total_records': 0,
            'formats_used': set(),
            'last_export': None
        }
    
    def export_parquet(
        self, 
        df: pl.DataFrame, 
        filename: str,
        partition_cols: Optional[List[str]] = None,
        compression: str = 'snappy'
    ) -> Path:
        """
        Export to Parquet format (recommended for performance).
        
        LEARNING FOCUS: Why Parquet is ideal for analytics:
        - Columnar storage (only read needed columns)
        - Excellent compression (often 10x smaller than CSV)
        - Schema evolution support
        - Fast filtering and aggregation
        
        TODO FOR STUDENT:
        1. Compare file sizes: CSV vs Parquet vs JSON
        2. Test read performance differences
        3. Experiment with different compression algorithms
        """
        try:
            output_path = self.output_dir / f"{filename}.parquet"
            
            if partition_cols:
                # Partitioned write for large datasets
                logger.info(f"Writing partitioned Parquet to {output_path} by {partition_cols}")
                # TODO FOR STUDENT: Implement partitioning when you have large datasets
                # df.write_parquet(output_path, partition_by=partition_cols)
                df.write_parquet(output_path, compression=compression)
            else:
                logger.info(f"Writing Parquet to {output_path}")
                df.write_parquet(output_path, compression=compression)
            
            self._update_stats(len(df), 'parquet')
            logger.info(f"Exported {len(df)} records to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Parquet export failed: {e}")
            raise
    
    def export_csv(
        self, 
        df: pl.DataFrame, 
        filename: str,
        include_header: bool = True
    ) -> Path:
        """
        Export to CSV format (for compatibility with other tools).
        
        LEARNING NOTE: CSV is human-readable but inefficient for large data.
        Use for small datasets or when other tools require CSV input.
        """
        try:
            output_path = self.output_dir / f"{filename}.csv"
            logger.info(f"Writing CSV to {output_path}")
            
            df.write_csv(output_path, include_header=include_header)
            
            self._update_stats(len(df), 'csv')
            logger.info(f"Exported {len(df)} records to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"CSV export failed: {e}")
            raise
    
    def export_json_lines(self, df: pl.DataFrame, filename: str) -> Path:
        """
        Export to JSONL format (one JSON object per line).
        
        LEARNING USE CASE: JSONL is great for:
        - Streaming processing
        - Log-like data
        - APIs that expect JSON
        - Easy debugging (human readable)
        """
        try:
            output_path = self.output_dir / f"{filename}.jsonl"
            logger.info(f"Writing JSONL to {output_path}")
            
            # Convert to JSON strings and write
            with open(output_path, 'w') as f:
                for row in df.iter_rows(named=True):
                    f.write(json.dumps(row, default=str) + '\\n')
            
            self._update_stats(len(df), 'jsonl')
            logger.info(f"Exported {len(df)} records to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"JSONL export failed: {e}")
            raise
    
    def export_for_ml_training(
        self, 
        df: pl.DataFrame, 
        target_col: str,
        test_size: float = 0.2,
        random_state: int = 42
    ) -> Dict[str, Path]:
        """
        Export datasets split for ML training.
        
        TODO FOR STUDENT - ML PREPARATION:
        1. [ ] Implement proper train/validation/test splits
        2. [ ] Handle time-based splits for time series data
        3. [ ] Export feature importance metadata
        4. [ ] Create data dictionaries for features
        
        IMPORTANT: For horse racing, use time-based splits, not random!
        """
        try:
            logger.info(f"Preparing ML training datasets with target: {target_col}")
            
            # TODO FOR STUDENT: Implement time-based splitting
            # For racing data, you want to train on past data, test on future
            
            # Simple random split (replace with time-based)
            shuffled = df.sample(n=len(df), shuffle=True, seed=random_state)
            split_idx = int(len(shuffled) * (1 - test_size))
            
            train_df = shuffled[:split_idx]
            test_df = shuffled[split_idx:]
            
            # Export splits
            paths = {
                'train': self.export_parquet(train_df, 'ml_train'),
                'test': self.export_parquet(test_df, 'ml_test'),
            }
            
            # Export metadata
            metadata = {
                'target_column': target_col,
                'train_size': len(train_df),
                'test_size': len(test_df),
                'split_date': datetime.now().isoformat(),
                'features': list(df.columns),
                'random_state': random_state
            }
            
            metadata_path = self.output_dir / 'ml_metadata.json'
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            paths['metadata'] = metadata_path
            
            logger.info(f"ML datasets exported: train={len(train_df)}, test={len(test_df)}")
            return paths
            
        except Exception as e:
            logger.error(f"ML export failed: {e}")
            raise
    
    def export_daily_summary_report(self, df: pl.DataFrame, date_str: str) -> Path:
        """
        Create a daily summary report in multiple formats.
        
        LEARNING PURPOSE: Show how different formats serve different needs:
        - Parquet: For analysis tools
        - CSV: For spreadsheets  
        - JSON: For web APIs
        """
        try:
            logger.info(f"Creating daily summary report for {date_str}")
            
            # Create summary statistics
            summary_stats = df.select([
                pl.len().alias('total_races'),
                pl.col('track_clean').n_unique().alias('unique_tracks'),
                pl.col('race_minutes').min().alias('first_race'),
                pl.col('race_minutes').max().alias('last_race'),
                pl.col('source').value_counts().alias('sources'),
            ])
            
            # Export in multiple formats for different use cases
            base_filename = f"daily_summary_{date_str}"
            
            parquet_path = self.export_parquet(summary_stats, base_filename)
            csv_path = self.export_csv(summary_stats, base_filename)
            
            # Create human-readable report
            report_path = self.output_dir / f"{base_filename}_report.txt"
            with open(report_path, 'w') as f:
                f.write(f"Daily Racing Summary - {date_str}\\n")
                f.write("=" * 40 + "\\n\\n")
                f.write(str(summary_stats))
                f.write(f"\\n\\nGenerated at: {datetime.now()}\\n")
            
            logger.info(f"Daily summary exported to {parquet_path}")
            return parquet_path
            
        except Exception as e:
            logger.error(f"Daily summary export failed: {e}")
            raise
    
    def _update_stats(self, record_count: int, format_type: str):
        """Update export statistics for monitoring."""
        self.export_stats['total_exports'] += 1
        self.export_stats['total_records'] += record_count
        self.export_stats['formats_used'].add(format_type)
        self.export_stats['last_export'] = datetime.now().isoformat()
    
    def get_export_summary(self) -> Dict[str, Any]:
        """Get export statistics for monitoring and reporting."""
        stats = self.export_stats.copy()
        stats['formats_used'] = list(stats['formats_used'])  # Convert set to list for JSON
        return stats
    
    def cleanup_old_files(self, days_to_keep: int = 30):
        """
        Clean up old export files to manage disk space.
        
        TODO FOR STUDENT: Implement smart cleanup:
        1. [ ] Keep important files (summaries, ML datasets)
        2. [ ] Archive to cheaper storage before deletion
        3. [ ] Implement retention policies by file type
        """
        logger.info(f"Cleaning up files older than {days_to_keep} days")
        # TODO: Implement cleanup logic
        pass


# TODO FOR STUDENT - PERFORMANCE BENCHMARKING:
if __name__ == "__main__":
    """
    Benchmark export performance across different formats.
    
    LEARNING EXERCISE:
    1. Create datasets of different sizes
    2. Time export operations
    3. Measure file sizes
    4. Test read-back performance
    """
    
    # Create sample data for benchmarking
    sample_size = 10000
    sample_data = pl.DataFrame({
        'race_date': ['2024-01-15'] * sample_size,
        'track': ['Ellerslie'] * sample_size,
        'race_number': list(range(1, sample_size + 1)),
        'horse_name': [f'Horse_{i}' for i in range(sample_size)],
        'jockey': [f'Jockey_{i % 100}' for i in range(sample_size)],
        'odds': [2.5 + (i % 50) for i in range(sample_size)],
        'finish_position': [(i % 8) + 1 for i in range(sample_size)],
    })
    
    print("🏇 Testing Data Export Performance")
    print("=" * 45)
    
    exporter = DataExporter("test_exports")
    
    # Benchmark different formats
    import time
    
    formats_to_test = [
        ('parquet', exporter.export_parquet),
        ('csv', exporter.export_csv),
        ('jsonl', exporter.export_json_lines),
    ]
    
    for format_name, export_func in formats_to_test:
        start_time = time.time()
        
        if format_name == 'jsonl':
            path = export_func(sample_data, f'benchmark_{format_name}')
        else:
            path = export_func(sample_data, f'benchmark_{format_name}')
        
        export_time = time.time() - start_time
        file_size = path.stat().st_size / 1024 / 1024  # MB
        
        print(f"{format_name.upper()}:")
        print(f"  Export time: {export_time:.2f}s")
        print(f"  File size: {file_size:.2f}MB")
        print(f"  Records/second: {sample_size/export_time:.0f}")
        print()
    
    print("Export statistics:")
    print(exporter.get_export_summary())
    
    print("\\n🎯 TODO: Test with larger datasets and implement partitioning!")