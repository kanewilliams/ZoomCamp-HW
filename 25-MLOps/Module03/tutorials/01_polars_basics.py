"""
Polars Learning Tutorial #1: Basics and Performance

LEARNING MISSION: Master Polars fundamentals through horse racing examples

This tutorial covers:
1. DataFrames vs Pandas comparison
2. Lazy evaluation concepts
3. Column operations and expressions
4. Performance benchmarking
5. Memory efficiency

RUN THIS: python tutorials/01_polars_basics.py
"""

import polars as pl
import pandas as pd
import time
import sys
from typing import Tuple
import numpy as np

print("🏇 Polars Learning Tutorial #1: Basics and Performance")
print("=" * 60)


def create_sample_racing_data(size: int = 10000) -> Tuple[pl.DataFrame, pd.DataFrame]:
    """Create sample racing data for Polars vs Pandas comparison."""
    
    print(f"📊 Creating {size:,} sample racing records...")
    
    # Generate sample data
    np.random.seed(42)  # For reproducible results
    
    data = {
        'race_date': pd.date_range('2023-01-01', periods=size//100, freq='D').repeat(100)[:size],
        'track': np.random.choice(['Ellerslie', 'Trentham', 'Riccarton', 'Te Rapa'], size),
        'race_number': np.random.randint(1, 12, size),
        'horse_name': [f'Horse_{i}' for i in range(size)],
        'jockey': np.random.choice([f'Jockey_{i}' for i in range(50)], size),
        'trainer': np.random.choice([f'Trainer_{i}' for i in range(30)], size),
        'barrier': np.random.randint(1, 15, size),
        'weight': np.random.uniform(50, 65, size),
        'odds': np.random.uniform(1.1, 100.0, size),
        'finish_position': np.random.randint(1, 15, size),
        'margin': np.random.uniform(0, 20, size),
        'prize_money': np.random.randint(5000, 100000, size)
    }
    
    # Create both DataFrames
    polars_df = pl.DataFrame(data)
    pandas_df = pd.DataFrame(data)
    
    print(f"✅ Created sample data: {polars_df.shape[0]:,} rows, {polars_df.shape[1]} columns")
    return polars_df, pandas_df


def tutorial_1_basic_operations():
    """Tutorial 1: Basic DataFrame operations."""
    
    print("\\n" + "="*50)
    print("📚 TUTORIAL 1: Basic DataFrame Operations")
    print("="*50)
    
    # Create sample data
    df_pl, df_pd = create_sample_racing_data(1000)
    
    print("\\n1️⃣ DataFrame Info Comparison:")
    print("-" * 30)
    
    print("POLARS:")
    print(f"  Shape: {df_pl.shape}")
    print(f"  Columns: {df_pl.columns}")
    print(f"  Dtypes: {df_pl.dtypes}")
    
    print("\\nPANDAS:")
    print(f"  Shape: {df_pd.shape}")
    print(f"  Memory usage: {df_pd.memory_usage(deep=True).sum():,} bytes")
    
    print("\\n2️⃣ Basic Statistics:")
    print("-" * 20)
    
    # Polars way
    print("POLARS describe():")
    print(df_pl.select(['weight', 'odds', 'finish_position']).describe())
    
    print("\\nPANDAS describe():")
    print(df_pd[['weight', 'odds', 'finish_position']].describe())
    
    print("\\n3️⃣ Filtering Examples:")
    print("-" * 20)
    
    # Polars filtering
    winners_pl = df_pl.filter(pl.col('finish_position') == 1)
    print(f"Polars winners: {len(winners_pl)} races")
    
    # Pandas filtering  
    winners_pd = df_pd[df_pd['finish_position'] == 1]
    print(f"Pandas winners: {len(winners_pd)} races")
    
    print("\\n4️⃣ Aggregation Examples:")
    print("-" * 25)
    
    # Polars aggregation
    track_stats_pl = df_pl.group_by('track').agg([
        pl.len().alias('total_races'),
        pl.col('odds').mean().alias('avg_odds'),
        pl.col('prize_money').sum().alias('total_prize_money')
    ]).sort('total_races', descending=True)
    
    print("POLARS track statistics:")
    print(track_stats_pl)
    
    # Pandas aggregation
    track_stats_pd = df_pd.groupby('track').agg({
        'race_date': 'count',
        'odds': 'mean',
        'prize_money': 'sum'
    }).rename(columns={'race_date': 'total_races', 'odds': 'avg_odds'})
    
    print("\\nPANDAS track statistics:")
    print(track_stats_pd)


def tutorial_2_lazy_evaluation():
    """Tutorial 2: Understanding lazy evaluation."""
    
    print("\\n" + "="*50)
    print("📚 TUTORIAL 2: Lazy Evaluation Power")
    print("="*50)
    
    df_pl, _ = create_sample_racing_data(5000)
    
    print("\\n🚀 LAZY EVALUATION CONCEPT:")
    print("Polars can build a query plan without executing it!")
    print("This allows for optimization before execution.")
    
    # Build a lazy query
    print("\\n1️⃣ Building Lazy Query:")
    print("-" * 25)
    
    lazy_query = (
        df_pl.lazy()  # Convert to lazy
        .filter(pl.col('finish_position') <= 3)  # Top 3 finishers
        .with_columns([
            pl.col('odds').log().alias('log_odds'),  # Log transform
            (pl.col('weight') / pl.col('weight').mean()).alias('weight_ratio')
        ])
        .group_by(['track', 'jockey'])
        .agg([
            pl.len().alias('races'),
            pl.col('finish_position').mean().alias('avg_finish'),
            pl.col('log_odds').mean().alias('avg_log_odds')
        ])
        .filter(pl.col('races') >= 3)  # Minimum 3 races
        .sort('avg_finish')
    )
    
    print("✅ Lazy query built (not executed yet!)")
    print(f"Query type: {type(lazy_query)}")
    
    # Show the query plan
    print("\\n2️⃣ Query Optimization Plan:")
    print("-" * 30)
    print("Polars optimizes the query before execution:")
    # Note: .explain() shows the optimized query plan
    try:
        print(lazy_query.explain())
    except:
        print("(Query plan visualization not available in this version)")
    
    # Execute the query
    print("\\n3️⃣ Executing Query:")
    print("-" * 20)
    start_time = time.time()
    result = lazy_query.collect()  # Execute the lazy query
    exec_time = time.time() - start_time
    
    print(f"✅ Query executed in {exec_time:.4f} seconds")
    print(f"Result shape: {result.shape}")
    print("\\nTop jockey-track combinations by average finish:")
    print(result.head())
    
    print("\\n🎯 KEY LEARNING:")
    print("- .lazy() builds query without execution")
    print("- Polars optimizes the entire pipeline")
    print("- .collect() executes the optimized query")
    print("- This is much more efficient than step-by-step execution!")


def tutorial_3_performance_comparison():
    """Tutorial 3: Performance benchmarking."""
    
    print("\\n" + "="*50)
    print("📚 TUTORIAL 3: Performance Showdown")
    print("="*50)
    
    # Test with larger dataset
    sizes = [1000, 10000, 50000]
    
    results = []
    
    for size in sizes:
        print(f"\\n🏃‍♂️ Testing with {size:,} records:")
        print("-" * 35)
        
        df_pl, df_pd = create_sample_racing_data(size)
        
        # Test 1: Filtering and aggregation
        print("Test 1: Filter + Group + Aggregate")
        
        # Polars version
        start = time.time()
        result_pl = (
            df_pl
            .filter(pl.col('finish_position') <= 5)
            .group_by('jockey')
            .agg([
                pl.len().alias('races'),
                pl.col('finish_position').mean().alias('avg_finish'),
                pl.col('odds').mean().alias('avg_odds')
            ])
            .filter(pl.col('races') >= 3)
            .sort('avg_finish')
        )
        polars_time = time.time() - start
        
        # Pandas version
        start = time.time()
        filtered_pd = df_pd[df_pd['finish_position'] <= 5]
        grouped_pd = filtered_pd.groupby('jockey').agg({
            'race_date': 'count',
            'finish_position': 'mean',
            'odds': 'mean'
        })
        grouped_pd.columns = ['races', 'avg_finish', 'avg_odds']
        result_pd = grouped_pd[grouped_pd['races'] >= 3].sort_values('avg_finish')
        pandas_time = time.time() - start
        
        speedup = pandas_time / polars_time
        
        print(f"  Polars: {polars_time:.4f}s")
        print(f"  Pandas: {pandas_time:.4f}s")
        print(f"  Speedup: {speedup:.1f}x faster!")
        
        results.append({
            'size': size,
            'polars_time': polars_time,
            'pandas_time': pandas_time,
            'speedup': speedup
        })
        
        # Test 2: Complex calculations
        print("Test 2: Complex calculations")
        
        # Polars version with expressions
        start = time.time()
        complex_pl = df_pl.with_columns([
            (pl.col('weight') / pl.col('weight').mean().over('track')).alias('relative_weight'),
            pl.col('odds').log().alias('log_odds'),
            (1.0 / pl.col('odds')).alias('implied_probability'),
            pl.col('finish_position').rank().over(['race_date', 'race_number']).alias('field_rank')
        ])
        polars_complex_time = time.time() - start
        
        # Pandas version
        start = time.time()
        df_pd_copy = df_pd.copy()
        df_pd_copy['relative_weight'] = df_pd_copy['weight'] / df_pd_copy.groupby('track')['weight'].transform('mean')
        df_pd_copy['log_odds'] = np.log(df_pd_copy['odds'])
        df_pd_copy['implied_probability'] = 1.0 / df_pd_copy['odds']
        df_pd_copy['field_rank'] = df_pd_copy.groupby(['race_date', 'race_number'])['finish_position'].rank()
        pandas_complex_time = time.time() - start
        
        complex_speedup = pandas_complex_time / polars_complex_time
        
        print(f"  Polars: {polars_complex_time:.4f}s")
        print(f"  Pandas: {pandas_complex_time:.4f}s") 
        print(f"  Speedup: {complex_speedup:.1f}x faster!")
    
    print("\\n📊 PERFORMANCE SUMMARY:")
    print("-" * 25)
    for result in results:
        print(f"Size {result['size']:,}: {result['speedup']:.1f}x speedup")
    
    print("\\n🎯 KEY LEARNINGS:")
    print("- Polars is consistently faster than Pandas")
    print("- Performance gap increases with data size")
    print("- Polars expressions are very efficient")
    print("- Lazy evaluation enables better optimization")


def tutorial_4_expressions_showcase():
    """Tutorial 4: Polars expressions showcase."""
    
    print("\\n" + "="*50)
    print("📚 TUTORIAL 4: Powerful Expressions")
    print("="*50)
    
    df_pl, _ = create_sample_racing_data(2000)
    
    print("\\n🔥 EXPRESSION EXAMPLES:")
    print("Polars expressions are composable and optimized")
    
    # Example 1: Window functions
    print("\\n1️⃣ Window Functions:")
    print("-" * 20)
    
    result = df_pl.with_columns([
        # Rank within each race
        pl.col('finish_position').rank().over(['race_date', 'race_number']).alias('race_rank'),
        
        # Running average for each jockey
        pl.col('finish_position').mean().over('jockey').alias('jockey_avg_finish'),
        
        # Best finish for each horse
        pl.col('finish_position').min().over('horse_name').alias('horse_best_finish'),
        
        # Track record count
        pl.len().over('track').alias('track_race_count')
    ])
    
    print("Added window function columns:")
    print(result.select(['horse_name', 'jockey', 'finish_position', 'jockey_avg_finish', 'horse_best_finish']).head())
    
    # Example 2: Complex conditional logic
    print("\\n2️⃣ Conditional Logic:")
    print("-" * 22)
    
    result = df_pl.with_columns([
        # Performance categories
        pl.when(pl.col('finish_position') == 1)
        .then(pl.lit('Winner'))
        .when(pl.col('finish_position') <= 3)
        .then(pl.lit('Placed'))
        .when(pl.col('finish_position') <= 5)
        .then(pl.lit('Top 5'))
        .otherwise(pl.lit('Also Ran'))
        .alias('performance_category'),
        
        # Odds categories
        pl.when(pl.col('odds') < 2.0)
        .then(pl.lit('Favorite'))
        .when(pl.col('odds') < 5.0)
        .then(pl.lit('Short Price'))
        .when(pl.col('odds') < 15.0)
        .then(pl.lit('Mid Price'))
        .otherwise(pl.lit('Long Shot'))
        .alias('odds_category')
    ])
    
    print("Performance and odds categorization:")
    print(result.group_by(['performance_category', 'odds_category']).len().sort('len', descending=True))
    
    # Example 3: String operations
    print("\\n3️⃣ String Operations:")
    print("-" * 20)
    
    result = df_pl.with_columns([
        # Extract jockey number
        pl.col('jockey').str.extract(r'(\\d+)').alias('jockey_number'),
        
        # Create display name
        pl.concat_str([
            pl.col('horse_name'),
            pl.lit(' ('),
            pl.col('jockey'),
            pl.lit(')')
        ]).alias('horse_jockey_display'),
        
        # Track abbreviation
        pl.col('track').str.slice(0, 3).str.to_uppercase().alias('track_abbrev')
    ])
    
    print("String manipulation results:")
    print(result.select(['horse_name', 'jockey', 'horse_jockey_display', 'track', 'track_abbrev']).head())
    
    print("\\n🎯 EXPRESSION POWER:")
    print("- Window functions: .over() for grouped operations")
    print("- Conditionals: .when().then().otherwise()")
    print("- String ops: .str namespace with regex support") 
    print("- All expressions are lazy and optimized!")


def tutorial_5_memory_efficiency():
    """Tutorial 5: Memory efficiency comparison."""
    
    print("\\n" + "="*50)
    print("📚 TUTORIAL 5: Memory Efficiency")
    print("="*50)
    
    # Test memory usage
    size = 100000
    df_pl, df_pd = create_sample_racing_data(size)
    
    print(f"\\n💾 Memory Usage Comparison ({size:,} records):")
    print("-" * 45)
    
    # Polars memory usage
    pl_memory = df_pl.estimated_size("mb")
    print(f"Polars DataFrame: {pl_memory:.1f} MB")
    
    # Pandas memory usage
    pd_memory = df_pd.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"Pandas DataFrame: {pd_memory:.1f} MB")
    
    memory_efficiency = pd_memory / pl_memory
    print(f"Memory efficiency: Polars uses {memory_efficiency:.1f}x less memory!")
    
    print("\\n🔍 Why Polars is More Memory Efficient:")
    print("- Columnar storage (better compression)")
    print("- No index overhead (like Pandas)")
    print("- Efficient string handling")
    print("- Lazy evaluation reduces intermediate results")
    print("- Arrow format optimizations")


def main():
    """Run all tutorials."""
    
    try:
        tutorial_1_basic_operations()
        tutorial_2_lazy_evaluation()
        tutorial_3_performance_comparison()
        tutorial_4_expressions_showcase()
        tutorial_5_memory_efficiency()
        
        print("\\n" + "="*60)
        print("🎉 CONGRATULATIONS!")
        print("You've completed Polars Tutorial #1")
        print("="*60)
        
        print("\\n🎯 NEXT STEPS:")
        print("1. Run tutorial 02_web_scraping_ethics.py")
        print("2. Implement real TAB.nz scraping")
        print("3. Apply Polars to your racing data")
        print("4. Benchmark with your own datasets")
        
        print("\\n📚 ADDITIONAL LEARNING:")
        print("- Polars User Guide: https://pola-rs.github.io/polars/")
        print("- Performance tips: Focus on lazy evaluation")
        print("- Expressions: Learn the .when().then() patterns")
        print("- Window functions: Master .over() for analytics")
        
    except KeyboardInterrupt:
        print("\\n\\n⏹️ Tutorial interrupted by user")
    except Exception as e:
        print(f"\\n\\n❌ Tutorial failed: {e}")
        print("Check your Python environment and try again")


if __name__ == "__main__":
    main()