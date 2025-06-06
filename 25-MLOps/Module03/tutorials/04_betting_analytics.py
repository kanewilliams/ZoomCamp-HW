"""
Betting Analytics Foundation Tutorial #4

⚠️  IMPORTANT DISCLAIMER ⚠️
This tutorial is for EDUCATIONAL PURPOSES ONLY.
- Learn data analysis techniques
- Understand statistical concepts
- Practice feature engineering
- NO actual betting recommendations
- Gambling can be addictive - seek help if needed

LEARNING MISSION: Understand betting analytics concepts through data science

This tutorial covers:
1. Statistical analysis of racing data
2. Probability and odds calculations
3. Feature engineering for prediction
4. Performance evaluation metrics
5. Risk management concepts

🚨 BIG TODO FOR STUDENT:
This is currently a foundation - you'll need to implement the actual analytics
based on your understanding of racing domain knowledge and statistical methods.
"""

import polars as pl
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, date
import random
import math

print("🏇 Betting Analytics Tutorial #4: Statistical Foundation")
print("=" * 60)
print("⚠️  EDUCATIONAL ONLY - NOT FOR ACTUAL BETTING")
print("=" * 60)


def create_sample_historical_data(days: int = 90, races_per_day: int = 40) -> pl.DataFrame:
    """
    Create sample historical racing data for analysis.
    
    🎯 LEARNING: This generates realistic racing data patterns
    """
    print(f"📊 Generating {days} days of historical racing data...")
    
    np.random.seed(42)  # Reproducible results
    
    data = []
    
    for day in range(days):
        race_date = date(2023, 10, 1) + timedelta(days=day)
        
        for race_num in range(1, races_per_day + 1):
            # Random number of runners per race (6-16 typical)
            num_runners = np.random.randint(6, 17)
            
            for position in range(1, num_runners + 1):
                # Generate odds based on finishing position (winners have lower odds)
                if position == 1:  # Winner
                    odds = np.random.uniform(1.5, 15.0)
                elif position <= 3:  # Placed
                    odds = np.random.uniform(2.0, 25.0)
                else:  # Unplaced
                    odds = np.random.uniform(3.0, 100.0)
                
                # Create horse data
                horse_id = f"H{day * races_per_day + race_num}_{position}"
                jockey_id = f"J{np.random.randint(1, 51)}"  # 50 jockeys
                trainer_id = f"T{np.random.randint(1, 101)}"  # 100 trainers
                
                data.append({
                    'race_date': race_date,
                    'track': np.random.choice(['Ellerslie', 'Trentham', 'Riccarton', 'Te Rapa', 'Awapuni']),
                    'race_number': race_num,
                    'horse_id': horse_id,
                    'horse_name': f'Horse_{horse_id}',
                    'jockey_id': jockey_id,
                    'trainer_id': trainer_id,
                    'barrier': position,  # Simplified
                    'weight': np.random.uniform(52.0, 62.0),
                    'starting_odds': odds,
                    'finish_position': position,
                    'field_size': num_runners,
                    'prize_money': np.random.randint(5000, 50000) if position <= 4 else 0,
                    'track_condition': np.random.choice(['Good', 'Slow', 'Heavy'], p=[0.6, 0.3, 0.1]),
                    'distance': np.random.choice([1200, 1400, 1600, 2000, 2400]),
                    'race_class': np.random.choice(['R65', 'R75', 'R85', 'Listed', 'Group3'], p=[0.4, 0.3, 0.2, 0.07, 0.03])
                })
    
    df = pl.DataFrame(data)
    print(f"✅ Generated {len(df):,} race results")
    return df


# =============================================================================
# TUTORIAL 1: Basic Statistical Analysis
# =============================================================================

def tutorial_1_basic_statistics(df: pl.DataFrame):
    """
    🎯 LEARNING: Basic statistical analysis of racing data
    
    TODO FOR STUDENT: Expand this with more sophisticated analysis
    """
    print("\\n" + "="*50)
    print("📚 TUTORIAL 1: Basic Statistical Analysis")
    print("="*50)
    
    print("\\n1️⃣ Win Rate Analysis:")
    print("-" * 25)
    
    # Jockey win rates
    jockey_stats = df.group_by('jockey_id').agg([
        pl.len().alias('total_rides'),
        (pl.col('finish_position') == 1).sum().alias('wins'),
        (pl.col('finish_position') <= 3).sum().alias('places'),
        pl.col('starting_odds').mean().alias('avg_odds')
    ]).with_columns([
        (pl.col('wins') / pl.col('total_rides') * 100).alias('win_rate_pct'),
        (pl.col('places') / pl.col('total_rides') * 100).alias('place_rate_pct')
    ]).filter(pl.col('total_rides') >= 20).sort('win_rate_pct', descending=True)
    
    print("Top jockeys by win rate (min 20 rides):")
    print(jockey_stats.head())
    
    print("\\n2️⃣ Track Bias Analysis:")
    print("-" * 25)
    
    # Barrier performance analysis
    barrier_stats = df.group_by('barrier').agg([
        pl.len().alias('runs'),
        (pl.col('finish_position') == 1).sum().alias('wins'),
        pl.col('finish_position').mean().alias('avg_finish')
    ]).with_columns([
        (pl.col('wins') / pl.col('runs') * 100).alias('win_rate_pct')
    ]).sort('barrier')
    
    print("Barrier performance analysis:")
    print(barrier_stats)
    
    print("\\n3️⃣ Odds Accuracy Analysis:")
    print("-" * 30)
    
    # TODO FOR STUDENT: Implement odds accuracy analysis
    # This would compare implied probability vs actual win rate
    odds_analysis = df.with_columns([
        (1.0 / pl.col('starting_odds')).alias('implied_probability'),
        (pl.col('finish_position') == 1).cast(pl.Int32).alias('won')
    ])
    
    # Group by odds ranges and calculate actual vs implied win rates
    odds_brackets = odds_analysis.with_columns([
        pl.when(pl.col('starting_odds') < 2.0).then(pl.lit('Odds < 2.0'))
        .when(pl.col('starting_odds') < 5.0).then(pl.lit('Odds 2.0-5.0'))
        .when(pl.col('starting_odds') < 10.0).then(pl.lit('Odds 5.0-10.0'))
        .when(pl.col('starting_odds') < 20.0).then(pl.lit('Odds 10.0-20.0'))
        .otherwise(pl.lit('Odds 20.0+'))
        .alias('odds_bracket')
    ]).group_by('odds_bracket').agg([
        pl.len().alias('total_runners'),
        pl.col('won').sum().alias('winners'),
        pl.col('implied_probability').mean().alias('avg_implied_prob')
    ]).with_columns([
        (pl.col('winners') / pl.col('total_runners')).alias('actual_win_rate')
    ])
    
    print("Odds accuracy by bracket:")
    print(odds_brackets)


# =============================================================================
# TUTORIAL 2: Feature Engineering for Prediction
# =============================================================================

def tutorial_2_feature_engineering(df: pl.DataFrame) -> pl.DataFrame:
    """
    🎯 LEARNING: Create features for predictive modeling
    
    TODO FOR STUDENT: Add more sophisticated features based on racing domain knowledge
    """
    print("\\n" + "="*50)
    print("📚 TUTORIAL 2: Feature Engineering")
    print("="*50)
    
    print("Creating predictive features...")
    
    featured_df = df.sort(['race_date', 'race_number']).with_columns([
        # Historical performance features
        pl.col('finish_position').mean().over('jockey_id').alias('jockey_avg_finish'),
        pl.col('finish_position').mean().over('trainer_id').alias('trainer_avg_finish'),
        pl.col('finish_position').mean().over('horse_id').alias('horse_avg_finish'),
        
        # Win rate features
        (pl.col('finish_position') == 1).mean().over('jockey_id').alias('jockey_win_rate'),
        (pl.col('finish_position') == 1).mean().over('trainer_id').alias('trainer_win_rate'),
        
        # Track-specific performance
        pl.col('finish_position').mean().over(['jockey_id', 'track']).alias('jockey_track_avg'),
        pl.col('finish_position').mean().over(['trainer_id', 'track']).alias('trainer_track_avg'),
        
        # Distance performance
        pl.col('finish_position').mean().over(['jockey_id', 'distance']).alias('jockey_distance_avg'),
        
        # Recent form (TODO: implement proper rolling windows)
        pl.col('finish_position').mean().over('horse_id').alias('horse_recent_form'),  # Simplified
        
        # Market features
        pl.col('starting_odds').rank().over(['race_date', 'race_number']).alias('market_rank'),
        (1.0 / pl.col('starting_odds')).alias('implied_probability'),
        
        # Field features
        pl.col('field_size').alias('competition_level'),
        pl.col('weight').rank().over(['race_date', 'race_number']).alias('weight_rank'),
        
        # Track condition adjustments
        pl.when(pl.col('track_condition') == 'Heavy').then(1).otherwise(0).alias('heavy_track'),
        pl.when(pl.col('track_condition') == 'Good').then(1).otherwise(0).alias('good_track'),
        
        # Class/quality indicators
        pl.when(pl.col('race_class').is_in(['Listed', 'Group3'])).then(1).otherwise(0).alias('quality_race')
    ])
    
    print(f"✅ Created features for {len(featured_df)} records")
    print(f"Total features: {len(featured_df.columns)}")
    
    # Show sample of features
    print("\\nSample features:")
    feature_cols = ['horse_name', 'jockey_win_rate', 'trainer_win_rate', 'market_rank', 'implied_probability']
    print(featured_df.select(feature_cols).head())
    
    return featured_df


# =============================================================================
# TUTORIAL 3: Performance Evaluation Metrics
# =============================================================================

def tutorial_3_evaluation_metrics(df: pl.DataFrame):
    """
    🎯 LEARNING: Evaluate betting strategy performance
    
    TODO FOR STUDENT: Implement sophisticated evaluation metrics
    """
    print("\\n" + "="*50)
    print("📚 TUTORIAL 3: Performance Evaluation")
    print("="*50)
    
    print("\\n🎯 KEY METRICS FOR BETTING ANALYSIS:")
    print("-" * 40)
    
    # Simple strategy: bet on horses with odds between 2-5
    strategy_df = df.with_columns([
        pl.when((pl.col('starting_odds') >= 2.0) & (pl.col('starting_odds') <= 5.0))
        .then(1).otherwise(0).alias('strategy_bet'),
        
        # Profit calculation
        pl.when(pl.col('finish_position') == 1)
        .then(pl.col('starting_odds') - 1)  # Profit if win
        .otherwise(-1)  # Loss if lose
        .alias('bet_result')
    ])
    
    # Calculate strategy performance
    strategy_bets = strategy_df.filter(pl.col('strategy_bet') == 1)
    
    if len(strategy_bets) > 0:
        total_bets = len(strategy_bets)
        total_wins = strategy_bets.filter(pl.col('finish_position') == 1).height
        total_profit = strategy_bets.select(pl.col('bet_result').sum()).item()
        win_rate = (total_wins / total_bets) * 100
        roi = (total_profit / total_bets) * 100  # Assuming $1 bets
        
        print(f"📊 STRATEGY PERFORMANCE (Odds 2.0-5.0):")
        print(f"  Total bets: {total_bets:,}")
        print(f"  Winners: {total_wins:,}")
        print(f"  Win rate: {win_rate:.1f}%")
        print(f"  Total profit: ${total_profit:.2f}")
        print(f"  ROI: {roi:.1f}%")
        
        # Break down by track
        track_performance = strategy_bets.group_by('track').agg([
            pl.len().alias('bets'),
            (pl.col('finish_position') == 1).sum().alias('wins'),
            pl.col('bet_result').sum().alias('profit')
        ]).with_columns([
            (pl.col('wins') / pl.col('bets') * 100).alias('win_rate_pct'),
            (pl.col('profit') / pl.col('bets') * 100).alias('roi_pct')
        ])
        
        print("\\n📍 Performance by track:")
        print(track_performance)
    
    print("\\n🎯 TODO FOR STUDENT:")
    print("1. Implement Kelly Criterion for bet sizing")
    print("2. Add Sharpe ratio calculation")
    print("3. Calculate maximum drawdown")
    print("4. Implement confidence intervals")
    print("5. Add statistical significance testing")


# =============================================================================
# TUTORIAL 4: Risk Management Concepts
# =============================================================================

def tutorial_4_risk_management():
    """
    🎯 LEARNING: Risk management concepts for betting
    
    TODO FOR STUDENT: Study these concepts but NEVER implement for real money!
    """
    print("\\n" + "="*50)
    print("📚 TUTORIAL 4: Risk Management Concepts")
    print("="*50)
    
    print("\\n💡 THEORETICAL CONCEPTS (EDUCATION ONLY):")
    print("-" * 45)
    
    print("\\n1️⃣ Kelly Criterion:")
    print("   Formula: f = (bp - q) / b")
    print("   Where:")
    print("   - f = fraction of bankroll to bet")
    print("   - b = odds received (decimal odds - 1)")
    print("   - p = probability of winning")
    print("   - q = probability of losing (1 - p)")
    
    # Example calculation (theoretical only)
    def kelly_criterion(odds: float, win_probability: float) -> float:
        \"\"\"Calculate Kelly Criterion bet size.\"\"\"
        b = odds - 1  # Net odds
        p = win_probability
        q = 1 - p
        
        kelly_fraction = (b * p - q) / b
        return max(0, kelly_fraction)  # Never bet negative
    
    # Example
    example_odds = 3.0
    example_prob = 0.4
    kelly_size = kelly_criterion(example_odds, example_prob)
    
    print(f"\\n   Example: Odds {example_odds}, Win Prob {example_prob:.1%}")
    print(f"   Kelly suggests: {kelly_size:.1%} of bankroll")
    
    print("\\n2️⃣ Bankroll Management:")
    print("   - Never bet more than you can afford to lose")
    print("   - Set strict loss limits")
    print("   - Use fractional betting (1-5% of bankroll)")
    print("   - Track all bets and results")
    
    print("\\n3️⃣ Value Betting Concept:")
    print("   - Only bet when odds > true probability")
    print("   - Expected Value = (Win Prob × Odds) - 1")
    print("   - Positive EV = good bet (theoretically)")
    
    # Example EV calculation
    def expected_value(odds: float, win_probability: float) -> float:
        \"\"\"Calculate expected value of a bet.\"\"\"
        return (win_probability * odds) - 1
    
    ev_example = expected_value(3.0, 0.4)
    print(f"\\n   Example EV: {ev_example:.3f} (20% expected profit)")
    
    print("\\n4️⃣ Variance and Risk:")
    print("   - High variance = big swings in results")
    print("   - Bankroll needs to handle losing streaks")
    print("   - Standard deviation of returns matters")
    
    print("\\n⚠️  CRITICAL REMINDERS:")
    print("   - These are THEORETICAL concepts for learning")
    print("   - No betting system guarantees profit")
    print("   - Gambling can be addictive")
    print("   - House edge exists in all forms of gambling")
    print("   - Past performance ≠ future results")


# =============================================================================
# TUTORIAL 5: Advanced Analytics (TODO Framework)
# =============================================================================

def tutorial_5_advanced_analytics_todo():
    """
    🎯 LEARNING: Framework for advanced analytics
    
    🚨 BIG TODO FOR STUDENT: This is your roadmap for advanced implementation
    """
    print("\\n" + "="*50)
    print("📚 TUTORIAL 5: Advanced Analytics TODO")
    print("="*50)
    
    print("\\n🎯 ADVANCED ANALYTICS IMPLEMENTATION ROADMAP:")
    print("=" * 55)
    
    advanced_todos = {
        "📊 Machine Learning Models": [
            "[ ] Implement logistic regression for win prediction",
            "[ ] Try ensemble methods (Random Forest, XGBoost)",
            "[ ] Experiment with neural networks",
            "[ ] Add feature selection and engineering",
            "[ ] Implement cross-validation properly",
            "[ ] Handle class imbalance (winners are rare)",
        ],
        
        "📈 Time Series Analysis": [
            "[ ] Model horse form cycles",
            "[ ] Seasonal racing pattern analysis",
            "[ ] Track condition impact modeling",
            "[ ] Jockey/trainer hot streaks detection",
            "[ ] Market movement analysis",
        ],
        
        "🔍 Advanced Feature Engineering": [
            "[ ] Speed ratings and class adjustments",
            "[ ] Pace analysis (early, mid, late pace)",
            "[ ] Sectional times if available",
            "[ ] Breeding/pedigree factors",
            "[ ] Weight-for-age adjustments",
            "[ ] Track bias quantification",
        ],
        
        "💹 Market Analysis": [
            "[ ] Odds movement patterns",
            "[ ] Market efficiency testing",
            "[ ] Steam moves and smart money detection",
            "[ ] Overlay identification algorithms",
            "[ ] Arbitrage opportunity detection",
        ],
        
        "🎲 Statistical Methods": [
            "[ ] Bayesian updating for odds",
            "[ ] Monte Carlo simulations",
            "[ ] Confidence interval estimation",
            "[ ] A/B testing for strategies",
            "[ ] Bootstrap sampling for robustness",
        ],
        
        "📊 Performance Analytics": [
            "[ ] Sharpe ratio and risk-adjusted returns",
            "[ ] Maximum drawdown calculation",
            "[ ] Win rate vs. profitability analysis",
            "[ ] Strategy correlation analysis",
            "[ ] Benchmark comparison frameworks",
        ],
        
        "🛡️ Risk Management": [
            "[ ] Portfolio theory application",
            "[ ] Kelly Criterion implementation",
            "[ ] Stop-loss automation",
            "[ ] Bankroll simulation",
            "[ ] Stress testing strategies",
        ]
    }
    
    for category, tasks in advanced_todos.items():
        print(f"\\n{category}:")
        for task in tasks:
            print(f"  {task}")
    
    print("\\n" + "="*60)
    print("🎓 LEARNING RESOURCES FOR IMPLEMENTATION:")
    print("="*60)
    
    resources = {
        "📚 Books": [
            "The Intelligent Investor - Benjamin Graham",
            "A Man for All Markets - Edward Thorp", 
            "Beat the Dealer - Edward Thorp",
            "Trading and Exchanges - Larry Harris"
        ],
        
        "🔬 Academic Papers": [
            "Efficiency of racetrack betting markets",
            "Machine learning in sports betting",
            "The Kelly criterion in blackjack",
            "Behavioral biases in gambling markets"
        ],
        
        "💻 Technical Skills": [
            "Advanced Polars/Pandas operations",
            "Scikit-learn for ML implementation",
            "Backtesting frameworks",
            "Statistical testing in Python",
            "Time series analysis with statsmodels"
        ],
        
        "🏇 Domain Knowledge": [
            "Horse racing handicapping principles",
            "Track condition impacts",
            "Trainer and jockey patterns",
            "Breeding and pedigree analysis",
            "International racing differences"
        ]
    }
    
    for category, items in resources.items():
        print(f"\\n{category}:")
        for item in items:
            print(f"  • {item}")
    
    print("\\n" + "="*60)
    print("⚠️  FINAL ETHICAL REMINDER")
    print("="*60)
    print("🚨 This knowledge is for EDUCATION and RESEARCH only")
    print("🚨 Gambling can be seriously addictive")
    print("🚨 No strategy guarantees profits")
    print("🚨 Only bet what you can afford to lose")
    print("🚨 Seek help if gambling becomes a problem")
    print("\\n🆘 Help: https://www.gamblinghelponline.org.nz/")


# =============================================================================
# MAIN TUTORIAL RUNNER
# =============================================================================

def main():
    """Run all betting analytics tutorials."""
    
    print("🎯 Starting Betting Analytics Tutorial")
    print("⚠️  REMEMBER: This is for EDUCATION ONLY")
    print()
    
    try:
        # Generate sample data
        df = create_sample_historical_data(days=90, races_per_day=40)
        
        # Run tutorials
        tutorial_1_basic_statistics(df)
        
        featured_df = tutorial_2_feature_engineering(df)
        
        tutorial_3_evaluation_metrics(df)
        
        tutorial_4_risk_management()
        
        tutorial_5_advanced_analytics_todo()
        
        print("\\n" + "="*60)
        print("🎉 TUTORIAL COMPLETED!")
        print("="*60)
        
        print("\\n🎯 KEY LEARNINGS:")
        print("✅ Statistical analysis of racing data")
        print("✅ Feature engineering concepts")
        print("✅ Performance evaluation metrics") 
        print("✅ Risk management theory")
        print("✅ Advanced analytics roadmap")
        
        print("\\n🚀 NEXT STEPS (EDUCATIONAL):")
        print("1. Study the advanced TODO list")
        print("2. Research academic papers on market efficiency")
        print("3. Practice statistical analysis on real data")
        print("4. Learn about behavioral finance")
        print("5. Understand gambling addiction risks")
        
        print("\\n⚠️  REMEMBER:")
        print("This knowledge is for learning data science concepts")
        print("NOT for actual betting or gambling")
        print("Gambling can be addictive - get help if needed")
        
    except KeyboardInterrupt:
        print("\\n\\n⏹️ Tutorial interrupted by user")
    except Exception as e:
        print(f"\\n\\n❌ Tutorial failed: {e}")


if __name__ == "__main__":
    main()