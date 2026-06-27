"""
Gold seasonal aggregate fact table.

Primary purpose: LSTM training feature set. Aggregates all gameweek rows
per player per season into a single row of seasonal totals and averages.
Covers all seasons in the dataset (2016-17 → current).
"""
from pyspark import pipelines as dp


@dp.materialized_view(
    name="fpl.gold.fact_season_totals",
    comment=(
        "Seasonal aggregates per player — primary LSTM training feature table. "
        "Covers all seasons from 2016-17 to current."
    ),
    cluster_by_auto=True,
)
def fact_season_totals():
    return spark.sql("""
        SELECT
            player_name,
            season,
            position,
            team_name,
            COUNT(*)                    AS games_played,
            SUM(minutes)                AS total_minutes,
            SUM(total_points)           AS total_points,
            ROUND(AVG(total_points), 3) AS avg_points_per_game,
            SUM(goals_scored)           AS goals,
            SUM(assists)                AS assists,
            SUM(clean_sheets)           AS clean_sheets,
            SUM(saves)                  AS saves,
            SUM(bonus)                  AS bonus,
            SUM(bps)                    AS bps,
            ROUND(SUM(xG), 3)           AS xG_total,
            ROUND(SUM(xA), 3)           AS xA_total,
            ROUND(SUM(xGI), 3)          AS xGI_total,
            ROUND(SUM(xGC), 3)          AS xGC_total,
            ROUND(AVG(influence), 3)    AS avg_influence,
            ROUND(AVG(creativity), 3)   AS avg_creativity,
            ROUND(AVG(threat), 3)       AS avg_threat,
            ROUND(AVG(ict_index), 3)    AS avg_ict_index,
            ROUND(AVG(price_m), 3)      AS avg_price_m,
            MAX(price_m)                AS end_price_m,
            SUM(yellow_cards)           AS yellow_cards,
            SUM(red_cards)              AS red_cards,
            data_source
        FROM fpl.gold.fact_gameweek_performance
        GROUP BY player_name, season, position, team_name, data_source
    """)
