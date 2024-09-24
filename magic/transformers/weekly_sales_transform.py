import time
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.bigquery import BigQuery
from os import path
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_in_bigquery(*args, **kwargs) -> DataFrame:
    """
    Performs a transformation in BigQuery
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Specify your SQL transformation query
    query = '''
CREATE OR REPLACE TABLE `project_id.weekly_sales_dm.weekly_sales` AS
WITH order_with_menu AS (
    SELECT 
        o.order_id,
        o.menu_id,
        m.price,
        m.cogs,
        o.quantity,
        o.sales_date,
        DATE_TRUNC(o.sales_date, WEEK(MONDAY)) AS week_start,
        (o.quantity * m.price) AS daily_sales,
        (o.quantity * m.cogs) AS daily_cogs,
        (o.quantity * (m.price - m.cogs)) AS daily_profit
    FROM `project_id.stagging_area.order_clean` o
    JOIN `project_id.stagging_area.menu_clean` m 
    ON o.menu_id = m.menu_id 
    AND o.sales_date >= m.effective_date
),
order_with_promotion AS (
    SELECT
        owm.*,
        p.id AS promotion_id,
        p.disc_value,
        p.max_disc
    FROM order_with_menu owm
    LEFT JOIN `project_id.stagging_area.promotion_clean` p
    ON owm.sales_date BETWEEN p.start_date AND p.end_date
),
weekly_aggregated AS (
    SELECT 
        week_start,
        menu_id,
        MAX(promotion_id) AS promotion_id,
        SUM(quantity) AS weekly_quantity,
        SUM(daily_sales) AS weekly_sales,
        SUM(daily_cogs) AS weekly_cogs,
        SUM(daily_profit) AS weekly_profit_before_discount
    FROM order_with_promotion
    GROUP BY week_start, menu_id
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY week_start ASC, menu_id ASC) AS weekly_sales_id,
    EXTRACT(WEEK FROM week_start) AS week,
    menu_id,
    COALESCE(promotion_id, 0) AS promotion_id,
    weekly_quantity,
    weekly_sales,
    weekly_cogs,
    LEAST(weekly_sales * MAX(p.disc_value), MAX(p.max_disc)) AS weekly_discount,
    weekly_profit_before_discount - LEAST(weekly_sales * MAX(p.disc_value), MAX(p.max_disc)) AS weekly_profit
FROM weekly_aggregated
LEFT JOIN `project_id.stagging_area.promotion_clean` p
ON weekly_aggregated.week_start BETWEEN p.start_date AND p.end_date
GROUP BY week_start, menu_id, promotion_id, weekly_quantity, weekly_sales, weekly_cogs, weekly_profit_before_discount
ORDER BY weekly_sales_id ASC;

    '''


    loader = BigQuery.with_config(ConfigFileLoader(config_path, config_profile))

    print('Executing query...')
    loader.execute(query)
    print('Query executed successfully.')

    # Tambahkan delay 5 detik
    time.sleep(30)
    
    # Sample data dari tabel yang sudah dibuat
    sample_table = 'weekly_sales'
    sample_schema = 'weekly_sales_dm'
    return loader.sample(sample_schema, sample_table)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
