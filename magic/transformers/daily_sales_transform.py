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
    query = """
    CREATE OR REPLACE TABLE `project_id.daily_sales_dm.daily_sales` AS
WITH order_with_menu AS (
    SELECT 
        o.order_id,
        o.menu_id,
        m.price,
        m.cogs,
        o.quantity,
        o.sales_date,
        (o.quantity * m.price) AS daily_sales,
        (o.quantity * m.cogs) AS daily_cogs,
        (o.quantity * (m.price - m.cogs)) AS initial_profit
    FROM `project_id.stagging_area.order_clean` o
    JOIN `project_id.stagging_area.menu_clean` m 
    ON o.menu_id = m.menu_id 
    AND o.sales_date >= m.effective_date
),
order_with_promotion AS (
    SELECT
        owm.*,
        p.disc_value,
        p.max_disc,
        LEAST(owm.daily_sales * p.disc_value, p.max_disc) AS discount_value
    FROM order_with_menu owm
    LEFT JOIN `project_id.stagging_area.promotion_clean` p
    ON owm.sales_date BETWEEN p.start_date AND p.end_date
),
daily_aggregated AS (
    SELECT 
        sales_date,
        menu_id,
        SUM(quantity) AS daily_quantity,
        SUM(daily_sales) AS daily_sales,
        SUM(daily_cogs) AS daily_cogs,
        SUM(COALESCE(discount_value, 0)) AS total_discount,
        SUM(initial_profit) - SUM(COALESCE(discount_value, 0)) AS daily_profit
    FROM order_with_promotion
    GROUP BY sales_date, menu_id
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY sales_date ASC, menu_id ASC) AS sales_id,
    sales_date,
    menu_id,
    daily_quantity,
    daily_sales,
    daily_cogs,
    total_discount AS discount_value,
    daily_profit
FROM daily_aggregated
ORDER BY sales_id ASC;

"""

    loader = BigQuery.with_config(ConfigFileLoader(config_path, config_profile))

    print('Executing query...')
    loader.execute(query)
    print('Query executed successfully.')

    # Tambahkan delay 5 detik
    time.sleep(30)
    
    # Sample data dari tabel yang sudah dibuat
    sample_table = 'daily_sales'
    sample_schema = 'daily_sales_dm'
    return loader.sample(sample_schema, sample_table)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


