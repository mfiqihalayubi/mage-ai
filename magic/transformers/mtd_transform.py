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
CREATE OR REPLACE TABLE project_id.mtd_dm.mtd_sales AS
WITH order_with_menu AS (
    SELECT 
        o.order_id,
        o.menu_id,
        m.price,
        m.cogs,
        o.quantity,
        o.sales_date,
        EXTRACT(YEAR FROM DATE(o.sales_date)) AS year,
        EXTRACT(MONTH FROM DATE(o.sales_date)) AS month,
        (o.quantity * m.price) AS daily_sales,
        (o.quantity * m.cogs) AS daily_cogs,
        (o.quantity * (m.price - m.cogs)) AS daily_profit
    FROM project_id.stagging_area.order_clean o
    JOIN project_id.stagging_area.menu_clean m
    ON o.menu_id = m.menu_id
    AND o.sales_date >= m.effective_date
),
order_with_promotion AS (
    SELECT
        owm.*,
        pr.id AS promotion_id,
        pr.disc_value,
        pr.max_disc,
        CASE 
            WHEN owm.daily_sales * pr.disc_value <= pr.max_disc THEN owm.daily_sales * pr.disc_value
            ELSE pr.max_disc
        END AS discount_value
    FROM order_with_menu owm
    LEFT JOIN project_id.stagging_area.promotion_clean pr
    ON owm.sales_date BETWEEN pr.start_date AND pr.end_date
),
daily_aggregated AS (
    SELECT 
        year,
        month,
        sales_date,
        menu_id,
        COALESCE(MAX(promotion_id), 0) AS promotion_id,
        SUM(quantity) AS daily_quantity,
        SUM(daily_sales) AS daily_sales,
        SUM(daily_cogs) AS daily_cogs,
        SUM(COALESCE(discount_value, 0)) AS daily_discount,
        SUM(daily_profit - COALESCE(discount_value, 0)) AS daily_profit
    FROM order_with_promotion
    GROUP BY year, month, sales_date, menu_id
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY year, month, sales_date, menu_id) AS month_to_date_sales_id,
    year,
    month,
    sales_date,
    menu_id,
    promotion_id,
    SUM(daily_quantity) OVER (PARTITION BY year, month, menu_id ORDER BY sales_date) AS month_to_date_quantity,
    SUM(daily_sales) OVER (PARTITION BY year, month, menu_id ORDER BY sales_date) AS month_to_date_sales,
    SUM(daily_cogs) OVER (PARTITION BY year, month, menu_id ORDER BY sales_date) AS month_to_date_cogs,
    SUM(daily_discount) OVER (PARTITION BY year, month, menu_id ORDER BY sales_date) AS month_to_date_discount,
    SUM(daily_profit) OVER (PARTITION BY year, month, menu_id ORDER BY sales_date) AS month_to_date_profit
FROM daily_aggregated
ORDER BY month_to_date_sales_id;

    '''


    loader = BigQuery.with_config(ConfigFileLoader(config_path, config_profile))

    print('Executing query...')
    loader.execute(query)
    print('Query executed successfully.')

    # Tambahkan delay 5 detik
    time.sleep(30)
    
    # Sample data dari tabel yang sudah dibuat
    sample_table = 'mtd_sales'
    sample_schema = 'mtd_dm'
    return loader.sample(sample_schema, sample_table)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
