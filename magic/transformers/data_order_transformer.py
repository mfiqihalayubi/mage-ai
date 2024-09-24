import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def cleaning(df_order, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Remove duplicated rows
    df_order = df_order.drop_duplicates(ignore_index=True)

    # Remove missing values
    df_order = df_order.dropna()
    df_order.reset_index(drop=True, inplace=True)

    # Remove space and replace it with '_'
    df_order.columns = df_order.columns.str.replace(' ','_')

    # Strip column names
    df_order.columns = df_order.columns.str.strip()

    # Lower column names
    df_order.columns = df_order.columns.str.lower()

    # Change sales_date format
    df_order['sales_date'] = pd.to_datetime(df_order['sales_date']).dt.strftime('%Y-%m-%d')
    # df_order['sales_date'] = pd.to_datetime(df_order['sales_date'].astype(str), format='%Y%m%d')
    # df_order['sales_date'] = pd.to_datetime(df_order['sales_date'], format='%Y-%m-%d')

    # Sum quantity for every similar order_id, menu_id, and sales_date
    df_order = df_order.groupby(['order_id', 'menu_id', 'sales_date'], as_index=False).agg({'quantity':'sum'})

    df_order = df_order.sort_values(by=['order_id', 'menu_id', 'sales_date'])

    return df_order


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
