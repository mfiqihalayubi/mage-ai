import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df_order, *args, **kwargs):
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
    # Specify your transformation logic here
    df_period_dim = pd.DataFrame(columns = ['sales_date', 'week', 'month'])

    # Convert 'sales_date' to datetime format
    df_period_dim['sales_date'] = pd.to_datetime(df_order['sales_date'])

    # Extract week and month from 'sales_date'
    df_period_dim['week'] = df_period_dim['sales_date'].dt.strftime('%W')
    df_period_dim['month'] = df_period_dim['sales_date'].dt.month

    # Set standard date
    df_period_dim['sales_date'] = pd.to_datetime(df_period_dim['sales_date']).dt.strftime('%Y-%m-%d')

    # Drop duplicate
    df_period_dim.drop_duplicates(inplace=True)

    return df_period_dim


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
