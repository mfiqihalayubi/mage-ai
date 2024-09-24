import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df_promotion, *args, **kwargs):
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
    df_promotion = df_promotion.drop_duplicates(ignore_index=True)

    # Remove missing values
    df_promotion = df_promotion.dropna()
    df_promotion.reset_index(drop=True, inplace=True)

    # Strip column names
    df_promotion.columns = df_promotion.columns.str.strip()

    # Remove space and replace it with '_'
    df_promotion.columns = df_promotion.columns.str.replace(' ','_')

    # Lower column names
    df_promotion.columns = df_promotion.columns.str.lower()

    # Change start_date and end_date format
    df_promotion['start_date'] = pd.to_datetime(df_promotion['start_date']).dt.strftime('%Y-%m-%d')
    df_promotion['end_date'] = pd.to_datetime(df_promotion['end_date']).dt.strftime('%Y-%m-%d')
    # df_menu['effective_date'] = pd.to_datetime(df_menu['effective_date'], format='%Y-%m-%d')

    return df_promotion


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
