import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df_menu, *args, **kwargs):
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
    df_menu = df_menu.drop_duplicates(ignore_index=True)

    # Remove missing values
    df_menu = df_menu.dropna()
    df_menu.reset_index(drop=True, inplace=True)

    # Strip column names
    df_menu.columns = df_menu.columns.str.strip()

    # Remove space and replace it with '_'
    df_menu.columns = df_menu.columns.str.replace(' ','_')

    # Remove '_' character on 'name' column
    df_menu = df_menu.rename(columns = {'_name' : 'name'})

    # Lower column names
    df_menu.columns = df_menu.columns.str.lower()

    # Change effective_date format
    df_menu['effective_date'] = pd.to_datetime(df_menu['effective_date']).dt.strftime('%Y-%m-%d')
    # df_menu['effective_date'] = pd.to_datetime(df_menu['effective_date'], format='%Y-%m-%d')

    return df_menu


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
