import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data(*args, **kwargs):
    sheet_name = 'Promotion'
    df_promotion = pd.read_excel('your_spreadsheet_name', 
                                  sheet_name, engine='openpyxl')
    return df_promotion

# @test
# def test_output(output, *args) -> None:
#     assert output is not None, 'The output is undefined'
#     assert isinstance(output, dict), 'The output should be a dictionary of DataFrames'
