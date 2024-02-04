if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    filtered_df = data[(data['trip_distance'] > 0.0) & (data['passenger_count'] > 0)]
    filtered_df['lpep_pickup_date'] = filtered_df['lpep_pickup_datetime'].dt.date 
    filtered_df['lpep_dropoff_date'] = filtered_df['lpep_dropoff_datetime'].dt.date
    filtered_df.columns = (filtered_df.columns.str.replace(' ', '_').str.lower())
    
    print(f"Preprocessing rows with zero passenger count and trip distance : {len(data) - len(filtered_df)}")
    print(f"Final List of Dataframe : {len(filtered_df)}")
    return filtered_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


@test
def test_vendorid(output, *args) -> None:
    assert 'vendorid' in output.columns, 'The output has not column venderid'


@test
def test_passenger_count(output, *args) -> None:
    assert output['passenger_count'].isin([0]).sum() == 0, 'The output has records with passenger_count = 0'


@test
def test_trip_distance(output, *args) -> None:
    assert output['trip_distance'].isin([0.0]).sum() == 0, 'The output has records with trip_distance = 0.0'

    