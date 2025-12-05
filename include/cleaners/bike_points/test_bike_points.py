import pandas as pd
from pytest import approx, mark

from include.cleaners.bike_points.bike_points import clean_bike_points

batch_id = 'TEST-BATCH-ID'

raw = """
    [
      {
        "$type": "Tfl.Api.Presentation.Entities.Place, Tfl.Api.Presentation.Entities",
        "id": "BikePoints_1",
        "url": "/Place/BikePoints_1",
        "commonName": "River Street , Clerkenwell",
        "placeType": "BikePoint",
        "additionalProperties": [
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "TerminalName",
            "sourceSystemKey": "BikePoints",
            "value": "001023",
            "modified": "2025-09-12T14:36:21.533Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "Installed",
            "sourceSystemKey": "BikePoints",
            "value": "true",
            "modified": "2025-09-12T14:36:21.533Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "Locked",
            "sourceSystemKey": "BikePoints",
            "value": "false",
            "modified": "2025-09-12T14:36:21.533Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "InstallDate",
            "sourceSystemKey": "BikePoints",
            "value": "1278947280000",
            "modified": "2025-09-12T14:36:21.533Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "RemovalDate",
            "sourceSystemKey": "BikePoints",
            "value": "",
            "modified": "2025-09-12T14:36:21.533Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "Temporary",
            "sourceSystemKey": "BikePoints",
            "value": "false",
            "modified": "2025-09-12T14:36:21.533Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "NbBikes",
            "sourceSystemKey": "BikePoints",
            "value": "1",
            "modified": "2025-09-12T14:36:21.533Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "NbEmptyDocks",
            "sourceSystemKey": "BikePoints",
            "value": "15",
            "modified": "2025-09-12T14:36:21.533Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "NbDocks",
            "sourceSystemKey": "BikePoints",
            "value": "19",
            "modified": "2025-09-12T14:36:21.533Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "NbStandardBikes",
            "sourceSystemKey": "BikePoints",
            "value": "1",
            "modified": "2025-09-12T14:36:21.533Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "NbEBikes",
            "sourceSystemKey": "BikePoints",
            "value": "0",
            "modified": "2025-09-12T14:36:21.533Z"
          }
        ],
        "children": [],
        "childrenUrls": [],
        "lat": 51.529163,
        "lon": -0.10997
      },
      {
        "$type": "Tfl.Api.Presentation.Entities.Place, Tfl.Api.Presentation.Entities",
        "id": "BikePoints_2",
        "url": "/Place/BikePoints_2",
        "commonName": "Phillimore Gardens, Kensington",
        "placeType": "BikePoint",
        "additionalProperties": [
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "TerminalName",
            "sourceSystemKey": "BikePoints",
            "value": "001018",
            "modified": "2025-09-12T15:06:15.683Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "Installed",
            "sourceSystemKey": "BikePoints",
            "value": "true",
            "modified": "2025-09-12T15:06:15.683Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "Locked",
            "sourceSystemKey": "BikePoints",
            "value": "false",
            "modified": "2025-09-12T15:06:15.683Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "InstallDate",
            "sourceSystemKey": "BikePoints",
            "value": "1278585780000",
            "modified": "2025-09-12T15:06:15.683Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "RemovalDate",
            "sourceSystemKey": "BikePoints",
            "value": "",
            "modified": "2025-09-12T15:06:15.683Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "Temporary",
            "sourceSystemKey": "BikePoints",
            "value": "false",
            "modified": "2025-09-12T15:06:15.683Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "NbBikes",
            "sourceSystemKey": "BikePoints",
            "value": "5",
            "modified": "2025-09-12T15:06:15.683Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "NbEmptyDocks",
            "sourceSystemKey": "BikePoints",
            "value": "31",
            "modified": "2025-09-12T15:06:15.683Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "NbDocks",
            "sourceSystemKey": "BikePoints",
            "value": "37",
            "modified": "2025-09-12T15:06:15.683Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "NbStandardBikes",
            "sourceSystemKey": "BikePoints",
            "value": "1",
            "modified": "2025-09-12T15:06:15.683Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "NbEBikes",
            "sourceSystemKey": "BikePoints",
            "value": "4",
            "modified": "2025-09-12T15:06:15.683Z"
          }
        ],
        "children": [],
        "childrenUrls": [],
        "lat": 51.499606,
        "lon": -0.197574
      }
    ]
"""

@mark.parametrize(
    'bike_point_id, expected',
    [
        (
            1,
            dict(
                common_name='River Street , Clerkenwell',
                lat=51.529163,
                lon=-0.10997,
                nb_standard_bikes=1,
                nb_e_bikes=0,
                nb_empty_docks=15,
                updated_at=pd.to_datetime('2025-09-12T14:36:21.533Z', format='ISO8601'),
            ),
        ),
        (
            2,
            dict(
                common_name='Phillimore Gardens, Kensington',
                lat=51.499606,
                lon=-0.197574,
                nb_standard_bikes=1,
                nb_e_bikes=4,
                nb_empty_docks=31,
                updated_at=pd.to_datetime('2025-09-12T15:06:15.683Z', format='ISO8601'),
            ),
        ),
    ],
)
def test_clean(bike_point_id, expected):
    cleaned_df = clean_bike_points(raw, batch_id)
    row = cleaned_df.loc[cleaned_df['bike_point_id'] == bike_point_id].iloc[0]

    assert row['common_name'] == expected['common_name']
    assert row['batch_id'] == 'TEST-BATCH-ID'
    assert row['lat'] == approx(expected['lat'])
    assert row['lon'] == approx(expected['lon'])
    # from additional_props
    assert row['nb_standard_bikes'] == expected['nb_standard_bikes']
    assert row['nb_e_bikes'] == expected['nb_e_bikes']
    assert row['nb_empty_docks'] == expected['nb_empty_docks']
    assert row['updated_at'] == expected['updated_at']
