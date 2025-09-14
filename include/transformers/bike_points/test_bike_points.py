from pytest import approx

from include.transformers.bike_points.bike_points import clean_bike_points


def test_clean():
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

    cleaned_df = clean_bike_points(raw)

    assert cleaned_df.iloc[0]['id'] == 1
    assert cleaned_df.iloc[0]['commonName'] == 'River Street , Clerkenwell'
    assert cleaned_df.iloc[0]['NbStandardBikes'] == 1
    assert cleaned_df.iloc[0]['NbEBikes'] == 0
    assert cleaned_df.iloc[0]['NbEmptyDocks'] == 15
    assert cleaned_df.iloc[0]['lat'] == approx(51.529163)
    assert cleaned_df.iloc[0]['lon'] == approx(-0.10997)

    assert cleaned_df.iloc[1]['id'] == 2
    assert cleaned_df.iloc[1]['commonName'] == 'Phillimore Gardens, Kensington'
    assert cleaned_df.iloc[1]['NbStandardBikes'] == 1
    assert cleaned_df.iloc[1]['NbEBikes'] == 4
    assert cleaned_df.iloc[1]['NbEmptyDocks'] == 31
    assert cleaned_df.iloc[1]['lat'] == approx(51.499606)
    assert cleaned_df.iloc[1]['lon'] == approx(-0.197574)


