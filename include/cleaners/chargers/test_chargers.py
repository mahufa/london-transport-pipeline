import pandas as pd
from pytest import approx, mark

from include.cleaners.chargers.chargers import clean_chargers



raw = """
    [
      {
        "$type": "Tfl.Api.Presentation.Entities.Place, Tfl.Api.Presentation.Entities",
        "id": "EsbChargePoint_ChargePointESB-UT05X7-3",
        "url": "/Place/EsbChargePoint_ChargePointESB-UT05X7-3",
        "commonName": "21 Bridge Road Station UT05X7 Connector 3",
        "placeType": "ChargeConnector",
        "additionalProperties": [
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "Status",
            "sourceSystemKey": "EsbChargePoint",
            "value": "Unknown",
            "modified": "2025-09-15T10:04:36.16Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "ParentStation",
            "sourceSystemKey": "EsbChargePoint",
            "value": "ChargePointESB-UT05X7",
            "modified": "2025-09-15T10:04:36.16Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "ConnectorType",
            "sourceSystemKey": "EsbChargePoint",
            "value": "Type 2",
            "modified": "2025-09-15T10:04:36.16Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "ConnectorDescription",
            "sourceSystemKey": "EsbChargePoint",
            "value": "Type 2 Connector",
            "modified": "2025-09-15T10:04:36.16Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "ElectricalCharacteristics",
            "key": "Power",
            "sourceSystemKey": "EsbChargePoint",
            "value": "43kW",
            "modified": "2025-09-15T10:04:36.16Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "LastUpdated",
            "sourceSystemKey": "EsbChargePoint",
            "value": "Sep 15 2025 10:04AM",
            "modified": "2025-09-15T10:04:36.16Z"
          }
        ],
        "children": [],
        "childrenUrls": [],
        "lat": 51.564362,
        "lon": -0.278134
      },
      {
        "$type": "Tfl.Api.Presentation.Entities.Place, Tfl.Api.Presentation.Entities",
        "id": "ChargeMasterChargePoint_ChargePointCM-24122-61977",
        "url": "/Place/ChargeMasterChargePoint_ChargePointCM-24122-61977",
        "commonName": "16 Talgarth Road Station 24122 Connector 1",
        "placeType": "ChargeConnector",
        "additionalProperties": [
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "ParentStation",
            "sourceSystemKey": "ChargeMasterChargePoint",
            "value": "ChargePointCM-24122",
            "modified": "2025-09-15T10:04:36.52Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "Status",
            "sourceSystemKey": "ChargeMasterChargePoint",
            "value": "Unavailable",
            "modified": "2025-09-15T11:09:21.557Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "ConnectorType",
            "sourceSystemKey": "ChargeMasterChargePoint",
            "value": "CCS",
            "modified": "2025-09-15T10:04:36.52Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "ConnectorDescription",
            "sourceSystemKey": "ChargeMasterChargePoint",
            "value": "Combined Charging System",
            "modified": "2025-09-15T10:04:36.52Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "ElectricalCharacteristics",
            "key": "Volts",
            "sourceSystemKey": "ChargeMasterChargePoint",
            "value": "400",
            "modified": "2025-09-15T10:04:36.52Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "ElectricalCharacteristics",
            "key": "Amps",
            "sourceSystemKey": "ChargeMasterChargePoint",
            "value": "125",
            "modified": "2025-09-15T10:04:36.52Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "ElectricalCharacteristics",
            "key": "Power",
            "sourceSystemKey": "ChargeMasterChargePoint",
            "value": "50kW",
            "modified": "2025-09-15T10:04:36.52Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "ElectricalCharacteristics",
            "key": "Phase",
            "sourceSystemKey": "ChargeMasterChargePoint",
            "value": "DC",
            "modified": "2025-09-15T10:04:36.52Z"
          },
          {
            "$type": "Tfl.Api.Presentation.Entities.AdditionalProperties, Tfl.Api.Presentation.Entities",
            "category": "Description",
            "key": "LastUpdated",
            "sourceSystemKey": "ChargeMasterChargePoint",
            "value": "Sep 15 2025 10:04AM",
            "modified": "2025-09-15T10:04:36.52Z"
          }
        ],
        "children": [],
        "childrenUrls": [],
        "lat": 51.490597,
        "lon": -0.207703
      }
    ]
"""

@mark.parametrize(
    'connector_id, expected',
    [
        (
            'UT05X7-3',
            dict(
                common_name='21 Bridge Road Station UT05X7 Connector 3',
                lat=51.564362,
                lon=-0.278134,
                connector_type='Type 2',
                parent_station='ChargePointESB-UT05X7',
                status='Unknown',
                power_kw=43,
                updated_at=pd.to_datetime('2025-09-15T10:04:36.16Z', format='ISO8601'),
            ),
        ),
        (
            '24122-61977',
            dict(
                common_name='16 Talgarth Road Station 24122 Connector 1',
                lat=51.490597,
                lon=-0.207703,
                connector_type='CCS',
                parent_station='ChargePointCM-24122',
                status='Unavailable',
                power_kw=50,
                updated_at=pd.to_datetime('2025-09-15T11:09:21.557Z', format='ISO8601'),
            ),
        ),
    ],
)
def test_clean(connector_id, expected):

    cleaned_df = clean_chargers(raw)
    row = cleaned_df.loc[cleaned_df['connector_id'] == connector_id].iloc[0]

    assert row['common_name'] == expected['common_name']
    assert row['lat'] == approx(expected['lat'])
    assert row['lon'] == approx(expected['lon'])
    # from additional_props:
    assert row['connector_type'] == expected['connector_type']
    assert row['parent_station'] == expected['parent_station']
    assert row['status'] == expected['status']
    assert row['power_kw'] == expected['power_kw']
    assert row['updated_at'] == expected['updated_at']
