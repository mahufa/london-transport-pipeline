import pandas as pd
from pytest import approx, mark

from include.cleaners.roads.roads import clean_roads


batch_id = 'TEST-BATCH-ID'

raw = """
    [
      {
        "$type": "Tfl.Api.Presentation.Entities.DisruptedStreetSegment, Tfl.Api.Presentation.Entities",
        "streetName": "[B452] WINDMILL ROAD (TW8 )",
        "closure": "Closed",
        "directions": "Both directions",
        "lineString": "[[-0.308696,51.489609],[-0.308888,51.489881]]",
        "distruptedStreetId": "TIMS-21344046186",
        "disruptionId": "TIMS-213440",
        "startLat": 51.489609,
        "startLon": -0.308696,
        "endLat": 51.489881,
        "endLon": -0.308888,
        "severity": "Minimal",
        "category": "Works",
        "subCategory": "Motorways works",
        "comments": "[A4] Great West Road (Both directions) between [B452] Windmill Road and [M4] M 4 - Various road closures to be implemented to facilitate carriageway repairs. ",
        "startDateTime": "2025-07-09T21:00:00Z",
        "endDateTime": "2025-11-29T05:00:00Z",
        "levelOfInterest": "Low",
        "location": "[A4] GREAT WEST ROAD (TW8 ) (Hounslow)",
        "recurringSchedules": []
      },
      {
        "$type": "Tfl.Api.Presentation.Entities.DisruptedStreetSegment, Tfl.Api.Presentation.Entities",
        "streetName": "STRAIGHT ROAD (RM2 ,RM3 )",
        "closure": "Closed",
        "directions": "Both directions",
        "lineString": "[[0.212949,51.594536],[0.212794,51.59502]]",
        "distruptedStreetId": "TIMS-20677253453",
        "disruptionId": "TIMS-206772",
        "startLat": 51.594536,
        "startLon": 0.212949,
        "endLat": 51.59502,
        "endLon": 0.212794,
        "severity": "Moderate",
        "category": "Works",
        "subCategory": "TfL works",
        "comments": "Gallows Corner Flyover Refurbishment - [A12] Eastern Avenue East (Both directions) between [A12] Colchester Road and [A127] Southend Arterial Road - Various restrictions, including some overnight closures, to facilitate the refurbishment of Gallows Corner Flyover.",
        "startDateTime": "2025-03-15T21:00:00Z",
        "endDateTime": "2025-10-31T18:00:00Z",
        "levelOfInterest": "High",
        "location": "[A12] EASTERN AVENUE EAST (RM2 ,RM3 ) (Havering)",
        "recurringSchedules": []
      }
    ]
"""

@mark.parametrize(
    'disrupted_road_id, expected',
    [
        (
            'TIMS-21344046186',
            dict(
                disruption_id=213440,
                start_lat=51.489609,
                start_lon=-0.308696,
                end_lat=51.489881,
                end_lon=-0.308888,
                street_name='[B452] WINDMILL ROAD (TW8 )',
                closure='Closed',
                directions='Both directions',
                severity='Minimal',
                category='Works',
                sub_category='Motorways works',
                start_date_time=pd.to_datetime('2025-07-09T21:00:00Z', format='ISO8601'),
                end_date_time=pd.to_datetime('2025-11-29T05:00:00Z', format='ISO8601'),
            ),
        ),
        (
            'TIMS-20677253453',
            dict(
                disruption_id=206772,
                start_lat=51.594536,
                start_lon=0.212949,
                end_lat=51.59502,
                end_lon=0.212794,
                street_name='STRAIGHT ROAD (RM2 ,RM3 )',
                closure='Closed',
                directions='Both directions',
                severity='Moderate',
                category='Works',
                sub_category='TfL works',
                start_date_time=pd.to_datetime('2025-03-15T21:00:00Z', format='ISO8601'),
                end_date_time=pd.to_datetime('2025-10-31T18:00:00Z', format='ISO8601'),
            ),
        ),
    ],
)
def test_clean(disrupted_road_id, expected):

    cleaned_df = clean_roads(raw, batch_id)
    row = cleaned_df.loc[cleaned_df['disrupted_road_id'] == disrupted_road_id].iloc[0]

    assert row['disruption_id'] == expected['disruption_id']
    assert row['batch_id'] == 'TEST-BATCH-ID'
    assert row['start_lat'] == approx(expected['start_lat'])
    assert row['start_lon'] == approx(expected['start_lon'])
    assert row['end_lat'] == approx(expected['end_lat'])
    assert row['end_lon'] == approx(expected['end_lon'])
    assert row['street_name'] == expected['street_name']
    assert row['closure'] == expected['closure']
    assert row['directions'] == expected['directions']
    assert row['severity'] == expected['severity']
    assert row['category'] == expected['category']
    assert row['sub_category'] == expected['sub_category']
    assert row['start_date_time'] == expected['start_date_time']
    assert row['end_date_time'] == expected['end_date_time']
