from types import SimpleNamespace

from pytest import mark, raises

from include.helpers.dataset_utils import (
    get_batch_id_from_path,
    get_dataset_short_name,
    get_event_extras,
    get_layer_from_uri,
)


@mark.parametrize(
    'dataset_uri, expected_layer',
    [
        ('chargers/raw/', 'raw'),
        ('chargers/bronze/', 'bronze'),
        ('chargers/bronze', 'bronze'),
    ],
    ids=['raw', 'bronze', 'no_trailing_slash'],
)
def test_layer_is_correct(dataset_uri, expected_layer):
    assert get_layer_from_uri(dataset_uri) == expected_layer


@mark.parametrize(
    'path',
    [
        'roads/2024-01-01/20240101T000000.json',
        'roads/2024-01-01/20240101T000000.json.gz'
    ],
    ids=['json', 'gz']
)
def test_batch_id_is_correct(path):
    assert get_batch_id_from_path(path) == '240101T000000'


def test_dataset_short_name_is_correct():
    assert get_dataset_short_name('roads/raw/') == 'roads'


def test_get_event_extras_missing_extra_raises():
    events = {'roads/raw/': [
        _event(
            'roads/raw/',
            batch_id='x'
        )
    ]}  # raw expects 'path'
    with raises(KeyError):
        get_event_extras(events)


def test_get_event_extras_groups_by_dataset():
    events = {
        'roads/raw/': [
            _event(
                'roads/raw/',
                path='roads/2024-01-01/20240101T000000.json',
            ),
            _event(
                'roads/raw/',
                path='roads/2024-01-01/20240101T010000.json',
            ),
        ],
        'chargers/bronze/': [
            _event(
                'chargers/bronze/',
                batch_id='240101T000000',
            )
        ],
    }

    assert get_event_extras(events) == {
        'roads/raw/': [
            'roads/2024-01-01/20240101T000000.json',
            'roads/2024-01-01/20240101T010000.json',
        ],
        'chargers/bronze/': [
            '240101T000000',
        ],
    }


def _event(uri, **extra):
    return SimpleNamespace(uri=uri, extra=extra)