import json
from dataclasses import dataclass
from typing import Iterator

import ijson

# C backend replaces lone surrogates with '?' and rejects NaN/Infinity/overflow,
# so every parsed record is serializable; fail at import rather than fall back
IJSON_BACKEND = ijson.get_backend('yajl2_c')

# each element is a field name or a tuple of alternative field names
RecordKeyFields = tuple[str | tuple[str, ...], ...]


@dataclass(frozen=True)
class Accepted:
    key: str
    payload: str


@dataclass(frozen=True)
class Rejected:
    record: str
    error: str


def generate_clean_lines(
        stream,
        dlq_buffer,
        key_fields: RecordKeyFields,
) -> Iterator[tuple[int, str, str]]:
    ordinal = 0
    for record in IJSON_BACKEND.items(stream, 'item', use_float=True):
        result = classify_record(record, key_fields)

        if isinstance(result, Rejected):
            dlq_buffer.append((result.record, result.error))
            continue

        yield ordinal, result.key, result.payload
        ordinal += 1


def classify_record(
        record,
        key_fields: RecordKeyFields,
) -> Accepted | Rejected:
    if not isinstance(record, dict):
        return Rejected(json.dumps(record), 'Not a JSON object')

    record_str = _serialize_record(record)
    record_key = _extract_record_key(record, key_fields)
    error_msg = _validate_and_get_error_msg(record_key, record_str)

    if error_msg:
        return Rejected(record_str, error_msg)

    return Accepted(record_key, record_str)


def _serialize_record(record: dict) -> str:
    return json.dumps(
        record,
        ensure_ascii=False,
        allow_nan=False,
        separators=(',', ':'))


def _validate_and_get_error_msg(
        record_key: str | None,
        record_str: str,
) -> str | None:
    if record_key is None:
        return 'Missing record key'

    if '\\u0000' in record_str:
        return 'Contains null bytes'

    return None


def _extract_record_key(
        record: dict,
        key_fields: RecordKeyFields,
) -> str | None:
    parts = []
    for field in key_fields:
        alternatives = (field,) if isinstance(field, str) else field
        value = next(
            (record[name] for name in alternatives if record.get(name) is not None),
            None,
        )
        if value is None:
            return None
        parts.append(str(value))

    return ','.join(parts)
