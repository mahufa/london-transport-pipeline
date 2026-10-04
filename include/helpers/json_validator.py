import json
from typing import Iterator

import ijson

# each element is a field name or a tuple of alternative field names
RecordKeyFields = tuple[str | tuple[str, ...], ...]


def generate_clean_lines(
        stream,
        dlq_buffer,
        key_fields: RecordKeyFields,
) -> Iterator[tuple[int, str, str]]:
    ordinal = 0
    for record in ijson.items(stream, 'item', use_float=True):
        if not isinstance(record, dict):
            dlq_buffer.append((json.dumps(record), 'Not a JSON object'))
            continue

        try:
            record_str = _serialize_record(record)
            record_key = _extract_record_key(record, key_fields)
            error_msg = _validate_and_get_error_msg(record_key, record_str)

            if error_msg:
                dlq_buffer.append((record_str, error_msg))
                continue

            yield ordinal, record_key, record_str
            ordinal += 1

        except (TypeError, ValueError) as e:
            dlq_buffer.append(
                (str(record),
                 f'Serialization error: {e}')
            )


def _serialize_record(record: dict) -> str:
    record_str = json.dumps(
        record,
        ensure_ascii=False,
        allow_nan=False,
        separators=(',', ':'))
    record_str.encode('utf-8') # raises UnicodeEncodeError on lone surrogates (jsonb rejects them)

    return record_str


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
