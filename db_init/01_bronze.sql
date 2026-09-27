CREATE TABLE bronze.raw_tfl (
    source varchar(30)  NOT NULL,
    batch_id char(13)  NOT NULL,
    record_key text  NOT NULL,
    payload jsonb  NOT NULL,
    loaded_at timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT raw_tfl_pk PRIMARY KEY (source, batch_id, record_key)
);


CREATE TABLE bronze.rejected_records (
    id bigint  GENERATED ALWAYS AS IDENTITY,
    source varchar(30)  NOT NULL,
    batch_id char(13)  NOT NULL,
    stage varchar(10)  NOT NULL,
    record text  NOT NULL,
    error text  NOT NULL,
    rejected_at timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT rejected_records_stage_check CHECK (stage IN ('ingest', 'silver')),
    CONSTRAINT rejected_records_pk PRIMARY KEY (id)
);
