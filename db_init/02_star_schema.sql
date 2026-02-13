--bikes:
CREATE TABLE fct_bikes_availability_change (
    bike_point_id int  NOT NULL,
    updated_at timestamptz  NOT NULL,
    nb_standard_bikes int  NOT NULL,
    nb_e_bikes int  NOT NULL,
    nb_empty_docks int  NOT NULL,
    nb_docks int  NOT NULL,
    CONSTRAINT fct_bikes_availability_pk PRIMARY KEY (
        bike_point_id,
        updated_at
    )
);


CREATE TABLE dim_bike_point (
    id serial  NOT NULL,
    tfl_id int  NOT NULL,
    common_name varchar(100)  NOT NULL,
    lat decimal(8,5)  NOT NULL,
    lon decimal(8,5)  NOT NULL,
    CONSTRAINT dim_bike_point_unique_tfl_id UNIQUE (tfl_id) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_bike_point_pk PRIMARY KEY (id)
);


--chargers:
CREATE TABLE fct_connector_availability_change (
    charging_station_id int  NOT NULL,
    connector_id int  NOT NULL,
    updated_at timestamptz NOT NULL,
    status varchar(20)  NOT NULL,
    CONSTRAINT fct_connector_availability_pk PRIMARY KEY (
        charging_station_id,
        connector_id,
        updated_at
    )
);


CREATE TABLE dim_charging_station (
    id serial  NOT NULL,
    tfl_station_id varchar(40)  NOT NULL,
    name varchar(200)  NOT NULL,
    lat decimal(8,5)  NOT NULL,
    lon decimal(8,5)  NOT NULL,
    CONSTRAINT dim_charging_station_unique_tfl_id UNIQUE (tfl_station_id) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_charging_station_pk PRIMARY KEY (id)
);


CREATE TABLE dim_connector (
    id serial  NOT NULL,
    tfl_id char(22)  NOT NULL,
    power_kw int  NOT NULL,
    connector_type varchar(30)  NOT NULL,
    CONSTRAINT dim_unique_connector UNIQUE (tfl_id) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_connector_pk PRIMARY KEY (id)
);


--roads:
CREATE TABLE fct_disrupted_segment (
    closure_type_id int  NOT NULL,
    disruption_id int  NOT NULL,
    segment_id int  NOT NULL,
    start_date_time timestamptz  NOT NULL,
    end_date_time timestamptz  NOT NULL,
    CONSTRAINT fct_disrupted_segment_pk PRIMARY KEY (
        closure_type_id,
        disruption_id,
        segment_id
    )
);


CREATE TABLE dim_closure_type (
    id serial  NOT NULL,
    closure varchar(20)  NOT NULL,
    directions varchar(30)  NOT NULL,
    CONSTRAINT close_ak_1 UNIQUE (
        closure,
        directions
    ) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_closure_type_pk PRIMARY KEY (id)
);


CREATE TABLE dim_disruption (
    id serial  NOT NULL ,
    tfl_id int  NOT NULL,
    category varchar(20)  NOT NULL,
    subcategory varchar(30)  NOT NULL,
    severity varchar(20)  NOT NULL,
    CONSTRAINT dim_unique_dis_tfl_id UNIQUE (tfl_id) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_disruption_pk PRIMARY KEY (id)
);


CREATE TABLE dim_street_segment (
    id serial  NOT NULL,
    tfl_id char(17)  NOT NULL,
    start_lat decimal(8,5)  NOT NULL,
    start_lon decimal(8,5)  NOT NULL,
    end_lat decimal(8,5)  NOT NULL,
    end_lon decimal(8,5)  NOT NULL,
    street_name varchar(50)  NOT NULL,
    CONSTRAINT dim_unique_seg_tfl_id UNIQUE (tfl_id) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_street_segment_pk PRIMARY KEY (id)
);
