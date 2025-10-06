--bikes:
CREATE TABLE fct_bikes_availability (
    bike_point_id int  NOT NULL,
    time_id int  NOT NULL,
    nb_standard_bikes int  NOT NULL,
    nb_e_bikes int  NOT NULL,
    nb_empty_docks int  NOT NULL,
    nb_docks int  NOT NULL,
    CONSTRAINT fct_bikes_availability_pk PRIMARY KEY (bike_point_id,time_id)
);


CREATE TABLE dim_bike_point (
    id serial  NOT NULL,
    common_name varchar(100)  NOT NULL,
    lat decimal(8,5)  NOT NULL,
    lon decimal(8,5)  NOT NULL,
    CONSTRAINT dim_bike_point_unique_name UNIQUE (common_name) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_bike_point_pk PRIMARY KEY (id)
);


--chargers:
CREATE TABLE fct_connector_availability (
    time_id int  NOT NULL,
    charging_station_id int  NOT NULL,
    connector_id int  NOT NULL,
    status varchar(20)  NOT NULL,
    CONSTRAINT fct_connector_availability_pk PRIMARY KEY (time_id,charging_station_id,connector_id)
);


CREATE TABLE dim_charging_station (
    id serial  NOT NULL,
    tfl_station_id varchar(100)  NOT NULL,
    common_name varchar(200)  NOT NULL,
    lat decimal(8,5)  NOT NULL,
    lon decimal(8,5)  NOT NULL,
    CONSTRAINT dim_charging_station_unique_tfl_id UNIQUE (tfl_station_id) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_charging_station_unique_name UNIQUE (common_name) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_charging_station_pk PRIMARY KEY (id)
);


CREATE TABLE dim_connector (
    id serial  NOT NULL,
    power_kw int  NOT NULL,
    connector_type varchar(30)  NOT NULL,
    CONSTRAINT dim_unique_connector UNIQUE (power_kw, connector_type) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_connector_pk PRIMARY KEY (id)
);


--roads:
CREATE TABLE fct_disrupted_segment (
    closure_type_id int  NOT NULL,
    disruption_id int  NOT NULL,
    duration_id int  NOT NULL,
    street_id int  NOT NULL,
    CONSTRAINT fct_disrupted_segment_pk PRIMARY KEY (closure_type_id,disruption_id,duration_id,street_id)
);


CREATE TABLE dim_closure_type (
    id serial  NOT NULL,
    closure varchar(20)  NOT NULL,
    directions varchar(30)  NOT NULL,
    CONSTRAINT close_ak_1 UNIQUE (closure, directions) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_closure_type_pk PRIMARY KEY (id)
);


CREATE TABLE dim_disruption (
    disruption_tfl_id int  NOT NULL,
    category varchar(20)  NOT NULL,
    subcategory varchar(30)  NOT NULL,
    severity varchar(20)  NOT NULL,
    CONSTRAINT dim_disruption_pk PRIMARY KEY (disruption_tfl_id)
);


CREATE TABLE dim_duration (
    id serial  NOT NULL,
    start_date_time timestamptz  NOT NULL,
    end_date_time timestamptz  NOT NULL,
    duration interval  NOT NULL,
    CONSTRAINT dim_unique_interval UNIQUE (start_date_time, end_date_time) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_duration_pk PRIMARY KEY (id)
);


CREATE TABLE dim_street_segment (
    id serial  NOT NULL,
    start_lat decimal(8,5)  NOT NULL,
    start_lon decimal(8,5)  NOT NULL,
    end_lat decimal(8,5)  NOT NULL,
    end_lon decimal(8,5)  NOT NULL,
    street_name varchar(50)  NOT NULL,
    disrupted_road_tfl_id varchar(20)  NOT NULL,
    CONSTRAINT dim_unique_street_segment UNIQUE (start_lat, start_lon, end_lat, end_lon) NOT DEFERRABLE  INITIALLY IMMEDIATE,
    CONSTRAINT dim_street_segment_pk PRIMARY KEY (id)
);


-- common dims:
CREATE TABLE dim_time (
    id serial  NOT NULL,
    updated_at timestamptz  NOT NULL,
    year smallint  NOT NULL,
    month smallint  NOT NULL,
    day smallint  NOT NULL,
    hour smallint  NOT NULL,
    weekday smallint  NOT NULL,
    CONSTRAINT dim_time_pk PRIMARY KEY (id)
);
