CREATE TABLE staging_bike_points (
   bike_point_id int  NOT NULL,
   common_name varchar(100)  NOT NULL,
   lat decimal(8,5)  NOT NULL,
   lon decimal(8,5)  NOT NULL,
   nb_bikes int  NOT NULL,
   nb_docks int  NOT NULL,
   nb_e_bikes int  NOT NULL,
   nb_empty_docks int  NOT NULL,
   nb_standard_bikes int  NOT NULL,
   updated_at timestamptz  NOT NULL,
   batch_id char(13)  NOT NULL,
   CONSTRAINT staging_bike_points_pk PRIMARY KEY (bike_point_id, batch_id)
);


CREATE TABLE staging_chargers (
   connector_id varchar(20)  NOT NULL,
   common_name varchar(200)  NOT NULL,
   lat decimal(8,5)  NOT NULL,
   lon decimal(8,5)  NOT NULL,
   connector_type varchar(30)  NOT NULL,
   parent_station varchar(100)  NOT NULL,
   power_kw int  NOT NULL,
   status varchar(20)  NOT NULL,
   updated_at timestamptz  NOT NULL,
   batch_id char(13)  NOT NULL,
   CONSTRAINT staging_chargers_pk PRIMARY KEY (connector_id, batch_id)
);


CREATE TABLE staging_roads (
   street_name varchar(50)  NOT NULL,
   closure varchar(20)  NOT NULL,
   directions varchar(30)  NOT NULL,
   disrupted_road_id varchar(20)  NOT NULL,
   disruption_id int  NOT NULL,
   start_lat decimal(8,5)  NOT NULL,
   start_lon decimal(8,5)  NOT NULL,
   end_lat decimal(8,5)  NOT NULL,
   end_lon decimal(8,5)  NOT NULL,
   severity varchar(20)  NOT NULL,
   category varchar(20)  NOT NULL,
   sub_category varchar(30)  NOT NULL,
   start_date_time timestamptz  NOT NULL,
   end_date_time timestamptz  NOT NULL,
   batch_id char(13)  NOT NULL,
   CONSTRAINT staging_roads_pk PRIMARY KEY (disrupted_road_id, batch_id)
);