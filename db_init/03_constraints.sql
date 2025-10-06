ALTER TABLE fct_bikes_availability ADD CONSTRAINT fct_bikes_availability_dim_bike_point
    FOREIGN KEY (bike_point_id)
    REFERENCES dim_bike_point (id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;


ALTER TABLE fct_bikes_availability ADD CONSTRAINT fct_bikes_availability_dim_time
    FOREIGN KEY (time_id)
    REFERENCES dim_time (id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;


ALTER TABLE fct_connector_availability ADD CONSTRAINT fct_chargers_availability_dim_charging_station
    FOREIGN KEY (charging_station_id)
    REFERENCES dim_charging_station (id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;


ALTER TABLE fct_connector_availability ADD CONSTRAINT fct_chargers_availability_dim_time
    FOREIGN KEY (time_id)
    REFERENCES dim_time (id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;


ALTER TABLE fct_connector_availability ADD CONSTRAINT fct_connector_availability_dim_connector
    FOREIGN KEY (connector_id)
    REFERENCES dim_connector (id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;


ALTER TABLE fct_disrupted_segment ADD CONSTRAINT fct_disrupted_segment_dim_closure_type
    FOREIGN KEY (closure_type_id)
    REFERENCES dim_closure_type (id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;


ALTER TABLE fct_disrupted_segment ADD CONSTRAINT fct_disrupted_segment_dim_disruption
    FOREIGN KEY (disruption_id)
    REFERENCES dim_disruption (disruption_tfl_id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;


ALTER TABLE fct_disrupted_segment ADD CONSTRAINT fct_disrupted_segment_dim_duration
    FOREIGN KEY (duration_id)
    REFERENCES dim_duration (id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;


ALTER TABLE fct_disrupted_segment ADD CONSTRAINT fct_disrupted_segment_dim_street
    FOREIGN KEY (street_id)
    REFERENCES dim_street_segment (id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;
