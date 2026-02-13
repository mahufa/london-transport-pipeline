ALTER TABLE fct_bikes_availability_change ADD CONSTRAINT fct_bikes_availability_dim_bike_point
    FOREIGN KEY (bike_point_id)
    REFERENCES dim_bike_point (id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;


ALTER TABLE fct_connector_availability_change ADD CONSTRAINT fct_chargers_availability_dim_charging_station
    FOREIGN KEY (charging_station_id)
    REFERENCES dim_charging_station (id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;


ALTER TABLE fct_connector_availability_change ADD CONSTRAINT fct_connector_availability_dim_connector
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
    REFERENCES dim_disruption (id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;


ALTER TABLE fct_disrupted_segment ADD CONSTRAINT fct_disrupted_segment_dim_street
    FOREIGN KEY (segment_id)
    REFERENCES dim_street_segment (id)
    NOT DEFERRABLE
    INITIALLY IMMEDIATE
;
