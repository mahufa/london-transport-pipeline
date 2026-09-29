-- pg_input_is_valid() returns NULL for NULL input,
-- so missing values would pass as valid
CREATE OR REPLACE FUNCTION silver.is_valid(
    value text,
    type_name text)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(pg_input_is_valid(value, type_name), false)
$$;
