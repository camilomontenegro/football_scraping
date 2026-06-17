-- Elimina columnas de dim_stadium que nunca se rellenan en este proyecto
-- (0% de cobertura tras carga TM + enriquecimiento Wikidata).
-- Idempotente: DROP COLUMN IF EXISTS.

ALTER TABLE dim_stadium DROP COLUMN IF EXISTS capacity_intl;
ALTER TABLE dim_stadium DROP COLUMN IF EXISTS naming_rights;
ALTER TABLE dim_stadium DROP COLUMN IF EXISTS previous_names_raw;
ALTER TABLE dim_stadium DROP COLUMN IF EXISTS pitch_length_m;
ALTER TABLE dim_stadium DROP COLUMN IF EXISTS pitch_width_m;
ALTER TABLE dim_stadium DROP COLUMN IF EXISTS has_pitch_heating;
ALTER TABLE dim_stadium DROP COLUMN IF EXISTS roof_type;
ALTER TABLE dim_stadium DROP COLUMN IF EXISTS seats_covered;
ALTER TABLE dim_stadium DROP COLUMN IF EXISTS seats_vip;
ALTER TABLE dim_stadium DROP COLUMN IF EXISTS seats_standing;
ALTER TABLE dim_stadium DROP COLUMN IF EXISTS inaugurated_year;
ALTER TABLE dim_stadium DROP COLUMN IF EXISTS refurbished_year;
