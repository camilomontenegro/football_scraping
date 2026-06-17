-- Cloudinary folder paths can exceed 100 chars (e.g. Telstar 711-Stadion).
ALTER TABLE dim_stadium_master
    ALTER COLUMN cloudinary_public_id TYPE VARCHAR(255);
