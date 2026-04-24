
-- el id de tranfermakr no es un integer , sino que en la pagina es string. Es un código ES1, L1, F12, etc
--https://www.transfermarkt.es/navigation/wettbewerbe 
ALTER TABLE dim_competition 
ALTER COLUMN id_transfermarkt TYPE VARCHAR(50);

INSERT INTO dim_competition (canonical_name, id_transfermarkt, id_whoscored)
VALUES 
    ('La Liga', 'ES1', 4),
    ('UEFA Champions League', 'CL', 12);