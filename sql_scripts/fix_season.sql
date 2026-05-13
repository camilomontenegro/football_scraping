-- Script  para corregir el campo season en DIM_MATCH  y establecer un formato único.
-- Se añade este script  para el supuesto de que hayan copias de la base de datos que no tengan el campo normalizado
-- La normalización previa a la carga de datos se realiza  con utils.season_utils.py

-- hay dos partidos . Understat no es fuente  principal de la tabla dim_match 
select * from dim_match where data_source = 'understat';

-- borrar los evetnos y tiros asociados a esos partidos 
DELETE FROM fact_events WHERE match_id IN (1919, 1920);
DELETE FROM fact_shots  WHERE match_id IN (1919, 1920);

--  borrar esos partidos de dim_match 
DELETE FROM dim_match WHERE match_id IN (1919, 1920);


--  ver cuantos formatos de season hay y cuantos  registros tienen ese formato 
SELECT DISTINCT season, COUNT(*) FROM dim_match GROUP BY season ORDER BY season;

- Actualizar
UPDATE dim_match SET season = '2020/2021' WHERE season IN ('20/21', 'LaLiga 20/21');
UPDATE dim_match SET season = '2021/2022' WHERE season IN ('21/22', 'LaLiga 21/22');
UPDATE dim_match SET season = '2022/2023' WHERE season IN ('22/23', 'LaLiga 22/23');
UPDATE dim_match SET season = '2023/2024' WHERE season IN ('23/24', 'LaLiga 23/24');
UPDATE dim_match SET season = '2024/2025' WHERE season IN ('24/25', 'LaLiga 24/25');
UPDATE dim_match SET season = '2025/2026' WHERE season IN ('25/26', 'LaLiga 25/26');

--comprobar resultado 
SELECT DISTINCT season, COUNT(*) FROM dim_match GROUP BY season ORDER BY season;