-- ==========================================================
-- SCHEMA football_db
-- Auto-generated from the current PostgreSQL database schema.
-- Regenerate with pg_dump --schema-only when the DB changes.
-- ==========================================================

--
-- PostgreSQL database dump
--


-- Dumped from database version 18.3
-- Dumped by pg_dump version 18.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

ALTER TABLE IF EXISTS ONLY public.player_review DROP CONSTRAINT IF EXISTS player_review_suggested_canonical_id_fkey;
ALTER TABLE IF EXISTS ONLY public.player_review DROP CONSTRAINT IF EXISTS player_review_canonical_id_assigned_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_transfers DROP CONSTRAINT IF EXISTS fact_transfers_to_team_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_transfers DROP CONSTRAINT IF EXISTS fact_transfers_player_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_transfers DROP CONSTRAINT IF EXISTS fact_transfers_from_team_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_shots DROP CONSTRAINT IF EXISTS fact_shots_team_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_shots DROP CONSTRAINT IF EXISTS fact_shots_player_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_shots DROP CONSTRAINT IF EXISTS fact_shots_match_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_player_match_stats DROP CONSTRAINT IF EXISTS fact_player_match_stats_team_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_player_match_stats DROP CONSTRAINT IF EXISTS fact_player_match_stats_player_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_player_match_stats DROP CONSTRAINT IF EXISTS fact_player_match_stats_match_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_market_value DROP CONSTRAINT IF EXISTS fact_market_value_player_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_market_value DROP CONSTRAINT IF EXISTS fact_market_value_club_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_injuries DROP CONSTRAINT IF EXISTS fact_injuries_player_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_formations DROP CONSTRAINT IF EXISTS fact_formations_team_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_formations DROP CONSTRAINT IF EXISTS fact_formations_match_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_formations DROP CONSTRAINT IF EXISTS fact_formations_captain_player_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_events DROP CONSTRAINT IF EXISTS fact_events_team_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_events DROP CONSTRAINT IF EXISTS fact_events_player_id_fkey;
ALTER TABLE IF EXISTS ONLY public.fact_events DROP CONSTRAINT IF EXISTS fact_events_match_id_fkey;
ALTER TABLE IF EXISTS ONLY public.dim_team DROP CONSTRAINT IF EXISTS dim_team_home_stadium_master_id_fkey;
ALTER TABLE IF EXISTS ONLY public.dim_stadium_names_history DROP CONSTRAINT IF EXISTS dim_stadium_names_history_stadium_id_fkey;
ALTER TABLE IF EXISTS ONLY public.dim_stadium DROP CONSTRAINT IF EXISTS dim_stadium_canonical_team_id_fkey;
ALTER TABLE IF EXISTS ONLY public.dim_match DROP CONSTRAINT IF EXISTS dim_match_referee_id_fkey;
ALTER TABLE IF EXISTS ONLY public.dim_match DROP CONSTRAINT IF EXISTS dim_match_match_stadium_id_fkey;
ALTER TABLE IF EXISTS ONLY public.dim_match DROP CONSTRAINT IF EXISTS dim_match_home_team_id_fkey;
ALTER TABLE IF EXISTS ONLY public.dim_match DROP CONSTRAINT IF EXISTS dim_match_competition_id_fkey;
ALTER TABLE IF EXISTS ONLY public.dim_match DROP CONSTRAINT IF EXISTS dim_match_away_team_id_fkey;
ALTER TABLE IF EXISTS ONLY public.bridge_team_season_stadium DROP CONSTRAINT IF EXISTS bridge_team_season_stadium_stadium_id_fkey;
ALTER TABLE IF EXISTS ONLY public.bridge_team_season_stadium DROP CONSTRAINT IF EXISTS bridge_team_season_stadium_canonical_team_id_fkey;
DROP INDEX IF EXISTS public.ux_transfers_unique;
DROP INDEX IF EXISTS public.ux_team_whoscored;
DROP INDEX IF EXISTS public.ux_team_understat;
DROP INDEX IF EXISTS public.ux_team_transfermarkt;
DROP INDEX IF EXISTS public.ux_team_statsbomb;
DROP INDEX IF EXISTS public.ux_team_sofascore;
DROP INDEX IF EXISTS public.ux_stadium_team_validfrom;
DROP INDEX IF EXISTS public.ux_shots_unique;
DROP INDEX IF EXISTS public.ux_referee_whoscored;
DROP INDEX IF EXISTS public.ux_player_whoscored;
DROP INDEX IF EXISTS public.ux_player_understat;
DROP INDEX IF EXISTS public.ux_player_transfermkt;
DROP INDEX IF EXISTS public.ux_player_statsbomb;
DROP INDEX IF EXISTS public.ux_player_sofascore;
DROP INDEX IF EXISTS public.ux_player_match_stats;
DROP INDEX IF EXISTS public.ux_match_whoscored;
DROP INDEX IF EXISTS public.ux_match_understat;
DROP INDEX IF EXISTS public.ux_match_statsbomb;
DROP INDEX IF EXISTS public.ux_match_sofascore;
DROP INDEX IF EXISTS public.ux_market_value_unique;
DROP INDEX IF EXISTS public.ux_injuries_unique;
DROP INDEX IF EXISTS public.ux_formations_unique;
DROP INDEX IF EXISTS public.ux_events_unique;
DROP INDEX IF EXISTS public.ux_bridge_team_stadium_season_ctx;
DROP INDEX IF EXISTS public.idx_transfers_to_team;
DROP INDEX IF EXISTS public.idx_transfers_season;
DROP INDEX IF EXISTS public.idx_transfers_player;
DROP INDEX IF EXISTS public.idx_transfers_from_team;
DROP INDEX IF EXISTS public.idx_team_home_stadium;
DROP INDEX IF EXISTS public.idx_stadium_wikidata_qid;
DROP INDEX IF EXISTS public.idx_stadium_team_tm;
DROP INDEX IF EXISTS public.idx_stadium_team;
DROP INDEX IF EXISTS public.idx_stadium_name_lower;
DROP INDEX IF EXISTS public.idx_stadium_latlon;
DROP INDEX IF EXISTS public.idx_stadium_data_hash;
DROP INDEX IF EXISTS public.idx_shots_team;
DROP INDEX IF EXISTS public.idx_shots_player;
DROP INDEX IF EXISTS public.idx_shots_match;
DROP INDEX IF EXISTS public.idx_referee_name_lower;
DROP INDEX IF EXISTS public.idx_pms_team;
DROP INDEX IF EXISTS public.idx_pms_player;
DROP INDEX IF EXISTS public.idx_pms_match;
DROP INDEX IF EXISTS public.idx_player_review_unresolved;
DROP INDEX IF EXISTS public.idx_player_review_suggested;
DROP INDEX IF EXISTS public.idx_player_review_source;
DROP INDEX IF EXISTS public.idx_player_review_assigned;
DROP INDEX IF EXISTS public.idx_match_referee;
DROP INDEX IF EXISTS public.idx_match_match_stadium;
DROP INDEX IF EXISTS public.idx_match_home_team;
DROP INDEX IF EXISTS public.idx_match_date;
DROP INDEX IF EXISTS public.idx_match_away_team;
DROP INDEX IF EXISTS public.idx_market_value_player;
DROP INDEX IF EXISTS public.idx_market_value_date;
DROP INDEX IF EXISTS public.idx_market_value_club;
DROP INDEX IF EXISTS public.idx_injuries_player;
DROP INDEX IF EXISTS public.idx_formations_team;
DROP INDEX IF EXISTS public.idx_formations_match;
DROP INDEX IF EXISTS public.idx_fact_events_ws_event_id;
DROP INDEX IF EXISTS public.idx_fact_events_qualifiers;
DROP INDEX IF EXISTS public.idx_events_type;
DROP INDEX IF EXISTS public.idx_events_team;
DROP INDEX IF EXISTS public.idx_events_shot_zone;
DROP INDEX IF EXISTS public.idx_events_player;
DROP INDEX IF EXISTS public.idx_events_match;
DROP INDEX IF EXISTS public.idx_events_body_part;
DROP INDEX IF EXISTS public.idx_events_big_chance;
DROP INDEX IF EXISTS public.idx_events_assisted;
DROP INDEX IF EXISTS public.idx_dim_match_competition_id;
DROP INDEX IF EXISTS public.idx_dim_competition_whoscored_unique;
DROP INDEX IF EXISTS public.idx_dim_competition_understat_unique;
DROP INDEX IF EXISTS public.idx_dim_competition_transfermarkt_unique;
DROP INDEX IF EXISTS public.idx_dim_competition_statsbomb_unique;
DROP INDEX IF EXISTS public.idx_dim_competition_sofascore_unique;
DROP INDEX IF EXISTS public.idx_dim_competition_name_unique;
ALTER TABLE IF EXISTS ONLY public.player_review DROP CONSTRAINT IF EXISTS player_review_pkey;
ALTER TABLE IF EXISTS ONLY public.fact_transfers DROP CONSTRAINT IF EXISTS fact_transfers_pkey;
ALTER TABLE IF EXISTS ONLY public.fact_shots DROP CONSTRAINT IF EXISTS fact_shots_pkey;
ALTER TABLE IF EXISTS ONLY public.fact_player_match_stats DROP CONSTRAINT IF EXISTS fact_player_match_stats_pkey;
ALTER TABLE IF EXISTS ONLY public.fact_market_value DROP CONSTRAINT IF EXISTS fact_market_value_pkey;
ALTER TABLE IF EXISTS ONLY public.fact_injuries DROP CONSTRAINT IF EXISTS fact_injuries_pkey;
ALTER TABLE IF EXISTS ONLY public.fact_formations DROP CONSTRAINT IF EXISTS fact_formations_pkey;
ALTER TABLE IF EXISTS ONLY public.fact_events DROP CONSTRAINT IF EXISTS fact_events_pkey;
ALTER TABLE IF EXISTS ONLY public.dim_team DROP CONSTRAINT IF EXISTS dim_team_pkey;
ALTER TABLE IF EXISTS ONLY public.dim_stadium DROP CONSTRAINT IF EXISTS dim_stadium_pkey;
ALTER TABLE IF EXISTS ONLY public.dim_stadium_names_history DROP CONSTRAINT IF EXISTS dim_stadium_names_history_stadium_id_stadium_name_valid_fro_key;
ALTER TABLE IF EXISTS ONLY public.dim_stadium_names_history DROP CONSTRAINT IF EXISTS dim_stadium_names_history_pkey;
ALTER TABLE IF EXISTS ONLY public.dim_stadium_master DROP CONSTRAINT IF EXISTS dim_stadium_master_pkey;
ALTER TABLE IF EXISTS ONLY public.dim_referee DROP CONSTRAINT IF EXISTS dim_referee_pkey;
ALTER TABLE IF EXISTS ONLY public.dim_referee DROP CONSTRAINT IF EXISTS dim_referee_id_sofascore_key;
ALTER TABLE IF EXISTS ONLY public.dim_player DROP CONSTRAINT IF EXISTS dim_player_pkey;
ALTER TABLE IF EXISTS ONLY public.dim_match DROP CONSTRAINT IF EXISTS dim_match_pkey;
ALTER TABLE IF EXISTS ONLY public.dim_competition DROP CONSTRAINT IF EXISTS dim_competition_pkey;
ALTER TABLE IF EXISTS ONLY public.bridge_team_season_stadium DROP CONSTRAINT IF EXISTS bridge_team_season_stadium_pkey;
ALTER TABLE IF EXISTS public.player_review ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.fact_transfers ALTER COLUMN transfer_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.fact_shots ALTER COLUMN shot_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.fact_player_match_stats ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.fact_market_value ALTER COLUMN mv_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.fact_injuries ALTER COLUMN injury_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.fact_formations ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.fact_events ALTER COLUMN event_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.dim_team ALTER COLUMN canonical_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.dim_stadium_names_history ALTER COLUMN name_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.dim_stadium_master ALTER COLUMN stadium_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.dim_stadium ALTER COLUMN stadium_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.dim_referee ALTER COLUMN referee_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.dim_player ALTER COLUMN canonical_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.dim_match ALTER COLUMN match_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.dim_competition ALTER COLUMN canonical_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.bridge_team_season_stadium ALTER COLUMN bridge_id DROP DEFAULT;
DROP VIEW IF EXISTS public.vw_team_home_stadium;
DROP VIEW IF EXISTS public.vw_match_neutral_venue;
DROP SEQUENCE IF EXISTS public.player_review_id_seq;
DROP TABLE IF EXISTS public.player_review;
DROP SEQUENCE IF EXISTS public.fact_transfers_transfer_id_seq;
DROP TABLE IF EXISTS public.fact_transfers;
DROP SEQUENCE IF EXISTS public.fact_shots_shot_id_seq;
DROP TABLE IF EXISTS public.fact_shots;
DROP SEQUENCE IF EXISTS public.fact_player_match_stats_id_seq;
DROP TABLE IF EXISTS public.fact_player_match_stats;
DROP SEQUENCE IF EXISTS public.fact_market_value_mv_id_seq;
DROP TABLE IF EXISTS public.fact_market_value;
DROP SEQUENCE IF EXISTS public.fact_injuries_injury_id_seq;
DROP TABLE IF EXISTS public.fact_injuries;
DROP SEQUENCE IF EXISTS public.fact_formations_id_seq;
DROP TABLE IF EXISTS public.fact_formations;
DROP SEQUENCE IF EXISTS public.fact_events_event_id_seq;
DROP TABLE IF EXISTS public.fact_events;
DROP SEQUENCE IF EXISTS public.dim_team_canonical_id_seq;
DROP TABLE IF EXISTS public.dim_team;
DROP SEQUENCE IF EXISTS public.dim_stadium_stadium_id_seq;
DROP SEQUENCE IF EXISTS public.dim_stadium_names_history_name_id_seq;
DROP TABLE IF EXISTS public.dim_stadium_names_history;
DROP SEQUENCE IF EXISTS public.dim_stadium_master_stadium_id_seq;
DROP TABLE IF EXISTS public.dim_stadium_master;
DROP TABLE IF EXISTS public.dim_stadium;
DROP SEQUENCE IF EXISTS public.dim_referee_referee_id_seq;
DROP TABLE IF EXISTS public.dim_referee;
DROP SEQUENCE IF EXISTS public.dim_player_canonical_id_seq;
DROP TABLE IF EXISTS public.dim_player;
DROP SEQUENCE IF EXISTS public.dim_match_match_id_seq;
DROP TABLE IF EXISTS public.dim_match;
DROP SEQUENCE IF EXISTS public.dim_competition_canonical_id_seq;
DROP TABLE IF EXISTS public.dim_competition;
DROP SEQUENCE IF EXISTS public.bridge_team_season_stadium_bridge_id_seq;
DROP TABLE IF EXISTS public.bridge_team_season_stadium;
DROP SCHEMA IF EXISTS public;
--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA public;


--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA public IS 'standard public schema';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: bridge_team_season_stadium; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bridge_team_season_stadium (
    bridge_id integer NOT NULL,
    canonical_team_id integer NOT NULL,
    stadium_id integer NOT NULL,
    season_start character varying(9) NOT NULL,
    season_end character varying(9) NOT NULL,
    is_home boolean DEFAULT true,
    usage_context character varying(20) DEFAULT 'primary'::character varying NOT NULL
);


--
-- Name: bridge_team_season_stadium_bridge_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.bridge_team_season_stadium_bridge_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: bridge_team_season_stadium_bridge_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.bridge_team_season_stadium_bridge_id_seq OWNED BY public.bridge_team_season_stadium.bridge_id;


--
-- Name: dim_competition; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_competition (
    canonical_id integer NOT NULL,
    canonical_name character varying(150) NOT NULL,
    id_sofascore integer,
    id_understat character varying(50),
    id_transfermarkt character varying(50),
    id_statsbomb character varying(50),
    id_whoscored integer,
    created_at timestamp without time zone DEFAULT now(),
    country character varying(100),
    country_code character(2)
);


--
-- Name: dim_competition_canonical_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dim_competition_canonical_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_competition_canonical_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dim_competition_canonical_id_seq OWNED BY public.dim_competition.canonical_id;


--
-- Name: dim_match; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_match (
    match_id integer NOT NULL,
    match_date date,
    competition character varying(100),
    season character varying(20),
    home_team_id integer,
    away_team_id integer,
    home_score smallint,
    away_score smallint,
    data_source character varying(50),
    id_sofascore integer,
    id_understat integer,
    id_statsbomb character varying(50),
    id_whoscored integer,
    competition_id integer,
    attendance integer,
    temperature_c numeric(4,1),
    humidity_pct smallint,
    precipitation_mm numeric(5,1),
    wind_speed_kmh numeric(5,1),
    weather_code smallint,
    referee_id integer,
    venue_name character varying(200),
    manager_home character varying(150),
    manager_away character varying(150),
    ht_score character varying(10),
    ft_score character varying(10),
    match_stadium_id integer,
    match_venue_source character varying(32)
);


--
-- Name: dim_match_match_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dim_match_match_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_match_match_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dim_match_match_id_seq OWNED BY public.dim_match.match_id;


--
-- Name: dim_player; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_player (
    canonical_id integer NOT NULL,
    canonical_name character varying(150) NOT NULL,
    nationality character varying(80),
    birth_date date,
    "position" character varying(50),
    id_sofascore integer,
    id_understat integer,
    id_transfermarkt integer,
    id_statsbomb character varying(50),
    id_whoscored integer,
    created_at timestamp without time zone DEFAULT now(),
    photo_url text
);


--
-- Name: dim_player_canonical_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dim_player_canonical_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_player_canonical_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dim_player_canonical_id_seq OWNED BY public.dim_player.canonical_id;


--
-- Name: dim_referee; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_referee (
    referee_id integer NOT NULL,
    canonical_name character varying(150) NOT NULL,
    country character varying(80),
    id_sofascore integer,
    id_whoscored integer,
    id_transfermarkt integer,
    data_source character varying(50) DEFAULT 'sofascore'::character varying,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: dim_referee_referee_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dim_referee_referee_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_referee_referee_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dim_referee_referee_id_seq OWNED BY public.dim_referee.referee_id;


--
-- Name: dim_stadium; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_stadium (
    stadium_id integer NOT NULL,
    canonical_team_id integer,
    id_transfermarkt_team integer NOT NULL,
    team_slug character varying(150),
    valid_from_season character varying(20) NOT NULL,
    valid_to_season character varying(20) NOT NULL,
    stadium_name character varying(200),
    capacity integer,
    seats_total integer,
    built_year smallint,
    owner character varying(200),
    operator character varying(200),
    address character varying(300),
    city character varying(120),
    country character varying(80),
    surface character varying(80),
    architect character varying(200),
    tm_url character varying(400),
    wikidata_qid character varying(20),
    latitude numeric(9,6),
    longitude numeric(9,6),
    altitude_m integer,
    timezone character varying(64),
    wikipedia_url character varying(500),
    image_url text,
    data_hash character(40),
    data_source character varying(50) DEFAULT 'transfermarkt'::character varying,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    vip_boxes smallint,
    construction_cost character varying(120),
    is_current boolean DEFAULT true,
    seats_vip integer,
    CONSTRAINT dim_stadium_check CHECK (((valid_from_season)::text <= (valid_to_season)::text))
);


--
-- Name: dim_stadium_master; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_stadium_master (
    stadium_id integer NOT NULL,
    canonical_name character varying(200) NOT NULL,
    capacity integer,
    seats_total integer,
    surface character varying(50),
    city character varying(100),
    country character varying(100),
    latitude numeric(9,6),
    longitude numeric(9,6),
    altitude_m integer,
    timezone character varying(50),
    built_year integer,
    wikidata_qid character varying(20),
    cloudinary_public_id character varying(255),
    is_current boolean DEFAULT true,
    owner character varying(150),
    address character varying(255),
    tm_url text,
    wikipedia_url text,
    image_url text,
    seats_vip integer
);


--
-- Name: dim_stadium_master_stadium_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dim_stadium_master_stadium_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_stadium_master_stadium_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dim_stadium_master_stadium_id_seq OWNED BY public.dim_stadium_master.stadium_id;


--
-- Name: dim_stadium_names_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_stadium_names_history (
    name_id integer NOT NULL,
    stadium_id integer NOT NULL,
    stadium_name character varying(200) NOT NULL,
    valid_from_year integer,
    valid_to_year integer,
    is_current boolean DEFAULT false
);


--
-- Name: dim_stadium_names_history_name_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dim_stadium_names_history_name_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_stadium_names_history_name_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dim_stadium_names_history_name_id_seq OWNED BY public.dim_stadium_names_history.name_id;


--
-- Name: dim_stadium_stadium_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dim_stadium_stadium_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_stadium_stadium_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dim_stadium_stadium_id_seq OWNED BY public.dim_stadium.stadium_id;


--
-- Name: dim_team; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_team (
    canonical_id integer NOT NULL,
    canonical_name character varying(150) NOT NULL,
    country character varying(80),
    id_sofascore integer,
    id_understat integer,
    id_statsbomb character varying(50),
    id_whoscored integer,
    id_transfermarkt integer,
    created_at timestamp without time zone DEFAULT now(),
    home_stadium_master_id integer
);


--
-- Name: COLUMN dim_team.home_stadium_master_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.dim_team.home_stadium_master_id IS 'FK a dim_stadium_master: sede habitual actual (última temporada en bridge).';


--
-- Name: dim_team_canonical_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dim_team_canonical_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_team_canonical_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dim_team_canonical_id_seq OWNED BY public.dim_team.canonical_id;


--
-- Name: fact_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fact_events (
    event_id integer NOT NULL,
    match_id integer NOT NULL,
    player_id integer NOT NULL,
    team_id integer NOT NULL,
    event_type character varying(50),
    minute smallint,
    second smallint,
    x numeric(7,4),
    y numeric(7,4),
    end_x numeric(7,4),
    end_y numeric(7,4),
    outcome character varying(50),
    data_source character varying(30),
    qualifiers jsonb,
    whoscored_event_id bigint,
    body_part character varying(20),
    goal_mouth_y numeric(7,2),
    goal_mouth_z numeric(7,2),
    angle numeric(7,2),
    length numeric(7,2),
    pass_end_x numeric(7,4),
    pass_end_y numeric(7,4),
    is_assisted boolean,
    is_individual_play boolean,
    is_big_chance boolean,
    is_key_pass boolean,
    is_fast_break boolean,
    shot_zone character varying(30),
    shot_placement character varying(20),
    situation_detail character varying(30),
    blocked_x numeric(7,4),
    blocked_y numeric(7,4),
    related_player_id integer
);


--
-- Name: fact_events_event_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.fact_events_event_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fact_events_event_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.fact_events_event_id_seq OWNED BY public.fact_events.event_id;


--
-- Name: fact_formations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fact_formations (
    id integer NOT NULL,
    match_id integer NOT NULL,
    team_id integer NOT NULL,
    side character varying(4) NOT NULL,
    formation_name character varying(20) NOT NULL,
    captain_player_id integer,
    start_minute smallint DEFAULT 0 NOT NULL,
    end_minute smallint,
    data_source character varying(30) DEFAULT 'whoscored'::character varying,
    created_at timestamp without time zone DEFAULT now()
);


--
-- Name: fact_formations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.fact_formations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fact_formations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.fact_formations_id_seq OWNED BY public.fact_formations.id;


--
-- Name: fact_injuries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fact_injuries (
    injury_id integer NOT NULL,
    player_id integer NOT NULL,
    season character varying(20),
    injury_type character varying(200),
    date_from date,
    date_until date,
    days_absent integer,
    matches_missed smallint,
    club_name character varying(200),
    club_id_tm integer,
    club_slug character varying(150)
);


--
-- Name: fact_injuries_injury_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.fact_injuries_injury_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fact_injuries_injury_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.fact_injuries_injury_id_seq OWNED BY public.fact_injuries.injury_id;


--
-- Name: fact_market_value; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fact_market_value (
    mv_id integer NOT NULL,
    player_id integer NOT NULL,
    value_date date NOT NULL,
    market_value bigint NOT NULL,
    market_value_raw character varying(100),
    club_id integer,
    club_name character varying(200),
    id_tm_club integer,
    created_at timestamp without time zone DEFAULT now()
);


--
-- Name: fact_market_value_mv_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.fact_market_value_mv_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fact_market_value_mv_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.fact_market_value_mv_id_seq OWNED BY public.fact_market_value.mv_id;


--
-- Name: fact_player_match_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fact_player_match_stats (
    id integer NOT NULL,
    match_id integer NOT NULL,
    player_id integer NOT NULL,
    team_id integer NOT NULL,
    is_starter boolean,
    "position" character varying(10),
    shirt_no smallint,
    age smallint,
    height_cm smallint,
    weight_kg smallint,
    is_man_of_the_match boolean,
    subbed_in_minute smallint,
    subbed_out_minute smallint,
    rating numeric(4,2),
    passes_total smallint,
    passes_accurate smallint,
    passes_key smallint,
    pass_success_pct numeric(5,2),
    shots_total smallint,
    shots_on_target smallint,
    shots_off_target smallint,
    shots_blocked smallint,
    dribbles_attempted smallint,
    dribbles_won smallint,
    dribbles_lost smallint,
    tackles_total smallint,
    tackles_successful smallint,
    interceptions smallint,
    clearances smallint,
    aerials_total smallint,
    aerials_won smallint,
    fouls_committed smallint,
    was_dribbled_past smallint,
    dispossessed smallint,
    touches smallint,
    offsides_caught smallint,
    corners_total smallint,
    corners_accurate smallint,
    throw_ins_total smallint,
    throw_ins_accurate smallint,
    saves_total smallint,
    saves_parried_safe smallint,
    saves_parried_danger smallint,
    claims_high smallint,
    collected smallint,
    possession_pct numeric(5,2),
    data_source character varying(30) DEFAULT 'whoscored'::character varying,
    created_at timestamp without time zone DEFAULT now()
);


--
-- Name: fact_player_match_stats_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.fact_player_match_stats_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fact_player_match_stats_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.fact_player_match_stats_id_seq OWNED BY public.fact_player_match_stats.id;


--
-- Name: fact_shots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fact_shots (
    shot_id integer NOT NULL,
    match_id integer NOT NULL,
    player_id integer NOT NULL,
    team_id integer NOT NULL,
    minute smallint,
    x numeric(7,4),
    y numeric(7,4),
    xg numeric(7,4),
    result character varying(30),
    shot_type character varying(30),
    situation character varying(50),
    data_source character varying(30)
);


--
-- Name: fact_shots_shot_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.fact_shots_shot_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fact_shots_shot_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.fact_shots_shot_id_seq OWNED BY public.fact_shots.shot_id;


--
-- Name: fact_transfers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fact_transfers (
    transfer_id integer NOT NULL,
    player_id integer NOT NULL,
    season character varying(20),
    transfer_date date,
    from_team_id integer,
    from_team_name character varying(200),
    to_team_id integer,
    to_team_name character varying(200),
    fee_raw character varying(100),
    fee_euros bigint,
    fee_currency character varying(10) DEFAULT 'â‚¬'::character varying,
    transfer_type character varying(50),
    is_loan boolean DEFAULT false,
    id_tm_from_team integer,
    id_tm_to_team integer,
    created_at timestamp without time zone DEFAULT now()
);


--
-- Name: fact_transfers_transfer_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.fact_transfers_transfer_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fact_transfers_transfer_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.fact_transfers_transfer_id_seq OWNED BY public.fact_transfers.transfer_id;


--
-- Name: player_review; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.player_review (
    id integer NOT NULL,
    source_name character varying(150) NOT NULL,
    source_system character varying(50) NOT NULL,
    source_id character varying(50) NOT NULL,
    suggested_canonical_id integer,
    similarity_score smallint,
    resolved boolean DEFAULT false,
    canonical_id_assigned integer,
    created_at timestamp without time zone DEFAULT now(),
    reviewed_at timestamp without time zone,
    source_team_id character varying(50),
    source_team_name character varying(150),
    competition character varying(100),
    season character varying(20)
);


--
-- Name: player_review_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.player_review_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: player_review_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.player_review_id_seq OWNED BY public.player_review.id;


--
-- Name: vw_match_neutral_venue; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.vw_match_neutral_venue AS
 WITH neutral_comps AS (
         SELECT dim_competition.canonical_id,
            dim_competition.canonical_name
           FROM public.dim_competition
          WHERE ((dim_competition.canonical_name)::text = ANY (ARRAY[('Champions League'::character varying)::text, ('Europa League'::character varying)::text, ('Europa Conference League'::character varying)::text, ('FIFA World Cup'::character varying)::text, ('European Championship'::character varying)::text, ('Copa America'::character varying)::text, ('FIFA Club World Cup'::character varying)::text, ('UEFA Women''s EURO'::character varying)::text, ('FIFA Women''s World Cup'::character varying)::text]))
        )
 SELECT match_id,
    match_date,
    competition_id,
    home_team_id,
    away_team_id,
    ((competition_id IN ( SELECT neutral_comps.canonical_id
           FROM neutral_comps)) AND (EXISTS ( SELECT 1
           FROM neutral_comps nc
          WHERE ((nc.canonical_id = m.competition_id) AND ((nc.canonical_name)::text = ANY (ARRAY[('FIFA World Cup'::character varying)::text, ('European Championship'::character varying)::text, ('Copa America'::character varying)::text, ('FIFA Club World Cup'::character varying)::text, ('UEFA Women''s EURO'::character varying)::text, ('FIFA Women''s World Cup'::character varying)::text])))))) AS is_neutral_candidate
   FROM public.dim_match m;


--
-- Name: vw_team_home_stadium; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.vw_team_home_stadium AS
 SELECT dt.canonical_id,
    dt.canonical_name,
    dt.country,
    dt.home_stadium_master_id,
    sm.canonical_name AS stadium_name,
    sm.wikidata_qid,
    sm.city,
    sm.country AS stadium_country,
    sm.capacity,
    sm.latitude,
    sm.longitude
   FROM (public.dim_team dt
     LEFT JOIN public.dim_stadium_master sm ON ((sm.stadium_id = dt.home_stadium_master_id)));


--
-- Name: bridge_team_season_stadium bridge_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bridge_team_season_stadium ALTER COLUMN bridge_id SET DEFAULT nextval('public.bridge_team_season_stadium_bridge_id_seq'::regclass);


--
-- Name: dim_competition canonical_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_competition ALTER COLUMN canonical_id SET DEFAULT nextval('public.dim_competition_canonical_id_seq'::regclass);


--
-- Name: dim_match match_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_match ALTER COLUMN match_id SET DEFAULT nextval('public.dim_match_match_id_seq'::regclass);


--
-- Name: dim_player canonical_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_player ALTER COLUMN canonical_id SET DEFAULT nextval('public.dim_player_canonical_id_seq'::regclass);


--
-- Name: dim_referee referee_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_referee ALTER COLUMN referee_id SET DEFAULT nextval('public.dim_referee_referee_id_seq'::regclass);


--
-- Name: dim_stadium stadium_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_stadium ALTER COLUMN stadium_id SET DEFAULT nextval('public.dim_stadium_stadium_id_seq'::regclass);


--
-- Name: dim_stadium_master stadium_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_stadium_master ALTER COLUMN stadium_id SET DEFAULT nextval('public.dim_stadium_master_stadium_id_seq'::regclass);


--
-- Name: dim_stadium_names_history name_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_stadium_names_history ALTER COLUMN name_id SET DEFAULT nextval('public.dim_stadium_names_history_name_id_seq'::regclass);


--
-- Name: dim_team canonical_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_team ALTER COLUMN canonical_id SET DEFAULT nextval('public.dim_team_canonical_id_seq'::regclass);


--
-- Name: fact_events event_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_events ALTER COLUMN event_id SET DEFAULT nextval('public.fact_events_event_id_seq'::regclass);


--
-- Name: fact_formations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_formations ALTER COLUMN id SET DEFAULT nextval('public.fact_formations_id_seq'::regclass);


--
-- Name: fact_injuries injury_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_injuries ALTER COLUMN injury_id SET DEFAULT nextval('public.fact_injuries_injury_id_seq'::regclass);


--
-- Name: fact_market_value mv_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_market_value ALTER COLUMN mv_id SET DEFAULT nextval('public.fact_market_value_mv_id_seq'::regclass);


--
-- Name: fact_player_match_stats id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_player_match_stats ALTER COLUMN id SET DEFAULT nextval('public.fact_player_match_stats_id_seq'::regclass);


--
-- Name: fact_shots shot_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_shots ALTER COLUMN shot_id SET DEFAULT nextval('public.fact_shots_shot_id_seq'::regclass);


--
-- Name: fact_transfers transfer_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_transfers ALTER COLUMN transfer_id SET DEFAULT nextval('public.fact_transfers_transfer_id_seq'::regclass);


--
-- Name: player_review id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_review ALTER COLUMN id SET DEFAULT nextval('public.player_review_id_seq'::regclass);


--
-- Name: bridge_team_season_stadium bridge_team_season_stadium_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bridge_team_season_stadium
    ADD CONSTRAINT bridge_team_season_stadium_pkey PRIMARY KEY (bridge_id);


--
-- Name: dim_competition dim_competition_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_competition
    ADD CONSTRAINT dim_competition_pkey PRIMARY KEY (canonical_id);


--
-- Name: dim_match dim_match_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_match
    ADD CONSTRAINT dim_match_pkey PRIMARY KEY (match_id);


--
-- Name: dim_player dim_player_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_player
    ADD CONSTRAINT dim_player_pkey PRIMARY KEY (canonical_id);


--
-- Name: dim_referee dim_referee_id_sofascore_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_referee
    ADD CONSTRAINT dim_referee_id_sofascore_key UNIQUE (id_sofascore);


--
-- Name: dim_referee dim_referee_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_referee
    ADD CONSTRAINT dim_referee_pkey PRIMARY KEY (referee_id);


--
-- Name: dim_stadium_master dim_stadium_master_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_stadium_master
    ADD CONSTRAINT dim_stadium_master_pkey PRIMARY KEY (stadium_id);


--
-- Name: dim_stadium_names_history dim_stadium_names_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_stadium_names_history
    ADD CONSTRAINT dim_stadium_names_history_pkey PRIMARY KEY (name_id);


--
-- Name: dim_stadium_names_history dim_stadium_names_history_stadium_id_stadium_name_valid_fro_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_stadium_names_history
    ADD CONSTRAINT dim_stadium_names_history_stadium_id_stadium_name_valid_fro_key UNIQUE (stadium_id, stadium_name, valid_from_year, valid_to_year);


--
-- Name: dim_stadium dim_stadium_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_stadium
    ADD CONSTRAINT dim_stadium_pkey PRIMARY KEY (stadium_id);


--
-- Name: dim_team dim_team_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_team
    ADD CONSTRAINT dim_team_pkey PRIMARY KEY (canonical_id);


--
-- Name: fact_events fact_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_events
    ADD CONSTRAINT fact_events_pkey PRIMARY KEY (event_id);


--
-- Name: fact_formations fact_formations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_formations
    ADD CONSTRAINT fact_formations_pkey PRIMARY KEY (id);


--
-- Name: fact_injuries fact_injuries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_injuries
    ADD CONSTRAINT fact_injuries_pkey PRIMARY KEY (injury_id);


--
-- Name: fact_market_value fact_market_value_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_market_value
    ADD CONSTRAINT fact_market_value_pkey PRIMARY KEY (mv_id);


--
-- Name: fact_player_match_stats fact_player_match_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_player_match_stats
    ADD CONSTRAINT fact_player_match_stats_pkey PRIMARY KEY (id);


--
-- Name: fact_shots fact_shots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_shots
    ADD CONSTRAINT fact_shots_pkey PRIMARY KEY (shot_id);


--
-- Name: fact_transfers fact_transfers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_transfers
    ADD CONSTRAINT fact_transfers_pkey PRIMARY KEY (transfer_id);


--
-- Name: player_review player_review_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_review
    ADD CONSTRAINT player_review_pkey PRIMARY KEY (id);


--
-- Name: idx_dim_competition_name_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_dim_competition_name_unique ON public.dim_competition USING btree (canonical_name);


--
-- Name: idx_dim_competition_sofascore_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_dim_competition_sofascore_unique ON public.dim_competition USING btree (id_sofascore) WHERE (id_sofascore IS NOT NULL);


--
-- Name: idx_dim_competition_statsbomb_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_dim_competition_statsbomb_unique ON public.dim_competition USING btree (id_statsbomb) WHERE (id_statsbomb IS NOT NULL);


--
-- Name: idx_dim_competition_transfermarkt_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_dim_competition_transfermarkt_unique ON public.dim_competition USING btree (id_transfermarkt) WHERE (id_transfermarkt IS NOT NULL);


--
-- Name: idx_dim_competition_understat_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_dim_competition_understat_unique ON public.dim_competition USING btree (id_understat) WHERE (id_understat IS NOT NULL);


--
-- Name: idx_dim_competition_whoscored_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_dim_competition_whoscored_unique ON public.dim_competition USING btree (id_whoscored) WHERE (id_whoscored IS NOT NULL);


--
-- Name: idx_dim_match_competition_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dim_match_competition_id ON public.dim_match USING btree (competition_id);


--
-- Name: idx_events_assisted; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_assisted ON public.fact_events USING btree (is_assisted) WHERE (is_assisted IS TRUE);


--
-- Name: idx_events_big_chance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_big_chance ON public.fact_events USING btree (is_big_chance) WHERE (is_big_chance IS TRUE);


--
-- Name: idx_events_body_part; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_body_part ON public.fact_events USING btree (body_part) WHERE (body_part IS NOT NULL);


--
-- Name: idx_events_match; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_match ON public.fact_events USING btree (match_id);


--
-- Name: idx_events_player; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_player ON public.fact_events USING btree (player_id);


--
-- Name: idx_events_shot_zone; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_shot_zone ON public.fact_events USING btree (shot_zone) WHERE (shot_zone IS NOT NULL);


--
-- Name: idx_events_team; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_team ON public.fact_events USING btree (team_id);


--
-- Name: idx_events_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_type ON public.fact_events USING btree (event_type);


--
-- Name: idx_fact_events_qualifiers; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_fact_events_qualifiers ON public.fact_events USING gin (qualifiers);


--
-- Name: idx_fact_events_ws_event_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_fact_events_ws_event_id ON public.fact_events USING btree (whoscored_event_id) WHERE (whoscored_event_id IS NOT NULL);


--
-- Name: idx_formations_match; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_formations_match ON public.fact_formations USING btree (match_id);


--
-- Name: idx_formations_team; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_formations_team ON public.fact_formations USING btree (team_id);


--
-- Name: idx_injuries_player; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_injuries_player ON public.fact_injuries USING btree (player_id);


--
-- Name: idx_market_value_club; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_market_value_club ON public.fact_market_value USING btree (club_id);


--
-- Name: idx_market_value_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_market_value_date ON public.fact_market_value USING btree (value_date);


--
-- Name: idx_market_value_player; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_market_value_player ON public.fact_market_value USING btree (player_id);


--
-- Name: idx_match_away_team; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_match_away_team ON public.dim_match USING btree (away_team_id);


--
-- Name: idx_match_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_match_date ON public.dim_match USING btree (match_date);


--
-- Name: idx_match_home_team; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_match_home_team ON public.dim_match USING btree (home_team_id);


--
-- Name: idx_match_match_stadium; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_match_match_stadium ON public.dim_match USING btree (match_stadium_id);


--
-- Name: idx_match_referee; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_match_referee ON public.dim_match USING btree (referee_id) WHERE (referee_id IS NOT NULL);


--
-- Name: idx_player_review_assigned; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_player_review_assigned ON public.player_review USING btree (canonical_id_assigned);


--
-- Name: idx_player_review_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_player_review_source ON public.player_review USING btree (source_system, source_id);


--
-- Name: idx_player_review_suggested; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_player_review_suggested ON public.player_review USING btree (suggested_canonical_id);


--
-- Name: idx_player_review_unresolved; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_player_review_unresolved ON public.player_review USING btree (resolved) WHERE (resolved IS FALSE);


--
-- Name: idx_pms_match; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pms_match ON public.fact_player_match_stats USING btree (match_id);


--
-- Name: idx_pms_player; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pms_player ON public.fact_player_match_stats USING btree (player_id);


--
-- Name: idx_pms_team; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pms_team ON public.fact_player_match_stats USING btree (team_id);


--
-- Name: idx_referee_name_lower; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_referee_name_lower ON public.dim_referee USING btree (lower((canonical_name)::text));


--
-- Name: idx_shots_match; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_shots_match ON public.fact_shots USING btree (match_id);


--
-- Name: idx_shots_player; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_shots_player ON public.fact_shots USING btree (player_id);


--
-- Name: idx_shots_team; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_shots_team ON public.fact_shots USING btree (team_id);


--
-- Name: idx_stadium_data_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stadium_data_hash ON public.dim_stadium USING btree (id_transfermarkt_team, data_hash);


--
-- Name: idx_stadium_latlon; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stadium_latlon ON public.dim_stadium USING btree (latitude, longitude) WHERE ((latitude IS NOT NULL) AND (longitude IS NOT NULL));


--
-- Name: idx_stadium_name_lower; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stadium_name_lower ON public.dim_stadium USING btree (lower((stadium_name)::text));


--
-- Name: idx_stadium_team; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stadium_team ON public.dim_stadium USING btree (canonical_team_id);


--
-- Name: idx_stadium_team_tm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stadium_team_tm ON public.dim_stadium USING btree (id_transfermarkt_team);


--
-- Name: idx_stadium_wikidata_qid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stadium_wikidata_qid ON public.dim_stadium USING btree (wikidata_qid) WHERE (wikidata_qid IS NOT NULL);


--
-- Name: idx_team_home_stadium; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_home_stadium ON public.dim_team USING btree (home_stadium_master_id) WHERE (home_stadium_master_id IS NOT NULL);


--
-- Name: idx_transfers_from_team; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_transfers_from_team ON public.fact_transfers USING btree (from_team_id);


--
-- Name: idx_transfers_player; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_transfers_player ON public.fact_transfers USING btree (player_id);


--
-- Name: idx_transfers_season; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_transfers_season ON public.fact_transfers USING btree (season);


--
-- Name: idx_transfers_to_team; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_transfers_to_team ON public.fact_transfers USING btree (to_team_id);


--
-- Name: ux_bridge_team_stadium_season_ctx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_bridge_team_stadium_season_ctx ON public.bridge_team_season_stadium USING btree (canonical_team_id, stadium_id, season_start, season_end, usage_context);


--
-- Name: ux_events_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_events_unique ON public.fact_events USING btree (match_id, player_id, event_type, minute, COALESCE((second)::integer, '-1'::integer), COALESCE(x, '-1.0'::numeric), COALESCE(y, '-1.0'::numeric), data_source);


--
-- Name: ux_formations_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_formations_unique ON public.fact_formations USING btree (match_id, team_id, start_minute, data_source);


--
-- Name: ux_injuries_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_injuries_unique ON public.fact_injuries USING btree (player_id, season, injury_type, date_from);


--
-- Name: ux_market_value_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_market_value_unique ON public.fact_market_value USING btree (player_id, value_date);


--
-- Name: ux_match_sofascore; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_match_sofascore ON public.dim_match USING btree (id_sofascore) WHERE (id_sofascore IS NOT NULL);


--
-- Name: ux_match_statsbomb; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_match_statsbomb ON public.dim_match USING btree (id_statsbomb) WHERE (id_statsbomb IS NOT NULL);


--
-- Name: ux_match_understat; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_match_understat ON public.dim_match USING btree (id_understat) WHERE (id_understat IS NOT NULL);


--
-- Name: ux_match_whoscored; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_match_whoscored ON public.dim_match USING btree (id_whoscored) WHERE (id_whoscored IS NOT NULL);


--
-- Name: ux_player_match_stats; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_player_match_stats ON public.fact_player_match_stats USING btree (match_id, player_id, data_source);


--
-- Name: ux_player_sofascore; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_player_sofascore ON public.dim_player USING btree (id_sofascore) WHERE (id_sofascore IS NOT NULL);


--
-- Name: ux_player_statsbomb; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_player_statsbomb ON public.dim_player USING btree (id_statsbomb) WHERE (id_statsbomb IS NOT NULL);


--
-- Name: ux_player_transfermkt; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_player_transfermkt ON public.dim_player USING btree (id_transfermarkt) WHERE (id_transfermarkt IS NOT NULL);


--
-- Name: ux_player_understat; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_player_understat ON public.dim_player USING btree (id_understat) WHERE (id_understat IS NOT NULL);


--
-- Name: ux_player_whoscored; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_player_whoscored ON public.dim_player USING btree (id_whoscored) WHERE (id_whoscored IS NOT NULL);


--
-- Name: ux_referee_whoscored; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_referee_whoscored ON public.dim_referee USING btree (id_whoscored) WHERE (id_whoscored IS NOT NULL);


--
-- Name: ux_shots_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_shots_unique ON public.fact_shots USING btree (match_id, player_id, minute, x, y, data_source);


--
-- Name: ux_stadium_team_validfrom; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_stadium_team_validfrom ON public.dim_stadium USING btree (id_transfermarkt_team, valid_from_season);


--
-- Name: ux_team_sofascore; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_team_sofascore ON public.dim_team USING btree (id_sofascore) WHERE (id_sofascore IS NOT NULL);


--
-- Name: ux_team_statsbomb; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_team_statsbomb ON public.dim_team USING btree (id_statsbomb) WHERE (id_statsbomb IS NOT NULL);


--
-- Name: ux_team_transfermarkt; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_team_transfermarkt ON public.dim_team USING btree (id_transfermarkt) WHERE (id_transfermarkt IS NOT NULL);


--
-- Name: ux_team_understat; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_team_understat ON public.dim_team USING btree (id_understat) WHERE (id_understat IS NOT NULL);


--
-- Name: ux_team_whoscored; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_team_whoscored ON public.dim_team USING btree (id_whoscored) WHERE (id_whoscored IS NOT NULL);


--
-- Name: ux_transfers_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_transfers_unique ON public.fact_transfers USING btree (player_id, season, transfer_date, COALESCE(id_tm_from_team, '-1'::integer), COALESCE(id_tm_to_team, '-1'::integer));


--
-- Name: bridge_team_season_stadium bridge_team_season_stadium_canonical_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bridge_team_season_stadium
    ADD CONSTRAINT bridge_team_season_stadium_canonical_team_id_fkey FOREIGN KEY (canonical_team_id) REFERENCES public.dim_team(canonical_id);


--
-- Name: bridge_team_season_stadium bridge_team_season_stadium_stadium_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bridge_team_season_stadium
    ADD CONSTRAINT bridge_team_season_stadium_stadium_id_fkey FOREIGN KEY (stadium_id) REFERENCES public.dim_stadium_master(stadium_id);


--
-- Name: dim_match dim_match_away_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_match
    ADD CONSTRAINT dim_match_away_team_id_fkey FOREIGN KEY (away_team_id) REFERENCES public.dim_team(canonical_id);


--
-- Name: dim_match dim_match_competition_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_match
    ADD CONSTRAINT dim_match_competition_id_fkey FOREIGN KEY (competition_id) REFERENCES public.dim_competition(canonical_id);


--
-- Name: dim_match dim_match_home_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_match
    ADD CONSTRAINT dim_match_home_team_id_fkey FOREIGN KEY (home_team_id) REFERENCES public.dim_team(canonical_id);


--
-- Name: dim_match dim_match_match_stadium_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_match
    ADD CONSTRAINT dim_match_match_stadium_id_fkey FOREIGN KEY (match_stadium_id) REFERENCES public.dim_stadium(stadium_id) ON DELETE SET NULL;


--
-- Name: dim_match dim_match_referee_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_match
    ADD CONSTRAINT dim_match_referee_id_fkey FOREIGN KEY (referee_id) REFERENCES public.dim_referee(referee_id) ON DELETE SET NULL;


--
-- Name: dim_stadium dim_stadium_canonical_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_stadium
    ADD CONSTRAINT dim_stadium_canonical_team_id_fkey FOREIGN KEY (canonical_team_id) REFERENCES public.dim_team(canonical_id);


--
-- Name: dim_stadium_names_history dim_stadium_names_history_stadium_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_stadium_names_history
    ADD CONSTRAINT dim_stadium_names_history_stadium_id_fkey FOREIGN KEY (stadium_id) REFERENCES public.dim_stadium_master(stadium_id);


--
-- Name: dim_team dim_team_home_stadium_master_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_team
    ADD CONSTRAINT dim_team_home_stadium_master_id_fkey FOREIGN KEY (home_stadium_master_id) REFERENCES public.dim_stadium_master(stadium_id);


--
-- Name: fact_events fact_events_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_events
    ADD CONSTRAINT fact_events_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.dim_match(match_id);


--
-- Name: fact_events fact_events_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_events
    ADD CONSTRAINT fact_events_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.dim_player(canonical_id);


--
-- Name: fact_events fact_events_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_events
    ADD CONSTRAINT fact_events_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.dim_team(canonical_id);


--
-- Name: fact_formations fact_formations_captain_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_formations
    ADD CONSTRAINT fact_formations_captain_player_id_fkey FOREIGN KEY (captain_player_id) REFERENCES public.dim_player(canonical_id);


--
-- Name: fact_formations fact_formations_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_formations
    ADD CONSTRAINT fact_formations_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.dim_match(match_id);


--
-- Name: fact_formations fact_formations_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_formations
    ADD CONSTRAINT fact_formations_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.dim_team(canonical_id);


--
-- Name: fact_injuries fact_injuries_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_injuries
    ADD CONSTRAINT fact_injuries_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.dim_player(canonical_id);


--
-- Name: fact_market_value fact_market_value_club_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_market_value
    ADD CONSTRAINT fact_market_value_club_id_fkey FOREIGN KEY (club_id) REFERENCES public.dim_team(canonical_id);


--
-- Name: fact_market_value fact_market_value_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_market_value
    ADD CONSTRAINT fact_market_value_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.dim_player(canonical_id);


--
-- Name: fact_player_match_stats fact_player_match_stats_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_player_match_stats
    ADD CONSTRAINT fact_player_match_stats_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.dim_match(match_id);


--
-- Name: fact_player_match_stats fact_player_match_stats_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_player_match_stats
    ADD CONSTRAINT fact_player_match_stats_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.dim_player(canonical_id);


--
-- Name: fact_player_match_stats fact_player_match_stats_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_player_match_stats
    ADD CONSTRAINT fact_player_match_stats_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.dim_team(canonical_id);


--
-- Name: fact_shots fact_shots_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_shots
    ADD CONSTRAINT fact_shots_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.dim_match(match_id);


--
-- Name: fact_shots fact_shots_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_shots
    ADD CONSTRAINT fact_shots_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.dim_player(canonical_id);


--
-- Name: fact_shots fact_shots_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_shots
    ADD CONSTRAINT fact_shots_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.dim_team(canonical_id);


--
-- Name: fact_transfers fact_transfers_from_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_transfers
    ADD CONSTRAINT fact_transfers_from_team_id_fkey FOREIGN KEY (from_team_id) REFERENCES public.dim_team(canonical_id);


--
-- Name: fact_transfers fact_transfers_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_transfers
    ADD CONSTRAINT fact_transfers_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.dim_player(canonical_id);


--
-- Name: fact_transfers fact_transfers_to_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_transfers
    ADD CONSTRAINT fact_transfers_to_team_id_fkey FOREIGN KEY (to_team_id) REFERENCES public.dim_team(canonical_id);


--
-- Name: player_review player_review_canonical_id_assigned_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_review
    ADD CONSTRAINT player_review_canonical_id_assigned_fkey FOREIGN KEY (canonical_id_assigned) REFERENCES public.dim_player(canonical_id);


--
-- Name: player_review player_review_suggested_canonical_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_review
    ADD CONSTRAINT player_review_suggested_canonical_id_fkey FOREIGN KEY (suggested_canonical_id) REFERENCES public.dim_player(canonical_id);


--
-- PostgreSQL database dump complete
--
