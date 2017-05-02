-- 
-- connect postgres postgres
--
 
-- Tablespace: ts_users
-- DROP TABLESPACE ts_users
CREATE TABLESPACE ts_users
  OWNER postgres
  LOCATION '/u01/pgdata1/9.5/main/pg_tblspc/users';
GRANT CREATE ON TABLESPACE ts_users TO postgres WITH GRANT OPTION;

-- Tablespace: ts_temp
-- DROP TABLESPACE ts_temp
CREATE TABLESPACE ts_temp
  OWNER postgres
  LOCATION '/u01/pgdata1/9.5/main/pg_tblspc/temp';
GRANT CREATE ON TABLESPACE ts_temp TO postgres WITH GRANT OPTION;

-- Role: users
-- DROP ROLE users;
CREATE ROLE users
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
COMMENT ON ROLE users IS 'non superusers and system administrators';
GRANT CREATE ON TABLESPACE ts_users TO users;
GRANT CREATE ON TABLESPACE ts_temp TO users;

ALTER ROLE users
  SET default_tablespace = 'ts_users';
ALTER ROLE users
  SET temp_tablespaces = ts_temp;

-- Role: dev1owners
-- DROP ROLE dev1owners;
CREATE ROLE dev1owners
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
GRANT users TO dev1owners;

-- Role: dev1users
-- DROP ROLE dev1users;
CREATE ROLE dev1users
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
GRANT users TO dev1users;

-- Role: stas
-- DROP ROLE stas;
CREATE ROLE stas LOGIN
  ENCRYPTED PASSWORD 'md5e9ce0cb21471d044ccd74289e14320e6'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
ALTER ROLE stas
  SET role = 'users';
ALTER ROLE stas
  SET default_tablespace = 'ts_users';
ALTER ROLE stas
  SET temp_tablespaces = ts_temp;
GRANT users TO stas;
GRANT dev1owners TO stas;

-- Role: tester
-- DROP ROLE tester;
CREATE ROLE tester LOGIN
  ENCRYPTED PASSWORD 'md587cf38c20ed813c9573f78621b038764'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
ALTER ROLE tester
  SET default_tablespace = 'ts_users';
ALTER ROLE tester
  SET temp_tablespaces = ts_temp;
GRANT dev1users TO tester;
GRANT users TO tester;

-- Database: db_dev1
-- DROP DATABASE db_dev1;
CREATE DATABASE db_dev1
  WITH OWNER = stas
       ENCODING = 'UTF8'
       TABLESPACE = pg_default
       LC_COLLATE = 'en_US.UTF-8'
       LC_CTYPE = 'en_US.UTF-8'
       CONNECTION LIMIT = -1;
ALTER DATABASE db_dev1
  SET temp_tablespaces = ts_temp;
ALTER DATABASE db_dev1
  SET default_tablespace = 'ts_users';
ALTER DATABASE db_dev1
  OWNER TO stas;

-- 
-- connect db_dev1 postgres
--
 
-- Schema: usrmgmt
-- DROP SCHEMA usrmgmt;
CREATE SCHEMA usrmgmt AUTHORIZATION dev1owners;
GRANT ALL ON SCHEMA usrmgmt TO dev1owners WITH GRANT OPTION;
GRANT USAGE ON SCHEMA usrmgmt TO dev1users;
ALTER DEFAULT PRIVILEGES IN SCHEMA usrmgmt
    GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLES
    TO dev1owners WITH GRANT OPTION;
ALTER DEFAULT PRIVILEGES IN SCHEMA usrmgmt
    GRANT SELECT, UPDATE, USAGE ON SEQUENCES
    TO dev1owners WITH GRANT OPTION;
ALTER DEFAULT PRIVILEGES IN SCHEMA usrmgmt
    GRANT EXECUTE ON FUNCTIONS
    TO dev1owners;
ALTER DEFAULT PRIVILEGES IN SCHEMA usrmgmt
    GRANT USAGE ON TYPES
    TO dev1owners;

