-- 
-- connect db_dev1 stas
--
set role dev1owners;
set search_path=usrmgmt,pg_catalog,public;

-- Type: registation_status_en
-- DROP TYPE registation_status_en;
CREATE TYPE registation_status_en AS ENUM
   ('requested',
    'progress',
    'proved',
    'rejected',
    'registered',
    'deleted');
ALTER TYPE registation_status_en
  OWNER TO dev1owners;

-- Type: user_status_en
-- DROP TYPE user_status_en;
CREATE TYPE user_status_en AS ENUM
   ('registering',
    'registered',
    'connected',
    'blocked',
    'frozen',
    'closed',
    'dropped');
ALTER TYPE user_status_en
  OWNER TO dev1owners;
COMMENT ON TYPE user_status_en
  IS 'for users.status';

-- Sequence: register_id_seq
-- DROP SEQUENCE register_id_seq;
CREATE SEQUENCE register_id_seq
  INCREMENT 1
  MINVALUE 0
  MAXVALUE 2147483647
  START 1
  CACHE 1
  CYCLE;
ALTER TABLE register_id_seq
  OWNER TO dev1owners;
GRANT ALL ON SEQUENCE register_id_seq TO dev1owners;
GRANT USAGE ON SEQUENCE register_id_seq TO dev1users;

-- Sequence: user_id_seq
-- DROP SEQUENCE user_id_seq;
CREATE SEQUENCE user_id_seq
  INCREMENT 1
  MINVALUE 0
  MAXVALUE 2147483647
  START 1 
  CACHE 1;
ALTER TABLE user_id_seq
  OWNER TO dev1owners;
GRANT ALL ON SEQUENCE user_id_seq TO dev1owners;
GRANT USAGE ON SEQUENCE user_id_seq TO dev1users;

-- Table: registrations
-- DROP TABLE registrations;
CREATE TABLE registrations
(
  id integer NOT NULL DEFAULT nextval('register_id_seq'::regclass),
  status registation_status_en NOT NULL,
  logname character varying(64) NOT NULL,
  alias character varying(64) NOT NULL, -- nic
  created timestamp without time zone NOT NULL DEFAULT now(), -- Когда пользователь сохранил запрос на регистрацию.
  passwd character(32) NOT NULL,
  authcode character(32) NOT NULL, -- Код авторизации, который вычисляется приложением сразу при занесении в таблицу и при повторной регистрации на основе timestamp и полей: str(id)+logname+alias+passwd
  CONSTRAINT registations_pk PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE registrations
  OWNER TO dev1owners;
GRANT ALL ON TABLE registrations TO dev1owners;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE registrations TO dev1users;
COMMENT ON TABLE registrations
  IS 'журнал регистарций';
COMMENT ON COLUMN registrations.alias IS 'nic';
COMMENT ON COLUMN registrations.created IS 'Когда пользователь сохранил запрос на регистрацию.';
COMMENT ON COLUMN registrations.authcode IS 'Код авторизации, который вычисляется приложением сразу при занесении в таблицу и при повторной регистрации на основе timestamp и полей: str(id)+logname+alias+passwd';

-- Index: registrations_authcode
-- DROP INDEX registrations_authcode;
CREATE UNIQUE INDEX registrations_authcode
  ON registrations
  USING btree
  (authcode COLLATE pg_catalog."default");

-- Index: registrations_logname
-- DROP INDEX registrations_logname;
CREATE INDEX registrations_logname
  ON registrations
  USING btree
  (logname COLLATE pg_catalog."default");

-- Index: registrations_status
-- DROP INDEX registrations_status;
CREATE INDEX registrations_status
  ON registrations
  USING btree
  (status);

-- Table: users
-- DROP TABLE users;
CREATE TABLE users
(
  id integer NOT NULL DEFAULT nextval('user_id_seq'::regclass),
  status user_status_en NOT NULL,
  logname character varying(64) NOT NULL, -- unique login name
  alias character varying(64) NOT NULL, -- current visible name
  passwd character(32) NOT NULL,
  CONSTRAINT users_id_pk PRIMARY KEY (id),
  CONSTRAINT users_logname_uq UNIQUE (logname)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE users
  OWNER TO dev1owners;
GRANT ALL ON TABLE users TO dev1owners;
GRANT SELECT, INSERT ON TABLE users TO dev1users;
COMMENT ON TABLE users
  IS 'Реестр всех пользователей, которые были когда-либо зарегистрированы.
Может быть зачищена только после того, как будет удалён весь контент, который ссылается на этого пользователя.';
COMMENT ON COLUMN users.logname IS 'unique login name';
COMMENT ON COLUMN users.alias IS 'current visible name';

-- Index: users_id
-- DROP INDEX users_id;
CREATE UNIQUE INDEX users_id
  ON users
  USING btree
  (id);


