DO
$$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'mms_user') THEN
      CREATE DATABASE mms_user;
   END IF;
END
$$;
GRANT ALL PRIVILEGES ON DATABASE mms_user TO postgres;
