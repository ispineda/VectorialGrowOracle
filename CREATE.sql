-- CREACION DE TABLAS
DECLARE
    V_EXIST_COLLECTION      NUMBER;
    V_EXIST_DATA_VECTOR     NUMBER;
    V_SQL_TABLE_COLLECTION  VARCHAR2(32767);
    V_SQL_TABLE_DATA_VECTOR VARCHAR2(32767);
BEGIN
     V_SQL_TABLE_COLLECTION := '
    CREATE TABLE T_COLLECTION(
        id_collection   NUMBER GENERATED ALWAYS AS IDENTITY,
        name_collection VARCHAR2(50),
        len_vector      NUMBER,
        search_method   VARCHAR2(100),
        creation_date   DATE DEFAULT SYSDATE
    )';
    V_SQL_TABLE_DATA_VECTOR := '
    CREATE TABLE T_DATA_VECTOR(
        id_data_vector  NUMBER GENERATED ALWAYS AS IDENTITY,
        id_collection   NUMBER,
        description     VARCHAR2(4000),
        vector_name     VARCHAR2(100),
        creation_date   DATE DEFAULT SYSDATE,
        tags            CLOB,
        parameters      CLOB,
        vector          BLOB
    )';
    SELECT
        COUNT(*)
    INTO V_EXIST_COLLECTION
    FROM
        ALL_TABLES
    WHERE
        TABLE_NAME = 'T_COLLECTION';
    SELECT
        COUNT(*)
    INTO V_EXIST_DATA_VECTOR
    FROM
        ALL_TABLES
    WHERE
        TABLE_NAME = 'T_DATA_VECTOR';

    IF V_EXIST_COLLECTION = 0 THEN
        EXECUTE IMMEDIATE V_SQL_TABLE_COLLECTION;
        COMMIT;
    END IF;
    IF V_EXIST_DATA_VECTOR = 0 THEN
        EXECUTE IMMEDIATE V_SQL_TABLE_DATA_VECTOR;
        COMMIT;
    END IF;
END;