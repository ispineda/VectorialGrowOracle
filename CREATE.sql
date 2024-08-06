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
/--split
-- PAQUETE PARA ADMINISTRAR BASE VECTORIAL
CREATE OR REPLACE PACKAGE DBVECTORIAL IS
   
    TYPE T_BINARY_DOUBLE IS
        TABLE OF BINARY_DOUBLE;
    
    PROCEDURE CREATE_COLLECTION(
        P_NAME_COLLECTION IN VARCHAR2,
        P_LEN_VECTOR IN NUMBER, 
        P_SEARCH_METHOD IN VARCHAR2
    );
    
    PROCEDURE DELETE_COLLECTION(
        P_NAME_COLLECTION IN VARCHAR2 
    );
    
    PROCEDURE GET_COLLECTION_DETAILS (
        P_NAME_COLLECTION IN VARCHAR2,
        P_LEN_VECTOR OUT NUMBER,
        P_SEARCH_METHOD OUT VARCHAR2,
        P_CREATION_DATE OUT DATE
    );
    
END DBVECTORIAL;
/--split
CREATE OR REPLACE PACKAGE BODY DBVECTORIAL IS 
    
    PROCEDURE CREATE_COLLECTION(
        P_NAME_COLLECTION IN VARCHAR2,
        P_LEN_VECTOR IN NUMBER, 
        P_SEARCH_METHOD IN VARCHAR2
    ) IS
        Q_INSERT_COLLECTION VARCHAR2(4000);
    BEGIN
        Q_INSERT_COLLECTION := '
            INSERT INTO T_COLLECTION ( 
                NAME_COLLECTION, 
                LEN_VECTOR, 
                SEARCH_METHOD ) 
            VALUES ( :V1, :V2, :V3 )';
        EXECUTE IMMEDIATE Q_INSERT_COLLECTION USING P_NAME_COLLECTION, P_LEN_VECTOR, P_SEARCH_METHOD;
        COMMIT;
    END;
    
    PROCEDURE DELETE_COLLECTION (
        p_name_collection IN VARCHAR2 
    ) AS
        v_sql_delete_data VARCHAR2(4000);
        v_sql_delete_collection VARCHAR2(4000);
    BEGIN
        v_sql_delete_data := '
        DELETE FROM T_DATA_VECTOR
        WHERE ID_COLLECTION IN (
            SELECT ID_COLLECTION
            FROM T_COLLECTION
            WHERE NAME_COLLECTION = :v1
        )';
        
        v_sql_delete_collection := '
        DELETE FROM T_COLLECTION
        WHERE NAME_COLLECTION = :v1';
        
        EXECUTE IMMEDIATE v_sql_delete_data USING p_name_collection;
        EXECUTE IMMEDIATE v_sql_delete_collection USING p_name_collection;
        
        COMMIT;
        
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
    END;
    
    PROCEDURE GET_COLLECTION_DETAILS (
        P_NAME_COLLECTION IN VARCHAR2,
        P_LEN_VECTOR OUT NUMBER,
        P_SEARCH_METHOD OUT VARCHAR2,
        P_CREATION_DATE OUT DATE
    ) IS
    
    BEGIN
        SELECT LEN_VECTOR, SEARCH_METHOD, CREATION_DATE
        INTO P_LEN_VECTOR, P_SEARCH_METHOD, P_CREATION_DATE
        FROM T_COLLECTION
        WHERE NAME_COLLECTION = P_NAME_COLLECTION;
    
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                P_LEN_VECTOR := NULL;
                P_SEARCH_METHOD := NULL;
                P_CREATION_DATE := NULL;
    END;

    /* ____________________________________________________________________
      |                 Manejo vectorial con Blob                          |
      |____________________________________________________________________|
    */
    
    -- Función distancia euclidiana Blob
    FUNCTION EUCLIDIAN_DISTANCE_LOB( 
        V1 BLOB, 
        V2 BLOB, 
        BASE_BYTES NUMBER:= 8
    ) RETURN NUMBER
    IS
        BLOB_LEN_V1 NUMBER;
        BLOB_LEN_V2 NUMBER;
        
        LEN_V1 NUMBER;
        LEN_V2 NUMBER;
        
        BUFFER_V1 RAW(32767);
        BUFFER_V2  RAW(32767);
        
        AMOUNT INTEGER := 32767;
        OFFSET INTEGER := 1;
        
        DIST NUMBER :=0;
        FRAGMENT_V1 BINARY_DOUBLE;
        FRAGMENT_V2 BINARY_DOUBLE;
        
    BEGIN
        
        BLOB_LEN_V1 := DBMS_LOB.GETLENGTH(V1);
        BLOB_LEN_V2 := DBMS_LOB.GETLENGTH(V2);
        
        LEN_V1:= CEIL(BLOB_LEN_V1/BASE_BYTES);
        LEN_V2:= CEIL(BLOB_LEN_V2/BASE_BYTES);
        
        IF LEN_V1 <> LEN_V2 THEN
            RAISE_APPLICATION_ERROR(-20001, 'Los vectores deben tener la misma longitud.');
        END IF;
        
        LOOP
            EXIT WHEN OFFSET > BLOB_LEN_V1;
    
            DBMS_LOB.READ(V1, AMOUNT, OFFSET, BUFFER_V1);
            DBMS_LOB.READ(V2, AMOUNT, OFFSET, BUFFER_V2);
            
            FOR I IN 0..LEN_V1 - 1 LOOP
                FRAGMENT_V1:= UTL_RAW.CAST_TO_BINARY_DOUBLE(DBMS_LOB.SUBSTR(BUFFER_V1, BASE_BYTES, I * BASE_BYTES + 1));
                FRAGMENT_V2:= UTL_RAW.CAST_TO_BINARY_DOUBLE(DBMS_LOB.SUBSTR(BUFFER_V2, BASE_BYTES, I * BASE_BYTES + 1));
                
                DIST := DIST + POWER(
                    FRAGMENT_V1 - FRAGMENT_V2, 
                    2);
            END LOOP;
            
            OFFSET := OFFSET + AMOUNT;
        END LOOP;
        RETURN SQRT(DIST);
    END;
    
    FUNCTION SIMILITARY_COSINES_LOB(
        V1 BLOB, 
        V2 BLOB, 
        BASE_BYTES NUMBER:=8
    ) RETURN NUMBER
    IS
        BLOB_LEN_V1 NUMBER;
        BLOB_LEN_V2 NUMBER;
        
        LEN_V1 NUMBER;
        LEN_V2 NUMBER;
        
        BUFFER_V1 RAW(32767);
        BUFFER_V2  RAW(32767);
        
        AMOUNT INTEGER := 32767;
        OFFSET INTEGER := 1;
        
        DOT_PRODUCT NUMBER :=0;
        NORM_V1 NUMBER :=0;
        NORM_V2 NUMBER :=0;
        
        FRAGMENT_V1 BINARY_DOUBLE;
        FRAGMENT_V2 BINARY_DOUBLE;
        
    BEGIN
        
        BLOB_LEN_V1 := DBMS_LOB.GETLENGTH(V1);
        BLOB_LEN_V2 := DBMS_LOB.GETLENGTH(V2);
        
        LEN_V1:= CEIL(BLOB_LEN_V1/BASE_BYTES);
        LEN_V2:= CEIL(BLOB_LEN_V2/BASE_BYTES);
        
        IF LEN_V1 <> LEN_V2 THEN
            RAISE_APPLICATION_ERROR(-20001, 'LOS VECTORES DEBEN TENER LA MISMA LONGITUD.');
        END IF;
        
        LOOP
            EXIT WHEN OFFSET > BLOB_LEN_V1;
    
            DBMS_LOB.READ(V1, AMOUNT, OFFSET, BUFFER_V1);
            DBMS_LOB.READ(V2, AMOUNT, OFFSET, BUFFER_V2);
            
            FOR I IN 0..LEN_V1 - 1 LOOP
                FRAGMENT_V1:= UTL_RAW.CAST_TO_BINARY_DOUBLE(DBMS_LOB.SUBSTR(BUFFER_V1, BASE_BYTES, I * BASE_BYTES + 1));
                FRAGMENT_V2:= UTL_RAW.CAST_TO_BINARY_DOUBLE(DBMS_LOB.SUBSTR(BUFFER_V2, BASE_BYTES, I * BASE_BYTES + 1));
                
                DOT_PRODUCT := DOT_PRODUCT + (FRAGMENT_V1*FRAGMENT_V2);
                NORM_V1 := NORM_V1 + POWER(FRAGMENT_V1,2);
                NORM_V2 := NORM_V2 + POWER(FRAGMENT_V2,2);
            END LOOP;
            
            OFFSET := OFFSET + AMOUNT;
        END LOOP;
        NORM_V1 := SQRT(NORM_V1);
        NORM_V2 := SQRT(NORM_V2);
        
        IF NORM_V1 = 0 OR NORM_V2 = 0 THEN
            RAISE_APPLICATION_ERROR(-20002, 'EXISTE ALGUNA NORMA CON VALOR CERO');
        END IF;
        RETURN DOT_PRODUCT / (NORM_V1*NORM_V2);
    END;
    
    FUNCTION DOT_PRODUCT_LOB(
        V1 BLOB, 
        V2 BLOB, 
        BASE_BYTES NUMBER:=8
    ) RETURN NUMBER 
    IS
        BLOB_LEN_V1 NUMBER;
        BLOB_LEN_V2 NUMBER;
        
        LEN_V1 NUMBER;
        LEN_V2 NUMBER;
        
        BUFFER_V1 RAW(32767);
        BUFFER_V2  RAW(32767);
        
        AMOUNT INTEGER := 32767;
        OFFSET INTEGER := 1;
        DOT_PRODUCT NUMBER;
        
        FRAGMENT_V1 BINARY_DOUBLE;
        FRAGMENT_V2 BINARY_DOUBLE;
    BEGIN
        BLOB_LEN_V1 := DBMS_LOB.GETLENGTH(V1);
        BLOB_LEN_V2 := DBMS_LOB.GETLENGTH(V2);
        
        LEN_V1:= CEIL(BLOB_LEN_V1/BASE_BYTES);
        LEN_V2:= CEIL(BLOB_LEN_V2/BASE_BYTES);
        
        IF LEN_V1 <> LEN_V2 THEN
            RAISE_APPLICATION_ERROR(-20001, 'LOS VECTORES DEBEN TENER LA MISMA LONGITUD.');
        END IF;
        
        LOOP
            EXIT WHEN OFFSET > BLOB_LEN_V1;
    
            DBMS_LOB.READ(V1, AMOUNT, OFFSET, BUFFER_V1);
            DBMS_LOB.READ(V2, AMOUNT, OFFSET, BUFFER_V2);
            
            FOR I IN 0..LEN_V1 - 1 LOOP
                FRAGMENT_V1:= UTL_RAW.CAST_TO_BINARY_DOUBLE(DBMS_LOB.SUBSTR(BUFFER_V1, BASE_BYTES, I * BASE_BYTES + 1));
                FRAGMENT_V2:= UTL_RAW.CAST_TO_BINARY_DOUBLE(DBMS_LOB.SUBSTR(BUFFER_V2, BASE_BYTES, I * BASE_BYTES + 1));
                
                DOT_PRODUCT := DOT_PRODUCT + (FRAGMENT_V1*FRAGMENT_V2);
            END LOOP;
            
            OFFSET := OFFSET + AMOUNT;
        END LOOP;
        RETURN DOT_PRODUCT;
    END;
    
    /* ____________________________________________________________________
      |                 Convertidor BLOB a LIST                            |
      |____________________________________________________________________|
    */
    -- Función de conversion de datos
    FUNCTION CONVERT_BLOB_TO_LIST ( 
        V1 BLOB
    ) RETURN T_BINARY_DOUBLE 
    IS
        RESULT      T_BINARY_DOUBLE := T_BINARY_DOUBLE();
        BLOB_LEN_V1 NUMBER;
        LEN_V1      NUMBER;
        BUFFER_V1   RAW(32767);
        AMOUNT      INTEGER := 32767;
        OFFSET      INTEGER := 1;
        BASE        NUMBER := 8;
        FRAGMENT_V1 BINARY_DOUBLE;
    BEGIN
        BLOB_LEN_V1 := DBMS_LOB.GETLENGTH(V1);
        LEN_V1 := CEIL(BLOB_LEN_V1 / BASE);
        LOOP
            EXIT WHEN OFFSET > BLOB_LEN_V1;
            DBMS_LOB.READ(V1, AMOUNT, OFFSET, BUFFER_V1);
            FOR I IN 0..LEN_V1 - 1 LOOP
                FRAGMENT_V1 := UTL_RAW.CAST_TO_BINARY_DOUBLE(DBMS_LOB.SUBSTR(BUFFER_V1, BASE, I * BASE + 1));

                RESULT.EXTEND;
                RESULT(RESULT.LAST) := FRAGMENT_V1;
            END LOOP;

            OFFSET := OFFSET + AMOUNT;
        END LOOP;

        RETURN RESULT;
    END;
    
    /* ____________________________________________________________________
      |                 Manejo vectorial con LIST                          |
      |____________________________________________________________________|
    */
    
    FUNCTION EUCLIDIAN_DISTANCE_LIST (
        V1 BLOB, 
        V2 BLOB
    ) RETURN NUMBER 
    IS
        DIST        NUMBER := 0;
        FRAGMENT_V1 BINARY_DOUBLE;
        FRAGMENT_V2 BINARY_DOUBLE;
        LIST_V1     T_BINARY_DOUBLE := T_BINARY_DOUBLE();
        LIST_V2     T_BINARY_DOUBLE := T_BINARY_DOUBLE();
    BEGIN
        LIST_V1 := CONVERT_BLOB_TO_LIST(V1);
        LIST_V2 := CONVERT_BLOB_TO_LIST(V2);
        IF LIST_V1.COUNT <> LIST_V2.COUNT THEN
            RAISE_APPLICATION_ERROR(-20001, 'LOS VECTORES DEBEN TENER LA MISMA LONGITUD.');
        END IF;
    
        FOR I IN 1..LIST_V1.COUNT LOOP
            DIST := DIST + POWER(LIST_V1(I) - LIST_V2(I), 2);
        END LOOP;
    
        RETURN SQRT(DIST);
    END;
    
    FUNCTION DOT_PRODUCT_LIST(
        V1 BLOB, 
        V2 BLOB
    ) RETURN NUMBER 
    IS
        LIST_V1     T_BINARY_DOUBLE := T_BINARY_DOUBLE();
        LIST_V2     T_BINARY_DOUBLE := T_BINARY_DOUBLE();
        DOT_PRODUCT NUMBER := 0;
    BEGIN
        LIST_V1 := CONVERT_BLOB_TO_LIST(V1);
        LIST_V2 := CONVERT_BLOB_TO_LIST(V2);
        
        FOR I IN 1..LIST_V1.COUNT LOOP
            DOT_PRODUCT := DOT_PRODUCT + ( LIST_V1(I) * LIST_V2(I) );
        END LOOP;
        RETURN DOT_PRODUCT;
    END;

    FUNCTION SIMILITARY_COSINES_LIST (
        V1 BLOB, 
        V2 BLOB
    ) RETURN NUMBER 
    IS
        DOT_PRODUCT NUMBER := 0;
        NORM_V1     NUMBER := 0;
        NORM_V2     NUMBER := 0;
        LIST_V1     T_BINARY_DOUBLE := T_BINARY_DOUBLE();
        LIST_V2     T_BINARY_DOUBLE := T_BINARY_DOUBLE();
    BEGIN
        LIST_V1 := CONVERT_BLOB_TO_LIST(V1);
        LIST_V2 := CONVERT_BLOB_TO_LIST(V2);
        
        IF LIST_V1.COUNT <> LIST_V2.COUNT THEN
            RAISE_APPLICATION_ERROR(-20001, 'LOS VECTORES DEBEN TENER LA MISMA LONGITUD.');
        END IF;
    
        FOR I IN 1..LIST_V1.COUNT LOOP
            DOT_PRODUCT := DOT_PRODUCT + ( LIST_V1(I) * LIST_V2(I) );
            NORM_V1 := NORM_V1 + POWER(LIST_V1(I), 2);
            NORM_V2 := NORM_V2 + POWER(LIST_V2(I), 2);
        END LOOP;
        
        NORM_V1 := SQRT(NORM_V1);
        NORM_V2 := SQRT(NORM_V2);
        
        IF NORM_V1 = 0 OR NORM_V2 = 0 THEN
            RAISE_APPLICATION_ERROR(-20002, 'EXISTE ALGUNA NORMA CON VALOR CERO');
        END IF;
    
        RETURN ( DOT_PRODUCT / ( NORM_V1 * NORM_V2 ) );
    END;

END DBVECTORIAL;