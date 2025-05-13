CREATE OR REPLACE FUNCTION map_service_code(code smallint)
RETURNS smallint AS $$
BEGIN
    IF code IN (1) THEN
        RETURN 1; 
    ELSIF code IN (2, 10) THEN
        RETURN 2;
    ELSIF code IN (5, 7, 15, 17, 19, 20, 22, 24, 49, 52, 53, 57,58, 60, 62, 71, 72, 81) THEN
        RETURN 3; 
    ELSIF code IN (11, 18) THEN
        RETURN 4;
    ELSIF code IN (21, 31, 33, 34, 51, 55, 56, 61) THEN
        RETURN 5; 
    ELSIF code = 23 THEN
        RETURN 6; 
    ELSIF code IN (3, 4, 6, 8, 9, 12, 13, 14, 16, 25, 26, 27, 28, 29,
                   30, 32, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
                   46, 47, 48, 50, 54, 59, 63, 64, 65, 66, 67, 68, 69,
                   70, 73, 74, 75, 76, 77, 78, 79, 80, 82, 83, 84, 85,
                   86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99) THEN
        RETURN 7; 
    ELSE
        RETURN 8; 
    END IF;
END;
$$ LANGUAGE plpgsql;

SELECT map_service_code(0100000000); 

SELECT map_service_code(01::smallint); 


SELECT map_service_code(86::smallint); 