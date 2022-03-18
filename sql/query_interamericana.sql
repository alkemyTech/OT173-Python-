/* Obtain the data of people registered in "Universidad Abierta Interamericana" between the dates 09/01/2020 to 02/01/2021 */
SELECT
   univiersities AS university,
   carrera AS career,
   inscription_dates AS inscription_date,
   names AS name,
   sexo AS gender,
   fechas_nacimiento AS age,
   direcciones AS postal_code_location,
   email 
FROM
   rio_cuarto_interamericana 
WHERE
   univiersities = '-universidad-abierta-interamericana' 
   AND to_date(inscription_dates, 'YY/Mon/DD') BETWEEN to_date('20/SEP/01', 'YY/Mon/DD') AND to_date('21/FEB/01', 'YY/Mon/DD');
