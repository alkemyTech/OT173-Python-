/* Obtain the data of people registered in "Universidad nacional de la pampa" between the dates 09/01/2020 to 02/01/2021 */
SELECT
   universidad AS university,
   carrerra AS career,
   fechaiscripccion AS inscription_date,
   nombrre AS name,
   sexo AS gender,
   nacimiento AS age,
   codgoposstal AS postal_code,
   eemail AS email 
FROM
   moron_nacional_pampa 
WHERE
   universidad = 'Universidad nacional de la pampa' 
   AND to_date(fechaiscripccion, 'DD/MM/YYYY') BETWEEN to_date('01/09/2020', 'DD/MM/YYYY') AND to_date('01/02/2021', 'DD/MM/YYYY');