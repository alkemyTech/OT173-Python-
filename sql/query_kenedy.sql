/* Get data from "universidad-j.-f.-kennedy" between 01/9/2020 - 01/02/2021 */
/* https://alkemy-labs.atlassian.net/browse/PT173-18 */

SELECT
	universidades AS university,
	carreras AS career,
	fechas_de_inscripcion AS inscription_date,
	nombres AS "name",
	sexo AS gender,
	fechas_nacimiento AS age,
	codigos_postales AS postal_code,
	emails AS email

FROM uba_kenedy

WHERE
	universidades = 'universidad-j.-f.-kennedy'
	AND TO_DATE(fechas_de_inscripcion,'DD-Mon-YY') BETWEEN '01-Sep-20' AND '01-Feb-21'