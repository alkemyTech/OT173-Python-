SELECT
	universidades AS university,
	carreras AS career,
	fechas_de_inscripcion AS inscription_date,
	nombres AS "name",
	sexo AS gender,
	fechas_nacimiento AS age,
	codigos_postales AS postal_code,
	emails AS email
FROM
	uba_kenedy
WHERE
	universidades = 'universidad-de-buenos-aires'
	AND TO_DATE(fechas_de_inscripcion,'DD-Mon-YY') BETWEEN '01-Sep-20' AND '01-Feb-21'