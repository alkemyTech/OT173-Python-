/* Get data from "lat_sociales_cine" between 01/9/2020 - 01/02/2021 */
/* https://alkemy-labs.atlassian.net/browse/PT173-18 */

SELECT 	universities AS university,
		careers AS career,
		inscription_dates AS inscription_date,
		"names" AS "name",
		sexo AS gender,
		birth_dates AS age,
		direccion AS postal_code,
		locations AS "location",
		emails AS email
        
	FROM lat_sociales_cine AS cine

WHERE 
	universities = "UNIVERSIDAD-DEL-CINE"
	AND TO_DATE(inscription_dates, 'DD-MM-YYYY') BETWEEN '01-09-2020' AND '01-02-2021'