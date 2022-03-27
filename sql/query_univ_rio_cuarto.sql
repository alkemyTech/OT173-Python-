SET datestyle = dmy;

SELECT univiersities as university,
    carrera as career,
    inscription_dates as inscription_date,
    "names" as "name",
    sexo as gender,
    fechas_nacimiento as age,
    direcciones as "location",
    email as email
FROM rio_cuarto_interamericana

WHERE univiersities = 'Universidad-nacional-de-río-cuarto'
AND date(inscription_dates) BETWEEN '01/09/2020' and '01/02/2021'