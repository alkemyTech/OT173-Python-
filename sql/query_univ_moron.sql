SET datestyle = dmy;

SELECT universidad as university,
    carrerra as career,
    fechaiscripccion as inscription_date,
    nombrre as "name",
    sexo as gender,
    nacimiento as age,
    codgoposstal as postal_code,
    direccion as "location",
    eemail as email
FROM moron_nacional_pampa

WHERE universidad = 'Universidad de morón'
AND date(fechaiscripccion) BETWEEN '01/09/2020' and '01/02/2021'