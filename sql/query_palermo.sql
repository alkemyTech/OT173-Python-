SELECT universidad as university,
    careers as career,
    fecha_de_inscripcion as inscription_date,
    "names" as "name",
    sexo as gender,
    birth_dates as age,
    codigo_postal as postal_code,
    correos_electronicos as email
FROM palermo_tres_de_febrero
WHERE universidad = '_universidad_de_palermo'
    AND date(fecha_de_inscripcion) >= '01/Sep/20'
    AND date(fecha_de_inscripcion) <= '01/Feb/21'