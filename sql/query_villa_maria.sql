SELECT
  universidad as university,
  carrera as career,
  fecha_de_inscripcion as inscription_date,
  nombre as name,
  sexo as gender,
  fecha_nacimiento as age,
  localidad as location,
  email
FROM
    public.salvador_villa_maria
WHERE 
    universidad='UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA'
AND TO_DATE(fecha_de_inscripcion,'DD-Mon-YY') BETWEEN '01/Sep/20' AND '01/Feb/21'
