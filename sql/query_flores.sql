SELECT
  universidad as university,
  carrera as career,
  fecha_de_inscripcion as inscription_date,
  name,
  sexo as gender,
  fecha_nacimiento as age,
  codigo_postal as postal_code,
  correo_electronico as email
FROM
    public.flores_comahue
WHERE 
    universidad='UNIVERSIDAD DE FLORES'
AND TO_DATE(fecha_de_inscripcion,'YYYY-MM-DD') BETWEEN '2020/09/01' AND '2021/02/01'
