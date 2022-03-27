SELECT
  universidad as university,
  careers as career,
  fecha_de_inscripcion as inscription_date,
  names as name,
  sexo as gender,
  birth_dates as age,
  codigo_postal as postal_code,
  direcciones as location,
  correos_electronicos as email
  
FROM public.palermo_tres_de_febrero as tres_de_febrero
WHERE universidad= 'universidad_nacional_de_tres_de_febrero'
	AND TO_DATE(fecha_de_inscripcion, 'DD/Mon/YY') BETWEEN '01/Sep/2020' AND '01/Feb/2021'
