SELECT
  university,
  career,
  inscription_date,
  nombre as name,
  sexo as gender,
  birth_date as age,
  location,
  email
  
FROM public.jujuy_utn as utn
WHERE university= 'universidad tecnológica nacional'
	AND inscription_date BETWEEN '2020/09/01' AND '2021/02/01'
