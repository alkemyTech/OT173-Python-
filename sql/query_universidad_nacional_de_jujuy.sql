SELECT university,
    career,
    inscription_date,
    nombre as "name",
    sexo as gender,
    birth_date as age,
    location,
    email
FROM jujuy_utn
WHERE university = 'universidad nacional de jujuy'
    AND date(inscription_date) >= '2020/09/01'
    AND date(inscription_date) <= '2021/02/01'