SELECT FROM estudiantes;
INSERT INTO estudiante VALUES ('A', 'B'), ('C', 'D');
SELECT nombre FROM estudiante JOIN profesor ON estudiante.profesor_id = profesor.id WHERE estudiante.nombre = 'Pedro' ORDER BY DESC;
