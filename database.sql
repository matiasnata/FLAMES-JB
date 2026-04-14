
DROP TABLE IF EXISTS partidos;
CREATE TABLE partidos (
  id_partido int NOT NULL AUTO_INCREMENT,
  equipo_local varchar(30) NOT NULL,
  equipo_visitante varchar(30) NOT NULL,
  fecha timestamp NOT NULL,
  goles_local int NOT NULL,
  goles_visitante int NOT NULL,
  fase enum('Fase de grupos','Dieciseisavos de Final','Octavos de Final','Cuartos de Final','Semifinal','Final') NOT NULL,
  PRIMARY KEY (`id_partido`)
);

DROP TABLE IF EXISTS usuarios;

CREATE TABLE usuarios (
  id_usuario int NOT NULL AUTO_INCREMENT,
  nombre varchar(50) NOT NULL,
  email varchar(50) NOT NULL,
  PRIMARY KEY (id_usuario)
);

CREATE TABLE predicciones(
    Glocal int NOT NULL,
    Gvisitante int NOT NULL,
    id_usuario int NOT NULL,
    id_partido int NOT NULL,
    FOREIGN KEY (id_partido) REFERENCES partidos(id_partido),
    FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario),
    PRIMARY KEY (id_usuario, id_partido)
);
