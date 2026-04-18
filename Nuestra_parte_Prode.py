from flask import Flask, jsonify, request
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)

@app.route('/ranking', methods=['GET'])
def ranking():
    limit = request.args.get('_limit', default=10, type=int)
    offset = request.args.get('_offset', default=0, type=int)

    if limit < 1:
        return jsonify(
            {"errors": [{"code": "400", "message": "El limit debe ser mayor a 0", "level": "Bad Request", "Description": "Parametro invalido"}]}), 400

    if offset < 0:
        return jsonify(
            {"errors": [{"code": "400", "message": "El offset no puede ser negativo", "level": "Bad Request", "Description": "Parametro invalido"}]}), 400



    try :
        db =  mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="",
            database=""
             )
        cursor = db.cursor(dictionary=True)
        sql = """
        SELECT
        U.id_usuario,
            COALESCE(SUM(
            CASE
            
                 WHEN p.Glocal = m.goles_local AND p.Gvisitante = m.goles_visitante 
                 THEN 3
                 
                 WHEN p.Glocal > p.Gvisitante AND  m.goles_local > m.goles_visitante 
                 OR p.Glocal < p.Gvisitante AND m.goles_local < m.goles_visitante 
                 OR p.Glocal = p.Gvisitante AND m.goles_local = m.goles_visitante 
                 THEN 1 
                 
                 ELSE 0 
            END
        ), 0) AS puntos
        FROM usuarios AS U
        LEFT JOIN predicciones AS p ON U.id_usuario = p.id_usuario
        LEFT JOIN partidos AS m ON p.id_partido = m.id_partido AND m.goles_local IS NOT NULL 
        GROUP BY U.id_usuario
        ORDER BY puntos DESC
        LIMIT %s OFFSET %s;
        """
        sql_conteo = ("SELECT COUNT(*) as total FROM usuarios")
        cursor.execute(sql_conteo)
        total = cursor.fetchone()["total"]
        valores = (limit, offset)
        cursor.execute(sql, valores)
        filas = cursor.fetchall()
        url_base = request.base_url

        if total == 0:
            links = {}
        else:
            links = {
                "_first" : {"href": f"{url_base}?_offset=0&_limit={limit}"},
                "_last" : {"href": f"{url_base}?_offset={((total - 1) // limit) * limit}&_limit={limit}"},
            }

            if offset + limit < total:
                links["_next"] = {"href": f"{url_base}?_offset={offset+limit}&_limit={limit}"}

            if offset > 0:
                prev_offset = max(0, offset - limit)
                links["_prev"] = {"href": f"{url_base}?_offset={prev_offset}&_limit={limit}"}

        return jsonify({"_links": links, "ranking": filas}), 200

    except mysql.connector.Error as error:

        return jsonify ({"errors":[{"code": "500", "message": str(error), "level": "Internal Server Error", "description": "Error interno del servidor"}]}), 500


@app.route("/partidos/<int:id_partido>/resultado", methods=["PUT"])
def ingresar_resultados(id_partido):
    data = request.get_json()  # obtenemos los datos que el usuario ingresa al body
    goles_local = data.get('local')
    goles_visitante = data.get('visitante')
    partido = data.get(id_partido)

    if goles_local is None or goles_visitante is None:
        return jsonify({
            "errors": [{
                "code": "400",
                "message": "No ingresaste los goles de ambos equipos",
                "level": "error",
                "description": "Ingresa los goles de ambos equipos"
            }]
        }), 400

    db =  mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="",
            database=""
             )
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("SELECT goles_local, goles_visitante FROM partidos WHERE id_partido=%s",
                       (id_partido,))  # la coma para que lo reconozca como un unico elemento
        if not cursor.fetchone():
            return jsonify({
                "errors": [{
                    "code": "404",
                    "message": "Ingrese un id valido",
                    "level": "error",
                    "description": "No existe un partido con el id que ingresaste"
                }]
            }), 404
        else:
            query = """UPDATE partidos \
                       SET goles_local = %s, goles_visitante = %s WHERE id_partido=%s"""
            params = [goles_local, goles_visitante, id_partido]
            cursor.execute(query, params)
            db.commit()
            return " ", 204
    except Error as e:
        return jsonify({
            "errors":[{
                "code":"500",
                "message": "Error en la base de datos",
                "level": "error",
                "description":"Error interno en el servidor"
            }]
        }), 500



@app.route("/partidos/<int:id_partido>/prediccion", methods=["POST"])
def ingresar_prediccion(id_partido):
    data=request.get_json()
    id_usuario = data.get("id_usuario")
    goles_local = data.get("local")
    goles_visitante = data.get("visitante")


    if  id_usuario is None or goles_local is None or goles_visitante is None:
        return jsonify({
            "errors":[
                {
                    "code":"400",
                    "message": "ingrese todos los datos",
                    "level": "error",
                    "description":"te falto ingresar el id, los goles del local o los goles del visitante"
                }
            ]
        }), 400

    db =  mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="",
            database=""
             )
    cursor = db.cursor(dictionary=True)
    params = [id_usuario, id_partido, goles_local, goles_visitante]
    try:
        cursor.execute("Select id_usuario FROM usuarios WHERE id_usuario=%s", (id_usuario,))
        if not cursor.fetchone():
            return jsonify({
                "errors":[{
                    "code": "404",
                    "message": "Usuario no encontrado",
                    "level":"error",
                    "description": f"El usuario con ID {id_usuario} no existe"
                }]
            }), 404

        cursor.execute("SELECT 1 FROM predicciones WHERE id_usuario=%s AND id_partido=%s", (id_usuario, id_partido,))
        if cursor.fetchone():
            return jsonify ({
                "errors":[{
                    "code":"409",
                    "message":"No se pueden hacer dos predicciones del mismo partido",
                    "level": "error",
                    "description":"Este usuario ya hizo una predicción de este partido"
                }]
            }), 409

        cursor.execute("SELECT 1 FROM partidos WHERE id_partido=%s AND goles_local IS NULL AND goles_visitante IS NULL",(id_partido,))
        if not cursor.fetchone():
            return jsonify({
                "errors":[
                    {
                        "code":"404",
                        "message": "Partido No Encontrado",
                        "level": "error",
                        "description":"El id del partido ingresado no coincide con ninguno de la tabla sin jugar"
                    }
                ]
            }), 404

        else:
            query="""INSERT INTO predicciones (id_usuario, id_partido, Glocal, Gvisitante) VALUES(%s, %s, %s, %s)"""
            cursor.execute(query, params)
            db.commit()
            return "", 201
    except Error as e:
        return jsonify({
            "errors":[{
                "code":"500",
                "message": "Error en la base de datos",
                "level": "error",
                "description":"Error interno en el servidor"
            }]
        }), 500

if __name__ == "__main__":
    app.run(debug=True, port=5000)


