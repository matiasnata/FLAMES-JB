from flask import Blueprint, request, jsonify
from db import get_connection
from mysql.connector import Error

prode_bp = Blueprint('prode', __name__)

@prode_bp.route("/usuarios", methods=["GET"])
def obtener_usuarios():
    try:
        limit_arg = request.args.get ("_limit", default = "10")
        offset_arg = request.args.get ("_offset", default = "0")
        if not (limit_arg.isdigit() and offset_arg.isdigit()):
            return jsonify({
                "errors": [{
                    "code": "400",
                    "message": "Parametros invalidos",
                    "description": "Los parametros _limit y _offset deben ser numeros enteros."
                }]
            }), 400
        limit = int(limit_arg) 
        offset = int(offset_arg)   
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT COUNT(*) as total FROM usuarios")
        total_usuarios = cursor.fetchone()['total'] 
        # ejecuta la query
        cursor.execute(f"SELECT * FROM usuarios LIMIT {limit} OFFSET {offset}")
        resultado = cursor.fetchall()
        if not resultado:
            conn.close()
            return"", 204
        base_url = "http://127.0.0.1:8080/usuarios"
        ultimo_offset = max(0, ((total_usuarios - 1) // limit) * limit)
        prev_offset = 0
        if offset > 0:

            prev_offset = max(0, offset - limit)
        links = {
            "_first": {"href": f"{base_url}?_limit={limit}&_offset=0"},
            "_prev":  {"href": f"{base_url}?_limit={limit}&_offset={prev_offset}"},
            "_next": {"href": f"{base_url}?_limit={limit}&_offset={offset + limit}"},
            "_last": {"href": f"{base_url}?_limit={limit}&_offset={ultimo_offset}"}
        }

        # Solo agregamos el link "atrás" si no estamos en la primera página
        
        # 4. Respuesta final con el formato del Swagger
        cursor.close()
        conn.close()
        return jsonify({
            "usuarios": resultado,
            "_links": links
        }), 200
    except:

        return jsonify({
            "errors": [{
                "code": "500",
                "message": "Error interno del servidor",
                "description": "Fallo interno del servidor"
            }]
        }), 500
        
@prode_bp.route("/usuarios",methods=["POST"])
def crear_usuario():
    data = request.json
    if not data or "nombre" not in data or "email" not in data:
            return jsonify({
            "errors": [{
                "code": "400",
                "message": "Parametros inválidos",
                "description": "Corroborar datos ingresados"
            }]
        }), 400
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT email FROM usuarios")
        total_email = cursor.fetchall()
        for email_buscado in total_email:
            if email_buscado["email"] == data["email"]:
                cursor.close()
                conn.close()
                return jsonify({
                    "errors": [{
                    "code": "409",
                    "message": "Email duplicado",
                    "description": "El email ya existe, ingresa otro"
                    }]
            }), 409
        else:
            guarda_valores = "INSERT INTO usuarios(nombre,email) VALUES (%s,%s)"
            valores = (data["nombre"], data["email"])
            cursor.execute(guarda_valores, valores)
            conn.commit()
            cursor.close()
            conn.close()
            return jsonify("El usuario fue creado con éxito"), 201
    except:

        return jsonify({
            "errors": [{
                "code": "500",
                "message": "Error interno del servidor",
                "description": "Fallo interno del servidor"
            }]
        }), 500

@prode_bp.route("/usuarios/<id_buscado_por_usuario>",methods=["GET"])
def buscar_usuario_id(id_buscado_por_usuario):
    if not id_buscado_por_usuario.isdigit():
        return jsonify({
                "errors": [{
                    "code": "400",
                    "message": "Parametros invalidos",
                    "description": "El id buscado, tiene que ser un numero entero y positivo"
                }]
            }), 400
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try: 
        cursor.execute(f"SELECT id_usuario FROM usuarios WHERE id_usuario = {id_buscado_por_usuario}")
        id_encontrado = cursor.fetchone()
        if id_encontrado == None:
            cursor.close()
            conn.close()
            return jsonify({
            "errors": [{
                "code": "404",
                "message": "Dato inexistente",
                "description": "El id buscado no tiene un usuario asignado"
                }]
            }), 404
        else:
            cursor.execute(f"SELECT * FROM usuarios WHERE id_usuario = {id_buscado_por_usuario}")
            resultado = cursor.fetchone()
            cursor.close()
            conn.close()
            return jsonify(resultado),200
    except:

            return jsonify({
            "errors": [{
                "code": "500",
                "message": "Error interno del servidor",
                "description": "Fallo interno del servidor"
            }]
        }), 500

@prode_bp.route("/usuarios/<id_buscado_por_usuario>",methods=["PUT"])
def actualizar_usuario_id(id_buscado_por_usuario):
    data = request.json
    if not id_buscado_por_usuario.isdigit():
        return jsonify({
                "errors": [{
                    "code": "400",
                    "message": "Parametros invalidos",
                    "description": "El id buscado tiene que ser un numero entero y positivo"
                }]
            }), 400
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT email FROM usuarios")
        total_email = cursor.fetchall()
        for email_buscado in total_email:
            if email_buscado["email"] == data["email"]:
                cursor.close()
                conn.close()
                return jsonify({
                    "errors": [{
                    "code": "409",
                    "message": "Email duplicado",
                    "description": "El email ya existe, ingresa otro"
                    }]
            }), 409

        else:
            nombre_ingresado = data["nombre"]
            email_ingresado = data["email"]
            cursor.execute(f"SELECT * FROM usuarios WHERE id_usuario = {id_buscado_por_usuario}")
            actualizar = cursor.fetchone()
            if actualizar:
                cursor.execute(f"UPDATE usuarios SET nombre = '{nombre_ingresado}', email = '{email_ingresado}' WHERE id_usuario = {id_buscado_por_usuario}")
                conn.commit()
                cursor.close()
                conn.close()
                return "", 204
            else:
                cursor.execute(f"INSERT INTO usuarios (nombre,email) VALUES ('{nombre_ingresado}','{email_ingresado}')")   
                conn.commit()
                cursor.close()
                conn.close()
                return "", 201
    except:

            return jsonify({
            "errors": [{
                "code": "500",
                "message": "Error interno del servidor",
                "description": "Fallo interno del servidor"
            }]
        }), 500    

@prode_bp.route("/usuarios/<id_buscado_por_usuario>",methods=["DELETE"])
def eliminar_usuario(id_buscado_por_usuario):
    if not id_buscado_por_usuario.isdigit():
        return jsonify({
                "errors": [{
                    "code": "400",
                    "message": "Parametros invalidos",
                    "description": "El id buscado tiene que ser un numero entero y positivo"
                }]
            }), 400
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(f"SELECT id_usuario FROM usuarios WHERE id_usuario = {id_buscado_por_usuario}")
        id_encontrado = cursor.fetchone()
        if id_encontrado == None:
            cursor.close()
            conn.close()
            return jsonify({
            "errors": [{
                "code": "404",
                "message": "Dato inexistente",
                "description": "El id buscado no tiene un usuario asignado"
                }]
            }), 404
        else:
            cursor.execute(f"DELETE FROM usuarios WHERE id_usuario = {id_buscado_por_usuario}")
            conn.commit()
            cursor.close()
            conn.close()
            return "", 204
    except:

            return jsonify({
            "errors": [{
                "code": "500",
                "message": "Error interno del servidor",
                "description": "Fallo interno del servidor"
            }]
        }), 500        

@prode_bp.route('/ranking', methods=['GET'])
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
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
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
        
        cursor.close()
        conn.close()

        return jsonify({"_links": links, "ranking": filas}), 200

    except Error as e:
        return jsonify ({"errors":[{"code": "500", "message": str(e), "level": "Internal Server Error", "description": "Error interno del servidor"}]}), 500

@prode_bp.route("/partidos/<int:id_partido>/resultado", methods=["PUT"])
def ingresar_resultados(id_partido):
    data = request.get_json()  # obtenemos los datos que el usuario ingresa al body
    goles_local = data.get('local')
    goles_visitante = data.get('visitante')

    if goles_local is None or goles_visitante is None:
        return jsonify({
            "errors": [{
                "code": "400",
                "message": "No ingresaste los goles de ambos equipos",
                "level": "error",
                "description": "Ingresa los goles de ambos equipos"
            }]
        }), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("SELECT goles_local, goles_visitante FROM partidos WHERE id_partido=%s",
                       (id_partido,))  # la coma para que lo reconozca como un unico elemento
        if not cursor.fetchone():
            cursor.close()
            conn.close()
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
            conn.commit()
            cursor.close()
            conn.close()
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

@prode_bp.route("/partidos/<int:id_partido>/prediccion", methods=["POST"])
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

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    params = [id_usuario, id_partido, goles_local, goles_visitante]
    try:
        cursor.execute("Select id_usuario FROM usuarios WHERE id_usuario=%s", (id_usuario,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
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
            cursor.close()
            conn.close()
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
            cursor.close()
            conn.close()
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
            conn.commit()
            cursor.close()
            conn.close()
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
