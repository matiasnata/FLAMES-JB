from flask import Blueprint, request, jsonify
from db import get_connection #traigo el conector de la database, asi uso las tablas

partidos_bp = Blueprint('partidos',__name__) #aca creo la variable que tiene el blueprint de los partidos
# el blueprint es un paquete donde vamos a tener todos los endpoints, GET, POST ETC

# y aca arranco con los endpoints:

@partidos_bp.route("/partidos", methods=["POST"]) #creo la url /partidos, metodo post para crear justamente
def crear_partido():
    data = request.get_json() #aca agarra el JSON del post, osea los datos
    conn = get_connection()
    cursor = conn.cursor() #abro conexion y cursor, para ejecutar el sql
    query="""
    INSERT INTO partidos (equipo_local, equipo_visitante, fecha, fase)
    VALUES (%s,%s,%s,%s) 
    """ #defino una query para la table, seria como el code dentro del sql q vamos a ejecutar aca en el py
    # cada %s es un valor que va a ser reemplazado despues
    cursor.execute(query,(
        data['equipo_local'],
        data['equipo_visitante'],
        data['fecha'],
        data['fase']
    )) #inserto esa query en el db, con los valores del data (del JSON) que se ingresaron en cada columna
    conn.commit() #guardo los datos
    return jsonify({"mensaje": "Partido creado correctamente"}), 201

@partidos_bp.route("/partidos", methods=["GET"])
def obtener_partidos():
    equipo = request.args.get("equipo") #estas 3 lineas guarda lo que se escriba en la url
    fecha = request.args.get("fecha")
    fase = request.args.get("fase")

    page = int(request.args.get("page",1)) # indico la pagina que se va a mostrar
    per_page = int(request.args.get("per_page",10)) # indico cuantos resultados va en cada pagina

    offset = (page - 1) * per_page # indico desde donde va a empezar a traer los datos (del pricnipio)

    #ahora creo la query, sumandole a la misma, cada filtro, dependiendo la URL
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = "SELECT id_partido, equipo_local, equipo_visitante, fecha, fase FROM partidos WHERE 1=1"
        params = [] # creo una lista con cada filtro o parametro, q se va a extender x cada uno de ellos
        if equipo:
            query += " AND (equipo_local = %s OR equipo_visitante = %s)"
            params.extend([equipo, equipo])

        if fecha:
            query += " AND DATE(fecha) = %s"
            params.append(fecha)

        if fase:
            query += " AND fase = %s"
            params.append(fase)

        query += " LIMIT %s OFFSET %s" #esto es parte de la paginacion, lo recalca en el 4.2 del pdf
        params.extend([per_page, offset])

        cursor.execute(query, params) # ejecuto la query
        resultados = cursor.fetchall() #guardo los datos

        return jsonify(resultados), 200 #muestro la lista

    except Exception as e:
        return jsonify({"error": str(e)}), 500 #en caso de haber error lo muestro en el code
    
@partidos_bp.route("/partidos/<int:id>", methods=["GET"])
def obtener_partido(id):
    # el <int:id> agarra el numero que se pone en la url, ej: /partidos/5
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True) # dictionary=True para poder devolver JSON

        query = """
        SELECT p.id_partido, p.equipo_local, p.equipo_visitante, p.fecha, p.fase,
               p.goles_local, p.goles_visitante
        FROM partidos p
        WHERE p.id_partido = %s
        """
        # si no tiene resultado, los campos de goles van a venir como null, pero igual devuelve el partido
        cursor.execute(query, (id,))
        partido = cursor.fetchone() # fetchone porque busco uno solo por id

        if partido is None:
            return jsonify({"error": "Partido no encontrado"}), 404
        # si no existe el id en la tabla, devuelvo 404

        return jsonify(partido), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@partidos_bp.route("/partidos/<int:id>", methods=["DELETE"])
def eliminar_partido(id):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # primero verifico que el partido existe antes de intentar borrarlo
        cursor.execute("SELECT id_partido FROM partidos WHERE id_partido = %s", (id,))
        partido = cursor.fetchone()

        if partido is None:
            return jsonify({"error": "Partido no encontrado"}), 404

        # si existe, lo elimino
        cursor.execute("DELETE FROM partidos WHERE id_partido = %s", (id,))
        conn.commit() # guardo el cambio en la base de datos

        return jsonify({"mensaje": "Partido eliminado correctamente"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Endpoint PUT Obligatorio
@partidos_bp.route("/partidos/<int:id>/resultado", methods=["PUT"])  # Carga o actualiza el resultado de un partido
def cargar_resultado(id):
    try:
        data = request.get_json()  # Obtengo los datos que envió el usuario
        
        if 'goles_local' not in data or 'goles_visitante' not in data:  # Valida que el JSON tenga los goles del local y del visitante
            return jsonify({"error": "Faltan los goles en el cuerpo de la solicitud"}), 400  # Si falta alguno, devuelve error 400 (Bad Request)

        conn = get_connection()  # Conecto a la base de datos
        cursor = conn.cursor()  # Creo el cursor que ejecuta la secuencia en la base de datos y trae la confirmación

        # Ejecuta la sentencia SQL para actualizar (UPDATE) los goles en la tabla partidos (ya existen como NULL o con valor previo)
        query = """
            UPDATE partidos 
            SET goles_local = %s, goles_visitante = %s
            WHERE id_partido = %s
        """
        
        # El cursor ejecuta la actualización en la base de datos
        cursor.execute(query, (
            data['goles_local'], 
            data['goles_visitante'], 
            id
        ))
        
        conn.commit()  # Guarda los cambios

        if cursor.rowcount == 0:  # Si el cursor no afectó a ninguna fila en la tabla de partidos (base de datos)
            return jsonify({"error": "No se encontró el partido para cargar el resultado"}), 404  # Es porque el ID (partido) no existía

        return jsonify({"mensaje": "Resultado guardado correctamente"}), 200

    except Exception as e:  # Si algo sale mal (ej: error de conexión), devuelve el error
        return jsonify({"error": str(e)}), 500


# Endpoint PUT Opcional
@partidos_bp.route("/partidos/<int:id>", methods=["PUT"])  # Actualiza un partido completo (equipos, fecha, fase), según la ID
def actualizar_partido(id):
    try:
        data = request.get_json()
        
        # Valida los campos requeridos para el fixture según el Swagger
        campos_requeridos = ['equipo_local', 'equipo_visitante', 'fecha', 'fase']
        for campo in campos_requeridos:  # Valida que el usuario haya mandado todos los campos requeridos
            if campo not in data:
                return jsonify({"error": f"Falta el campo obligatorio: {campo}"}), 400  

        conn = get_connection()
        cursor = conn.cursor()

        # Actualiza la información del cronograma sin tocar los goles
        query = """
            UPDATE partidos 
            SET equipo_local = %s, equipo_visitante = %s, fecha = %s, fase = %s
            WHERE id_partido = %s
        """ 
        cursor.execute(query, (
            data['equipo_local'],
            data['equipo_visitante'],
            data['fecha'],
            data['fase'],
            id
        ))
        
        conn.commit()

        if cursor.rowcount == 0:
            return jsonify({"error": "Partido no encontrado para actualizar"}), 404

        return jsonify({"mensaje": "Datos del partido actualizados correctamente"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@partidos_bp.route("/partidos/<int:id>", methods=["PATCH"])
def actualizar_dato_partido(id):
    try:
        data = request.get_json()

        #Verifico que el body no este vacio
        if not data:
            return jsonify({"error": "El body no puede estar vacio"}), 400
        
        #Verifico que el partido existe
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT id_partido FROM partidos WHERE id_partido = %s", (id,))
        partido = cursor.fetchone()
        if partido is None:
         return jsonify({"error": "Partido no encontrado"}), 404
        
        #Agrego los campos que vinieron en el body
        campos = []
        valores = []

        if 'equipo_local' in data:
            campos.append("equipo_local = %s")
            valores.append(data['equipo_local'])

        if 'equipo_visitante' in data:
            campos.append("equipo_visitante = %s")
            valores.append(data['equipo_visitante'])

        if 'fecha' in data:
            campos.append("fecha = %s")
            valores.append(data['fecha'])

        if 'fase' in data:
            campos.append("fase = %s")
            valores.append(data['fase'])

        #Si no vino ningun campo valido, hay error
        if not campos:
            return jsonify({"error": "No se enviaron campos validos"}), 400

        #Armo y ejecuto el UPDATE
        set_clause = ", ".join(campos)
        valores.append(id)

        cursor = conn.cursor()
        cursor.execute(f"UPDATE partidos SET {set_clause} WHERE id_partido = %s", valores)
        conn.commit()

        return '', 204

    except Exception as e:
        return jsonify({"error": str(e)}), 500
