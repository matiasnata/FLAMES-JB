from flask import Flask #importo libreria flask
from routes.partidos import partidos_bp #traigo del partidos.py el blueprint
from routes.Prode import prode_bp

app = Flask(__name__) #creo la app, app seria el backend

app.register_blueprint(partidos_bp) # agrego las rutas de partidos a la app
app.register_blueprint(prode_bp)

if __name__ == "__main__":
    app.run(debug=True)
    #basicamente enciendo el servidor, es algo default de flask esto, con el debug para errores

