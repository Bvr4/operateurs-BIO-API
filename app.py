from flask import Flask
from flask_restful import Api, request, abort, fields, marshal_with
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
from datetime import date

# pd.options.mode.chained_assignment = None

app = Flask(__name__, instance_relative_config=True)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///conso_database.db'
db = SQLAlchemy(app)    

class Operateur(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    siret = db.Column(db.BigInteger, nullable=False)
    numero_bio = db.Column(db.BigInteger, nullable=False)
    nom = db.Column(db.String(150), nullable=False)
    cp = db.Column(db.Integer, nullable=False)
    date_engagement = db.Column(db.Date, nullable=False)
    producteur = db.Column(db.Boolean)
    preparateur = db.Column(db.Boolean)
    distributeur = db.Column(db.Boolean)
    restaurateur = db.Column(db.Boolean)
    stockeur = db.Column(db.Boolean)
    importateur = db.Column(db.Boolean)
    exportateur = db.Column(db.Boolean)
    organisme_certificateur = db.Column(db.String(100), nullable=False)

resource_fields = {
    'siret' : fields.Integer,
    'numero_bio' : fields.Integer,
    'nom' : fields.String,
    'cp' : fields.Integer,
    'date_engagement' : fields.String, 
    'producteur' : fields.Boolean, 
    'preparateur' : fields.Boolean, 
    'distributeur' : fields.Boolean, 
    'restaurateur' : fields.Boolean, 
    'stockeur' : fields.Boolean, 
    'importateur' : fields.Boolean, 
    'exportateur' : fields.Boolean, 
    'organisme_certificateur' : fields.String 
}

# Page d'Accueil
@app.route('/', methods=['GET'])
def home():
    return '''<h1>Opérateurs BIO</h1>
<p>Ce site est le prototype d’une API mettant à disposition des données sur les opérateurs BIO en France.</p>'''

# Initialisation de la BDD avec les dernières données disponibles sur le portail open data.
@app.route('/create_init_db')
def create_init_db():
    data = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/657789db-d349-4554-aef6-eabde4bd1c57', sep=';', encoding='iso8859_3') 

    # Comme il s'agit uniquement d'un programme de test, on peut se permettre de faire un peu de ménage dans les enregistrements, pour éviter les erreurs
    data['CODE POSTAL SIEGE SOCIAL'] = pd.to_numeric(data['CODE POSTAL SIEGE SOCIAL'], errors='coerce')
    data['NUMERO BIO'] = pd.to_numeric(data['NUMERO BIO'], errors='coerce')
    data['DATEENGAGEMENT'] = pd.to_datetime(data['DATEENGAGEMENT'], format='%Y-%m-%d', errors='coerce')
    data = data.dropna()

    # Création de la BDD
    db.create_all()

    # On parcourt les données pour les mettre en forme et les insérer dans la BDD
    for i in data.index:
        print(f"{data['SIRET'][i]} - {data['DATEENGAGEMENT'][i]}")
        producteur = False
        preparateur = False
        distributeur = False
        restaurateur = False
        stockeur = False
        importateur = False
        exportateur = False
        
        if isinstance(data['ACTIVITES'][i], str):
            if 'Production' in data['ACTIVITES'][i]:
                producteur = True
            if 'Préparation' in data['ACTIVITES'][i]:
                preparateur = True
            if 'Distribution' in data['ACTIVITES'][i]:
                distributeur = True
            if 'restauration' in data['ACTIVITES'][i]:
                restaurateur = True
            if 'stockage' in data['ACTIVITES'][i]:
                stockeur = True
            if 'importation' in data['ACTIVITES'][i]:
                importateur = True
            if 'exportation' in data['ACTIVITES'][i]:
                exportateur = True        

        operateur = Operateur(
            siret = data['SIRET'][i],
            numero_bio = int(data['NUMERO BIO'][i]),
            nom = data['DENOMINATION'][i],
            cp = data['CODE POSTAL SIEGE SOCIAL'][i],
            date_engagement = data['DATEENGAGEMENT'][i],
            producteur = producteur,
            preparateur = preparateur,
            distributeur = distributeur,
            restaurateur = restaurateur,
            stockeur = stockeur,
            importateur = importateur,
            exportateur = exportateur,
            organisme_certificateur = data['ORGANISME CERTIFICATEUR'][i]
        )
        db.session.add(operateur)
        
    db.session.commit()
    return 'Base de donnée initiale générée avec succès'


if __name__ == '__main__':    
    app.run(debug=True)