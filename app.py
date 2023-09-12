from flask import Flask
from flask_restful import Api, Resource, request, abort, fields, marshal_with, reqparse, inputs
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func 
import pandas as pd
from datetime import date

app = Flask(__name__, instance_relative_config=True)
api = Api(app)
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

operateur_put_args = reqparse.RequestParser()
operateur_put_args.add_argument("nom", type=str, help="dénomination de la structure", required=True)
operateur_put_args.add_argument("cp", type=int, help="code postal siège social", required=True)
operateur_put_args.add_argument("date_engagement", type=inputs.date, help="date d'engagement au format YYYY-MM-DD", required=True)
operateur_put_args.add_argument("producteur", type=bool, help="l'opérateur est il un producteur ?", required=True)
operateur_put_args.add_argument("preparateur", type=bool, help="l'opérateur est il un preparateur ?", required=True)
operateur_put_args.add_argument("distributeur", type=bool, help="l'opérateur est il un distributeur ?", required=True)
operateur_put_args.add_argument("restaurateur", type=bool, help="l'opérateur est il un restaurateur ?", required=True)
operateur_put_args.add_argument("stockeur", type=bool, help="l'opérateur est il un stockeur ?", required=True)
operateur_put_args.add_argument("importateur", type=bool, help="l'opérateur est il un importateur ?", required=True)
operateur_put_args.add_argument("exportateur", type=bool, help="l'opérateur est il un exportateur ?", required=True)
operateur_put_args.add_argument("organisme_certificateur", type=str, help="organisme certificateur BIO", required=True)

operateur_patch_args = reqparse.RequestParser()
operateur_patch_args.add_argument("nom", type=str, help="dénomination de la structure")
operateur_patch_args.add_argument("cp", type=int, help="code postal siège social")
operateur_patch_args.add_argument("date_engagement", type=inputs.date, help="date d'engagement au format YYYY-MM-DD")
operateur_patch_args.add_argument("producteur", type=bool, help="l'opérateur est il un producteur ?")
operateur_patch_args.add_argument("preparateur", type=bool, help="l'opérateur est il un preparateur ?")
operateur_patch_args.add_argument("distributeur", type=bool, help="l'opérateur est il un distributeur ?")
operateur_patch_args.add_argument("restaurateur", type=bool, help="l'opérateur est il un restaurateur ?")
operateur_patch_args.add_argument("stockeur", type=bool, help="l'opérateur est il un stockeur ?")
operateur_patch_args.add_argument("importateur", type=bool, help="l'opérateur est il un importateur ?")
operateur_patch_args.add_argument("exportateur", type=bool, help="l'opérateur est il un exportateur ?")
operateur_patch_args.add_argument("organisme_certificateur", type=str, help="organisme certificateur BIO")

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
        producteur = False
        preparateur = False
        distributeur = False
        restaurateur = False
        stockeur = False
        importateur = False
        exportateur = False
        
        # On teste la présence des activitées dans le champ source pour alimenter les booléens de la BDD
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


class Operateur_bio(Resource):
    @marshal_with(resource_fields)
    def get(self, siret):
        result = Operateur.query.filter_by(siret=siret).first()
        if not result:
            abort(404, message="Pas d'opérateur trouvé en base pour le SIRET fourni")
        return result

    @marshal_with(resource_fields)
    def put(self, siret):
        args = operateur_put_args.parse_args()
        print (args)
        result = Operateur.query.filter_by(siret=siret).first()
        if result:
            abort(409, message="Un enregistrement existe déjà pour ce numéro SIRET")

        # On cherche le dernier numéro bio en base, pour définir celui du nouvel enregistrement (les numéros se suivent)
        max_numero_bio = db.session.query(func.max(Operateur.numero_bio)).first()
        
        operateur = Operateur(
            siret = siret, 
            numero_bio = max_numero_bio[0] + 1,
            nom = args['nom'],
            cp = args['cp'],
            date_engagement = args['date_engagement'],
            producteur = args['producteur'],
            preparateur = args['preparateur'],
            distributeur = args['distributeur'],
            restaurateur = args['restaurateur'],
            stockeur = args['stockeur'],
            importateur = args['importateur'],
            exportateur = args['exportateur'],
            organisme_certificateur = args['organisme_certificateur']
            )
        db.session.add(operateur)
        db.session.commit()
        return operateur, 201
    
    @marshal_with(resource_fields)
    def patch(self, siret):
        args = operateur_patch_args.parse_args()
        # On teste si un enregistrement existe pour ce SIRET avant d'essayer de le mettre à jour
        result = Operateur.query.filter_by(siret=siret).first()
        if not result:
            abort(404, message="Pas d'opérateur trouvé en base pour le SIRET fourni")

        # Si les arguments sont renseignés dans le json reçu, on modifie les champs concernés
        if args['nom']:
            result.nom = args['nom']
        if args['cp']:
            result.cp = args['cp']
        if args['date_engagement']:
            result.date_engagement = args['date_engagement']
        if args['producteur']:
            result.producteur = args['producteur']
        if args['preparateur']:
            result.preparateur = args['preparateur']
        if args['distributeur']:
            result.distributeur = args['distributeur']
        if args['restaurateur']:
            result.restaurateur = args['restaurateur']
        if args['stockeur']:
            result.stockeur = args['stockeur']
        if args['importateur']:
            result.importateur = args['importateur']
        if args['exportateur']:
            result.exportateur = args['exportateur']
        if args['organisme_certificateur']:
            result.organisme_certificateur = args['organisme_certificateur']

        db.session.commit()
        return result
    
    def delete(self, siret):
        # On teste si un enregistrement existe pour ce SIRET avant d'essayer de le supprimer
        result = Operateur.query.filter_by(siret=siret).first()
        if not result:
            abort(404, message="Pas d'opérateur trouvé en base pour le SIRET fourni")

        Operateur.query.filter_by(siret=siret).delete()
        db.session.commit()
        return '', 204

api.add_resource(Operateur_bio, "/api/v1/resources/operateur/<int:siret>")

if __name__ == '__main__':    
    app.run(debug=True)