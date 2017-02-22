# -*- coding: utf-8 -*-
"""
Created on Sat Feb 18 13:56:58 2017

@author: sylvain
"""

""" Import des packages """
import re
import math
import pprint
import pymongo
from pymongo import MongoClient

""" connection à la base de données mongodb """
client = MongoClient('mongodb://localhost:27017/')
db = client.NYC

""" Une variable par collection """
health = db.health
subway = db.subway
business = db.business
resto=db.resto

""" Dans les données, la 1er ligne a été répliqué (je n'en connais pas la raison) c'est pourquoi on la supprime ici """
resto.remove({"Primary":"Primary"})

health.count() 
""" 78 """
subway.count() 
""" 1928 """
business.count() 
""" 76495 """
resto.count() 
"""771"""

""" Faisons un compte du nombre de restaurant par code postal """
resresto=resto.aggregate([{"$group":{"_id":"$CnAdrPrf_ZIP","count":{"$sum":1}}},{"$sort": {"count":-1}}])
bestcode=list(resresto)[0]
pprint.pprint(bestcode) 
""" Il semble avantageux d'habiter dans le zip code 10038 """

""" Regardons si il y a des hopitaux dans 1 de ces code postaux """
health.count({'location_1_zip':repr(bestcode['_id'])}) 
""" Il y en a 1 ! ce lieu semble parfait pour y vivre """
health.count({'location_1_zip':'10004'}) 
""" Il y en a pas ..."""


""" Regardons quel type de resto il y a dans le 10038, e théorie on ne devrait pas se faire de soucie pour la variété, il y a 308 resto ..."""
resresto=resto.aggregate([{"$match":{"CnAdrPrf_ZIP":bestcode['_id']}},{"$group":{"_id":"$Primary","count":{"$sum":1}}},{"$sort": {"count":1}}])
pprint.pprint(list(resresto))

resresto=resto.aggregate([{"$match":{"CnAdrPrf_ZIP":bestcode['_id']}},{"$group":{"_id":"$Secondary","count":{"$sum":1}}},{"$sort": {"count":1}}])
pprint.pprint(list(resresto))


""" Regardons quels type de commerce se trouve à coté """
resbusiness=business.aggregate([{"$match":{"Address ZIP":bestcode['_id']}},{"$group":{"_id":"$License Category","count":{"$sum":1}}},{"$sort": {"count":1}}])
pprint.pprint(list(resbusiness)) 
""" Interessant il y a une grande diversité de commerces notamment une dizaine de magasin d'électronics et de vente d'occasion"""


""" Maintenant que nous connaissons a peu près le lieu dans lequel nous voulons vivre (le code postal 10038) il faut maintenant déterminer les coordonnées exacte """
""" Pour déterminer les coordonnées j'ai décidé de calculer les coordonnées moyennes (de la latitude/longitude) """
""" Les lieux intéressant sont : L'hopital (avec un très gros poids), les magasin d'electronique, les ventes d'occasions et les personnes pouvant vendre des cigarettes étant donnée que je fume. """
""" Je ne prends pas en compte les restaurant dans ce calcul étant donnée qu'il y a beaucoup (308) et que nous n'avons pas accès à leur coordonnées """
""" xMoy:longitude moyenne, yMoy:latitude moyenne """
xMoy=0
yMoy=0
""" Poid utilisé pour le calcul de la moyenne"""
nbLieux=0

"""Calcul de la moyenne""" 
for doc in business.find({"Address ZIP":bestcode['_id']}): 
    if doc['Longitude']=='' or doc['Latitude']=='':
        x=0
        y=0
    else:
        if 'cigarette' in doc['License Category'].lower() or 'secondhand' in doc['License Category'].lower() or 'electronic' in doc['License Category'].lower():                       
            nbLieux=nbLieux+2
            x=2*float(doc['Longitude'])
            y=2*float(doc['Latitude']) 
        else:
            nbLieux=nbLieux+1
            x=float(doc['Longitude'])
            y=float(doc['Latitude'])
    xMoy=xMoy+x
    yMoy=yMoy+y

for doc in health.find({'location_1_zip':repr(bestcode['_id'])}):
    nbLieux=nbLieux+50
    x=50*doc['location_1']['coordinates'][0]
    y=50*doc['location_1']['coordinates'][1]
    xMoy=xMoy+x
    yMoy=yMoy+y

yMoy=yMoy/nbLieux
xMoy=xMoy/nbLieux

""" Les coordonnées du lieux idéal sont en [xMoy,yMoy] """
""" Avec l'approximation que la terre soit sphérique, une minute de latitude = 1,852 km.Une minute de longitude = (1,852 km)*cos(longitude) (une minute de lalitude (resp longitude) vaut 1/60*(1 degrès de latitude) (resp 1 degrès de longitude))"""
""" yMoyk et xMoyk sont les coordonnées en km """
yMoyk=yMoy*(1.852*60) 
xMoyk=xMoy*(1.852*60*math.cos(math.radians(xMoy)))

""" CALCUL DU LIEU REELLE FINAL """
""" xfin et yfin sont les coordonées (en degrès) d'un batiment qui est proche du batiment idéal et qui existe vraiment """
yfin=0
xfin=0

""" distance entre le batiment et [xMoyk,yMoyk]"""
dist=0

""" Adresse du batiment et donc de la rue final"""
add=''

''' Avec la BDD business '''
for doc in business.find({'Address ZIP':bestcode['_id']}):
    if doc['Longitude']!='' and doc['Latitude']!='':
        """Passage de degrès en km"""
        x=float(doc['Longitude'])*(1.852*60*math.cos(math.radians(float(doc['Longitude'])))) 
        y=float(doc['Latitude'])*(1.852*60)
        """ calcul de la distance """
        d=math.sqrt((x-xMoyk)*(x-xMoyk)+(y-yMoyk)*(y-yMoyk))
        if d<dist or add=='':
            print d
            xfin=float(doc['Longitude'])
            yfin=float(doc['Latitude'])
            add=str(doc['Address Street Name'])
            dist=d
print add
print xfin
print yfin
""" Le batiment est a 10km du lieu idéal théorique, c'est parce que celui-ci était dans la rivière"""
print d

""" On va a présent chercher le nombre de metro situé a environ 500m du lieu de résidence. On cherche e2 (en degrès) qui correspond a un décalage de 500m en latitude. De même on cherche e1 qui correspond a un décalage de 500m en longitude """
""" conversion mètre -> latitude/longitude """
e2=0.5/(1.852*60)
e1=0.5/(1.852*60*math.cos(math.radians(xfin)))

nbSubway=0
for doc in subway.find():
    """ Les données géographiques sont de la forme "POINT (X Y)" ,je fais un traitement afin de récupérer uniquement X et Y séparément"""
    geo=doc['the_geom']
    geo=geo.replace('(','')
    geo=geo.replace(')','')
    li=re.split(' ',geo)
    x=float(li[1])
    y=float(li[2])
    """ On regarde si l'entrée se situe dans le carré [xMoy +- e1, yMoy +- e2] de centre, le lieux de résidence et longueur de coté 1km """
    if x <= (xfin+e1) and x >= (xfin-e1):
        if y <= (yfin+e2) and y >= (yfin-e2):
            nbSubway=nbSubway+1

print nbSubway 
""" 79 Entrées de metro, c'est beaucoup plus que 5 ! """













