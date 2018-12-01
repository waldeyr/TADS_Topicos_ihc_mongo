# sudo apt-get install python3-pip
# sudo pip install pandas
# sudo python3 -m pip install pip --upgrade
# sudo python3 -m pip install pymongo

from pymongo import MongoClient
import pandas as pd
import bson

class Aula_IHC_Topicos:
    CONN = MongoClient(port=27017)
    DB = CONN.mycollection

    def loadBeers(self):
        return pd.read_csv('beers.csv', sep=',').iterrows()

    def loadBreweries(self):
        return pd.read_csv('breweries.csv', sep=',').iterrows()

    def loadCategories(self):
        return pd.read_csv('categories.csv', sep=',').iterrows()

    def loadStyles(self):
        return pd.read_csv('styles.csv', sep=',').iterrows()

    def insertDataIntoMongoDB(self, data):
        result = self.DB.reviews.insert_one(data)
        print('Created: {0} '.format(result.inserted_id))
        return


aula = Aula_IHC_Topicos()

for index, beerLine in aula.loadBeers():
    data = {}
    beerName = str(beerLine.values[2])
    data.update({'beer': beerName})
    for index, breweryLine in aula.loadBreweries():
        if breweryLine.values[0] == beerLine.values[1]:
            breweryName = str(breweryLine.values[1])
            breweryCountry = str(breweryLine.values[7])
            breweryCity = str(breweryLine.values[4])
            data.update({'brewery': breweryName})
            data.update({'country': breweryCountry})
            data.update({'city': breweryCity})
    aula.insertDataIntoMongoDB(data)
