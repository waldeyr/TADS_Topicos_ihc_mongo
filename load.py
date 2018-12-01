# sudo apt-get install python3-pip
# sudo pip install pandas
# sudo python3 -m pip install pip --upgrade
# sudo python3 -m pip install pymongo

from pymongo import MongoClient
import pandas as pd
import bson


class Aula_IHC_Topicos:
    # Database connection
    CONN = MongoClient('mongodb://localhost:27017')
    # Database name
    DB = CONN.beerdb

    def loadBeers(self):
        return pd.read_csv('beers.csv', sep=',').iterrows()

    def loadBreweries(self):
        return pd.read_csv('breweries.csv', sep=',').iterrows()

    def loadCategories(self):
        return pd.read_csv('categories.csv', sep=',').iterrows()

    def loadStyles(self):
        return pd.read_csv('styles.csv', sep=',').iterrows()

    def insertDataIntoMongoDB(self, data):
        # Insert into collection beerinfo
        result = self.DB.beerinfo.insert_one(data)
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
    for index, styleLine in aula.loadStyles():
        if styleLine.values[0] == beerLine.values[4]:
            styleName = str(styleLine.values[2])
            data.update({'style': styleName})
    for index, categoryLine in aula.loadCategories():
        if categoryLine.values[0] == beerLine.values[3]:
            categoryName = str(categoryLine.values[1])
            data.update({'category': categoryName})
    aula.insertDataIntoMongoDB(data)
