# sudo apt-get install python3-pip
# sudo pip install pandas
# sudo python3 -m pip install pip --upgrade
# sudo python3 -m pip install pymongo

from pymongo import MongoClient
import pandas as pd


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
        '''
        Example of data:
        data = {
            'name': 'AAAAA',
            'brewery': 'AAAAA'
        }
        '''
        result = self.DB.reviews.insert_one(data)
        print('Created: {0} '.format(result.inserted_id))
        return


aula = Aula_IHC_Topicos()

for index, linha in aula.loadStyles():
    print(linha)
