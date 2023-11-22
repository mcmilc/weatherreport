import mysql.connector
from southbayweather.weatherAPI.weatherClient import WeatherClient

mydb = mysql.connector.connect(host="localhost", user="root", password="IlMySQL48!")
print(mydb)


def populate_historical_temperature(
    db_connection: mysql.connector.MySQLConnection,
    start_date: str,
    end_date: str,
    weatherClient: WeatherClient,
):
    pass
