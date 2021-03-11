"""
Paola Vizcarra
10/29/2020

Using the database scraped from 
https://en.wikipedia.org/wiki/List_of_countries_by_carbon_dioxide_emissions,
sort the data by the Fossil CO2 Emissions 2017 (% of world) column.  
Extract the top 10 countries data, and plot them in a pie-graph using Matplotlib.
"""

import numpy as np
import matplotlib.pyplot as plt
from urllib.request import urlopen
import requests
from bs4 import BeautifulSoup
import sqlite3
import os

class MatPlotLib_Graph:
    def __init__(self, dataFile):
        self.labels = []
        self.sizes = []
        for i in dataFile:
            self.labels.append(i[0])
            self.sizes.append(i[1])
    def PieChart(self):
        # Pie chart, where the slices will be ordered and plotted counter-clockwise:
        explode = (0.1, 0, 0, 0, 0, 0, 0, 0, 0, 0)  # only "explode" 1st slice

        fig1, ax1 = plt.subplots()
        ax1.pie(self.sizes, explode=explode, labels=self.labels, autopct='%1.1f%%',
                shadow=True, startangle=180, pctdistance=0.83)
        ax1.set_title("Fossil CO2 emissions by Country")
        plt.show()

class BeautifulSoupScraper:
    def __init__(self):
        self.soupDict = { }
    def ReadFile(self, fileName):
        html = urlopen(fileName)
        soup = BeautifulSoup(html, 'html.parser')
        # find <table class: wikitable>
        table = soup.find("table", { "class" : "wikitable" })
        # find <tr>, start at 5th row
        for row in table.findAll("tr")[5:]:
            cells = row.findAll("td")
            if len(cells) == 8:
                cols=[x.text.strip() for x in cells]
                self.soupDict[cols[0]] = cols[4][:-1]
        return self.soupDict

class SQLite_database:
    # constructor
    def __init__(self, dbName) : 
        self.dbFile = dbName
        self.sqliteConnection = sqlite3.connect(self.dbFile) 
        self.cursor = self.sqliteConnection.cursor()

    # Create Database file
    def Connect(self):
        try:
            print("Database ", self.dbFile, \
                " created and Successfully Connected to SQLite")
            sqlite_select_Query = "select sqlite_version();"
            self.cursor.execute(sqlite_select_Query)
            record = self.cursor.fetchall()
            print("SQLite Database Version is: ", record)
            return True
        except sqlite3.Error as error:
            print("Error while connecting to sqlite", error)
            return False
                
    # Create Database Table
    def Table(self, dbQuery):
        try: 
            self.cursor.execute(dbQuery)
            self.sqliteConnection.commit()
            print("SQLite table created")
            return True
        except sqlite3.Error as error:
            print("Error while creating a sqlite table", error)
            return False

    # Insert data to Table
    def Insert(self, dbData, data_tuple):
        try:
            self.cursor.execute(dbData, data_tuple)
            self.sqliteConnection.commit()
            return True
        except sqlite3.Error as error:
            print("Failed to insert blob data into sqlite table", error)
            return False

    def ReadData4MatPlotLib(self, readQuery):
        listOfValues = {}
        try:
            self.cursor.execute(readQuery)
            records = self.cursor.fetchall()
            for row in records:
                listOfValues[row[0]] = row[1]
            return listOfValues
        except sqlite3.Error as error:
            print("Failed to read data from sqlite table", error)

    # Close Database       
    def Close(self):
        try:
            self.cursor.close()
        except sqlite3.Error as error:
            print("Error while closing", error)
        finally:
            if (self.sqliteConnection):
                self.sqliteConnection.close()
                print("The SQLite connection is closed")
                return True
            else:
                return False

    # Delete Database if found (to avoid errors later)
    def DeleteDatabase(self):
        if os.path.exists(self.dbFile):
            os.remove(self.dbFile)


def main():
    webSite = "https://en.wikipedia.org/wiki/List_of_countries_by_carbon_dioxide_emissions"
    myData = BeautifulSoupScraper()

    # Create Database
    dbEmissions = SQLite_database("SQLite_CO2Emissions.db")
    dbEmissions.Connect()
    # Create Table in Database
    dbEmissions.Table("CREATE TABLE Database ("\
        "ID INTEGER PRIMARY KEY, "\
        "Country STRING NOT NULL, "\
        "CO2World2017 FLOAT NOT NULL);")
    # Populate Table with Data: UniqueID, Country, CO2World2017
    ID = 0
    for key, value in myData.ReadFile(webSite).items():
            ID += 1
            country = key #int(key)
            co2emissions = value #float(value)
            dbEmissions.Insert("INSERT INTO Database"\
                "(ID, Country, CO2World2017) VALUES(?, ?, ?)",\
                (ID, country, co2emissions))
    
    # Get data from Table
    Country = dbEmissions.ReadData4MatPlotLib("SELECT Country, CO2World2017 from Database")
    # Close Table
    dbEmissions.Close()
    # Delete Table to avoid errors (when executing more than once)
    dbEmissions.DeleteDatabase()

    # Sort data (Country) in descending order, and return first 10 
    sorted_Country = (sorted(Country.items(), key=lambda x: x[1], reverse=True))[:10]

    # Pie Chart
    graph = MatPlotLib_Graph(sorted_Country)
    graph.PieChart()
    
if __name__ == "__main__":
    main()
