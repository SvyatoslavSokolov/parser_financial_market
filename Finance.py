import requests
import re
from datetime import datetime, timedelta
import mysql.connector
import numpy as np
import matplotlib.pyplot as plt
from tensorflow import keras
from tensorflow.keras.layers import Dense             


#Подключение к БД
mydb = mysql.connector.connect(host="localhost", user="root", password="root", database="finance")

#Обращение к БД 
mycursor = mydb.cursor()
count = mycursor.rowcount
mycursor.execute("SELECT symbol FROM stocks")
myresult = mycursor.fetchall()

#Тело функции
def data_stocks():
        print (x[0])
        #Дата
        try: r0 = requests.get('https://query1.finance.yahoo.com/v10/finance/quoteSummary/{0}?modules=price'.format(x[0]))
        except BaseException:
            print('Ошибка в имени: ', x[0])
        data0 = r0.json()
        #Полное имя
        try: longName = data0['quoteSummary']['result'][0]['price']['longName']
        except BaseException:
            print('longName ERROR ')
            longName = ''
        #Цена           
        try: price = data0['quoteSummary']['result'][0]['price']['regularMarketPrice']['raw']
        except BaseException:
            print('price ERROR ')
            price = ''
        #Изменение цены            
        try: pricechange = data0['quoteSummary']['result'][0]['price']['regularMarketChange']['fmt']
        except BaseException:
            print('pricechange ERROR ')
            pricechange = ''
        pricechange = str(pricechange).replace(".", ",")
        #Изменение цены в процентах        
        try: pricechangep = data0['quoteSummary']['result'][0]['price']['regularMarketChangePercent']['fmt']
        except BaseException:
            print('pricechangep ERROR ')
            pricechangep = ''
        #Тип валюты        
        try: typeprice = data0['quoteSummary']['result'][0]['price']['currency']
        except BaseException:
            print('typeprice ERROR ')
            typeprice = ''
        #Дивиденты        
        try: yields = data0['quoteSummary']['result'][0]['price']['postMarketChangePercent']['fmt']
        except BaseException:
            print('yields ERROR ')
            yields = ''    
        #Запись данных в mysql
        mycursor = mydb.cursor()
        sql = "UPDATE finance.stocks SET longname = %s, price = %s, pricechange = %s, pricechangep = %s, typeprice = %s, yields = %s WHERE symbol = %s "
        val = (longName,price,pricechange,pricechangep,typeprice,yields,x[0])
        mycursor.execute(sql, val)
        mydb.commit()

def info_stocks():
        
        try: r1 = requests.get('https://query1.finance.yahoo.com/v10/finance/quoteSummary/{0}?modules=assetProfile'.format(x[0]))
        except BaseException:
            print('Ошибка в имени: ', x[0])
        data1 = r1.json()
        
        #Город
        try: city = data1['quoteSummary']['result'][0]['assetProfile']['city']
        except BaseException:
            print('City ERROR ')
            сity = ''
        #Страна    
        try: country = data1['quoteSummary']['result'][0]['assetProfile']['country']
        except BaseException:
            print('Country ERROR ')
            сountry = ''
        #Сектор    
        try: sector = data1['quoteSummary']['result'][0]['assetProfile']['sector']
        except BaseException:
            print('Sector ERROR ')
            sector = ''
        #Индустрия    
        try: industry = data1['quoteSummary']['result'][0]['assetProfile']['industry']
        except BaseException:
            print('Industry ERROR ')
            industry = ''    
        #Запись данных в mysql
        mycursor = mydb.cursor()
        sql = "UPDATE finance.stocks SET city = %s, country = %s, sector = %s, industry = %s WHERE symbol = %s "
        val = (city,country,sector,industry,x[0])
        mycursor.execute(sql, val)
        mydb.commit()

#HISTORY
        
def history_test_column():

        try:
                mycursor = mydb.cursor()
                column_title = x[0]
                cell_type = "VARCHAR(45)"
                sql = "ALTER TABLE finance.history ADD (`%s` %s) " % (column_title, cell_type)
                mycursor.execute(sql)
                mydb.commit()
                print ('Столбик: ',x[0],' создан')
                
        except  BaseException:
                print ('Столбик: ',x[0],' уже создан')
                

#def history_get_requests

        try: r2 = requests.get('https://query1.finance.yahoo.com/v8/finance/chart/{0}?symbol={0}&period1=0&period2=9999999999&interval=1d'.format(x[0]))
        except BaseException:
               print('Ошибка в имени: ', x[0])
        data2 = r2.json()


        for i in data2:
                DateU = data2['chart']['result'][0]['timestamp']
                #Open = data2['chart']['result'][0]['indicators']['quote'][0]['open'] 
                #Volume=data2['chart']['result'][0]['indicators']['quote'][0]['volume']
                #High=data2['chart']['result'][0]['indicators']['quote'][0]['high']
                CloseU = data2['chart']['result'][0]['indicators']['quote'][0]['close']
                #Adjusted_Close=data2['chart']['result'][0]['indicators']['adjclose'][0]['adjclose']
                
#def history_check_date
        j=-1
        for i in DateU:
                j=j+1
                Date=[]
                Date.append(datetime.utcfromtimestamp(int(i)).strftime("%Y-%m-%d"))
                Date = str(Date).replace('[', '').replace(']', '').replace("'", "")
                
                column_title = Date
                column_price = CloseU[j]
                row_name = x[0]
                
                try:
                        mycursor = mydb.cursor()
                        sql = "INSERT INTO `finance`.`history` (`Date`) VALUES ('%s');" % (column_title)
                        mycursor.execute(sql)
                        mydb.commit()
                        print ('Строка: ',Date,' создана и обновлена: ',CloseU[j])
                        mycursor = mydb.cursor()
                        sql = "UPDATE `finance`.`history` SET `%s` = '%s' WHERE (`Date` = '%s');" % (row_name, column_price, column_title)
                        mycursor.execute(sql)
                        mydb.commit()                            
                except BaseException:
                        print ('Строка: ',Date,' уже создана и обновлена: ',CloseU[j])
                        mycursor = mydb.cursor()
                        sql = "UPDATE `finance`.`history` SET `%s` = '%s' WHERE (`Date` = '%s');" % (row_name, column_price, column_title)
                        mycursor.execute(sql)
                        mydb.commit()                                 

#Функции
       
for x in myresult:
        data_stocks() 
        info_stocks()
        history_test_column()
