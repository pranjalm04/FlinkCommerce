import json
import time
from faker import Faker
from logger import LoggerConfig
from kafka_config import KafkaProducerSingleton
import random
import datetime
import psycopg2
import configparser

currencyCodes = [
    'USD', 'EUR', 'JPY', 'GBP', 'AUD', 'CAD', 'CHF', 'CNY', 'SEK', 'NZD',
    'MXN', 'SGD', 'HKD', 'NOK', 'KRW', 'TRY', 'RUB', 'ZAR', 'BRL',
    'IDR', 'PLN', 'PHP', 'THB', 'MYR', 'CZK', 'HUF', 'ILS', 'CLP', 'AED',
    'COP', 'INR', 'KES', 'EGP', 'LKR', 'VND', 'QAR', 'BHD', 'KWD', 'OMR'
]
logger=LoggerConfig(name='FlinkCommerce',log_file='C:\\Users\\pranj\\Desktop\\PycharmProjects\\FlinkCommerce\\logs\\currency_conversion.log').logger
kafka=KafkaProducerSingleton()
fake=Faker()
def getConnection():
    configs=configparser.ConfigParser()
    configs.read('configs.conf')
    conn=""
    db_configs={
        "dbname":configs.get('Database','dbname'),
        "user":configs.get('Database','username'),
        "password":configs.get('Database','password'),
        "host":configs.get('Database','host'),
        "port":int(configs.get('Database','port'))
    }
    print(db_configs)
    try:
        conn=psycopg2.connect(dbname='flink_db',user='postgres',password='postgres',host='localhost',port=5432)
    except Exception as e:
        print('s')
        logger.info('cannot create a connection')
    return conn
def InsertData(conn,values,table):

    cursor=conn.cursor()
    try:
        cursor.execute('INSERT INTO {} (from_currency,to_currency,exchange_rate,last_updated)'.format(table)+\
                       'values(%s,%s,%s,%s)',values)
        conn.commit()
        logger.info("Inserted values {} in {} ".format(values,table))
    except Exception as e:
        logger.error("Error occured while inserting values",e)
        conn.rollback()
    finally:
        cursor.close()

def readSql(sql):
    try:
        conn=getConnection()
        cursor=conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        return results
    except Exception as e:
        logger.error("Cannot execute sql",e)
        return []
    finally:
        cursor.close()
        conn.close()
def executeSql(sql):
    conn=getConnection()
    cursor=conn.cursor()
    try:
        cursor.execute(sql)
        cursor.commit()

    except Exception as e:
        logger.error("Error occured while inserting values", e)
        conn.rollback()
    finally:
        cursor.close()


def generateData():
    conn=getConnection()
    for i in range(0,len(currencyCodes)):
        for j in range(0,len(currencyCodes)):
            if(currencyCodes[i]==currencyCodes[j]):
                exchangeRate=1
            else:
                exchangeRate=random.uniform(1,200)

            values=(currencyCodes[i],currencyCodes[j],exchangeRate,datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            InsertData(conn,values,'currency_conversion_rates')
    conn.close()

def simulateExchangeUpdate():
    kafka=KafkaProducerSingleton()
    currencyExchange=dict()
    while(True):

        currencyExchange['fromCurrency']=random.choice(currencyCodes)
        currencyExchange['toCurrency'] = random.choice(currencyCodes)
        currencyExchange['exchangeRate']= random.randint(1,5)
        currencyExchange['lastUpdated']= datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z')

        try:
            kafka.send(topic='currencyExchange',key=fake.uuid4(),value=json.dumps(currencyExchange).encode('utf-8'))
        except Exception as e:
            logger.error('Could not send values in topic currencyExchange')
            continue
        time.sleep(random.randint(1, 5))
    kafka.close()


if __name__=='__main__':
    results=readSql("select * from currency_conversion_rates")
    if len(results)!=0:
        generateData()
    simulateExchangeUpdate()


