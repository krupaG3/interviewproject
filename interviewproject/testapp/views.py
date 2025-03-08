from django.shortcuts import render
import csv
import os
import mysql.connector
from django.http import HttpResponse

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Krupa@321',
    'database': 'interviewdatabase'
}

def extract_data(file_name, region):
    data = []
    base_dir = os.path.dirname(os.path.abspath(__file__))  
    file_path = os.path.join(base_dir, "templates", "testapp", file_name)

    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                try:
                    row['region'] = region
                    row['QuantityOrdered'] = int(row.get('QuantityOrdered', 0))  
                    row['ItemPrice'] = float(row.get('ItemPrice', 0.0))  
                    row['PromotionDiscount'] = float(row.get('PromotionDiscount', 0.0))
                    data.append(row)
                except ValueError:
                    print(f"Skipping row due to conversion error: {row}")
    except FileNotFoundError:
        print(f"Error: File '{file_name}' not found at {file_path}")

    return data


def transfer_data(data):
    transfer = []  
    duplicated_values = set()  

    for x in data:
        try:
            x = x.copy() 
            x['total_sales'] = x['QuantityOrdered'] * x['ItemPrice']
            x['net_sale'] = x['total_sales'] - x['PromotionDiscount']

            if x['net_sale'] > 0 and x.get('OrderID') not in duplicated_values:
                transfer.append(x)
                duplicated_values.add(x['OrderID'])

        except KeyError as e:
            print(f"Skipping row due to missing key: {e}")
        except TypeError as e:
            print(f"Skipping row due to type error: {e}")

    return transfer


def load_data(data):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS sales_data (
        OrderID VARCHAR(255) PRIMARY KEY,
        OrderItemId VARCHAR(255),
        QuantityOrdered INT,
        ItemPrice FLOAT,
        PromotionDiscount FLOAT,
        total_sales FLOAT,
        net_sale FLOAT,
        region VARCHAR(10)
    )
    """
    cursor.execute(create_table_query)

    insert_query = """
    INSERT INTO sales_data (OrderID, OrderItemId, QuantityOrdered, ItemPrice, PromotionDiscount, total_sales, net_sale, region)
    VALUES (%(OrderID)s, %(OrderItemId)s, %(QuantityOrdered)s, %(ItemPrice)s, %(PromotionDiscount)s, %(total_sales)s, %(net_sale)s, %(region)s)
    ON DUPLICATE KEY UPDATE 
    OrderItemId = VALUES(OrderItemId), 
    QuantityOrdered = VALUES(QuantityOrdered), 
    ItemPrice = VALUES(ItemPrice),
    PromotionDiscount = VALUES(PromotionDiscount), 
    total_sales = VALUES(total_sales), 
    net_sale = VALUES(net_sale), 
    region = VALUES(region)
    """

    try:
        cursor.executemany(insert_query, data) 
        conn.commit()
        print("Data inserted successfully!")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def process_csv(request):
    file_list = [("file1.csv", "North"), ("file2.csv", "South")]  # List of (filename, region)

    all_data = []
    
    for file_name, region in file_list:
        data = extract_data(file_name, region)
        cleaned_data = transfer_data(data)
        all_data.extend(cleaned_data)  

    if all_data:
        load_data(all_data)
        return HttpResponse("CSV files processed and loaded into the database successfully!")
    else:
        return HttpResponse("No valid data found in the files.")





#QUESTION 2 : 
import requests
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.generics import ListAPIView
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status


db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Krupa@321',
    'database': 'interviewdatabase'
}


# def create_table():
#     def create_table():
#     conn= mysql.connector.connect(**db_config)
#     cursor = conn.cursor()
#     cursor.execute('''
#         CREATE TABLE IF NOT EXISTS jokes (
#             Id INT AUTO_INCREMENT PRIMARY KEY,
#             category VARCHAR(255),
#             Type VARCHAR(50),
#             joke TEXT,
#             setup TEXT,
#             delivery TEXT,
#             nsfw BOOLEAN,
#             political BOOLEAN,
#             sexist BOOLEAN,
#             safe BOOLEAN,
#             lang VARCHAR(10)'
#         '''
#         )
#     conn.commit()
#     conn.close()

# create_table()