import psycopg2
import re
import time
from statistics import median

#############

def execute_query_main(f_name, db, n, path, dic):
    
    # Connect to server
    
    connection = psycopg2.connect(dbname= db, user='postgres', password='Cojone12-')
    cursor = connection.cursor()
    
    time_lst = [] # collect different execution time
    
    for _ in range(n):
    
        # Clean query:
    
        txt = open(path + f_name , 'r')
        query_raw = re.sub(r'[\t|\n]', ' ', txt.read())
        query = re.sub(r' +', ' ', query_raw).strip()
        
        # Evaluate:
        
        start = time.time()
        cursor.execute(query)
        end = time.time()
        
        # Add value:
        
        time_lst.append(end - start)

    # Close connection

    cursor.close()
    connection.close()
    
    # what we want:
    
    name = " ".join(re.sub(r'\.txt','',f_name).split('_'))
    med_t = round(median(time_lst), 3)
    
    dic[name] = med_t

def execute_query_index(f_name, n, path, dic):
    
    connection = psycopg2.connect(dbname= 'Olist', user='postgres', password='Cojone12-')
    cursor = connection.cursor()
    
    time_lst = [] # collect different execution time
    
    for _ in range(n):
    
        # Clean query:
    
        txt = open(path + f_name , 'r')
        query_raw = re.sub(r'[\t|\n]', ' ', txt.read())
        query = re.sub(r' +', ' ', query_raw).strip()
        
        # Evaluate:
        
        cursor.execute('CREATE INDEX idx_installments ON payments (payment_installments)')    
        start = time.time()
        cursor.execute(query)
        end = time.time()
        cursor.execute('DROP INDEX idx_installments') 
        
        # Add value:
        
        time_lst.append(end - start)

    # Close connection

    cursor.close()
    connection.close()
    
    # what we want:
    
    name = " ".join(re.sub(r'\.txt','',f_name).split('_'))
    med_t = round(median(time_lst), 3)
    
    dic[name] = med_t
