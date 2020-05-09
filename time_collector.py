import re
import os

import pandas as pd

from utils import execute_query_main
from utils import execute_query_index

import matplotlib.pyplot as plt
import seaborn as sns

# Work on multiprocessing

import multiprocessing
from functools import partial

# Paths to different knd of queries:
    
PATH_M = '/Users/Dario/Google Drive/DS/First Year - Secon Semester/DM/homeworks/evaluate_time/queries/'
PATH_I = '/Users/Dario/Google Drive/DS/First Year - Secon Semester/DM/homeworks/evaluate_time/queries/opt_index/'
PATH_R = '/Users/Dario/Google Drive/DS/First Year - Secon Semester/DM/homeworks/evaluate_time/queries/opt_raw/'
PATH_S = '/Users/Dario/Google Drive/DS/First Year - Secon Semester/DM/homeworks/evaluate_time/queries/opt_schema/'

# Generate a dictionary that can be modified

manager = multiprocessing.Manager()
query_time = manager.dict()

# Woek on queries using olist (with constraints), index excluded: 

lst_query = [file for file in os.listdir(PATH_M) if re.search(r'\.txt', file)]
lst_1 = lst_query[0:3]
lst_2 = lst_query[3:6]
lst_3 = lst_query[6:9]
lst_4 = lst_query[9:12]

with multiprocessing.Pool(processes = 12) as pool:
            
    execute_q = partial(execute_query_main, db = 'Olist', n = 20, 
                        path = PATH_M, dic = query_time) 
    
    query1_3 = pool.map(execute_q, lst_1)
    query3_6 = pool.map(execute_q, lst_2)
    query6_9 = pool.map(execute_q, lst_3)
    query9_13 = pool.map(execute_q, lst_4)
    
# Query on index on olist with constraints:

q_name = [file for file in os.listdir(PATH_I) if re.search(r'\.txt', file)]
execute_query_main(*q_name, 'Olist', 20, PATH_I, query_time)
execute_query_index(*q_name, 20, PATH_I, query_time)

# Query on db with new schema:

lst_query = [file for file in os.listdir(PATH_S) if re.search(r'\.txt', file)]
for fle in lst_query:
    execute_query_main(fle, 'olist_geo_mod', 20, PATH_S, query_time)

# Query on database without costraints:

lst_query = [file for file in os.listdir(PATH_R) if re.search(r'\.txt', file)]
lst_1 = lst_query[0:3]
lst_2 = lst_query[3:6]
lst_3 = lst_query[6:9]
lst_4 = lst_query[9:12]

with multiprocessing.Pool(processes = 8) as pool:
            
    execute_q = partial(execute_query_main, db = 'olist_raw', n = 20, 
                        path = PATH_R, dic = query_time) 
    
    query1_3 = pool.map(execute_q, lst_1)
    query3_6 = pool.map(execute_q, lst_2)
    query6_9 = pool.map(execute_q, lst_3)
    query9_13 = pool.map(execute_q, lst_4)

# Prepare DataFrme:
    
df = pd.DataFrame.from_dict(query_time, orient = 'index', columns = ['median_time'])
df.reset_index(inplace = True)
df.sort_values(['index'], inplace = True)
df['class'] = [1,1,1,10,10,10,2,2,3,3,4,4,5,5,6,6,7,7,8,8,8,9,9,9,9]
df.sort_values(['class', 'median_time'], inplace = True, ignore_index = True,
               ascending=[True, False])
df.columns = ['query', 'median_time', 'class']

df['color'] = pd.Series(pd.factorize(df['class'])[0]).map(
                              lambda x: sns.color_palette("Paired")[x])# Assign a color to classes

# Plot the results:
     
plt.figure(figsize=(20,20))
sns.barplot(y = 'query', x = 'median_time', data = df, orient = 'h',
            palette = df['color']) 
plt.xlabel('median time (ms)', fontsize = 24, labelpad = 30)
plt.xticks(fontsize = 22)
plt.yticks(fontsize = 22)
plt.ylabel('')
plt.title('Query execution time', pad = 30, fontsize = 30)
plt.legend(title = '', labels='')
plt.show()


