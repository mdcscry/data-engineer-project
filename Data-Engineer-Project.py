#!/usr/bin/env python
# coding: utf-8

# In[2]:


import numpy as np
import pandas as pd
import os
import ast
from pandasql import sqldf as sqldf


### Functions
pysqldf = lambda q: sqldf(q, globals())


###Data Cleansing
dirpath = os.getcwd()
dirpath

# Load data
moviedata = ""
moviedata = pd.read_csv('./movies_metadata.csv')
#moviedata.info()
moviedata.drop_duplicates(inplace=True)
moviedata = moviedata[["original_title",'release_date','production_companies','genres','budget','revenue','popularity']]

#messy
moviedata['budget'] = pd.to_numeric(moviedata['budget'], errors='coerce')
#moviedata['budget'] = moviedata['budget'].replace(0, np.nan)
#moviedata['revenue'] = moviedata['revenue'].replace(0, np.nan)
moviedata ['profit'] = moviedata['revenue'] - moviedata['budget']
#moviedata ['profit']

moviedata.loc[2222,'genres'] = moviedata.loc[2222]['genres'].replace(" GATORADE","]")


# In[ ]:


###Genre Dataset
gen_df = moviedata.head(500)

gen_df['genres'] = gen_df['genres'].fillna('[]').apply(ast.literal_eval)
gen_df['genres'] = gen_df['genres'].apply(lambda x: [i['name'] for i in x] if isinstance(x, list) else [])
gen_outerjoin = gen_df.apply(lambda x: pd.Series(x['genres']),axis=1).stack().reset_index(level=1, drop=True)
genre_dat_final = gen_outerjoin.to_frame().merge(gen_df,left_index=True, right_index=True)


for index, row in genre_dat_final.iterrows():
    #print(row['genres'])
    genre_dat_final.loc[index,'orig_genre_name']='|'.join(row['genres'])


#rename to exploded renamed to "genre_derived"


# In[ ]:


genre_dat_final[['orig_genre_name']]


# In[3]:


#production company dataset
pc_df = moviedata.head(500)

#explode the genre
pc_df['production_companies'] = pc_df['production_companies'].fillna('[]').apply(ast.literal_eval)
pc_df['production_companies'] = pc_df['production_companies'].apply(lambda x: [i['name'] for i in x] if isinstance(x, list) else [])
outerjoin = pc_df.apply(lambda x: pd.Series(x['production_companies']),axis=1).stack().reset_index(level=1, drop=True)

pc_dat = outerjoin.to_frame().merge(pc_df,left_index=True, right_index=True)

for index, row in pc_dat_final.iterrows():
    #print(row['genres'])
    pc_dat_final.loc[index,'orig_genre_name']='|'.join(row['genres'])


# In[ ]:




#NO Keys generated but model would

PRODUCTION_COMP_FACT = pysqldf("""SELECT production_companies
                               ,orig_genre_name
                               ,sum(budget) as budget
                               ,sum(revenue) as revenue
                               ,sum(profit) as profit
                               ,sum(popularity) as popularity
                               ,release_year
                               ,count(distinct id) as movie_count
                               FROM pc_dat group by release_year,production_companies,orig_genre_name ;""")




GENRES_FACT = pysqldf("""SELECT production_company_derived as production_company
                          ,genres
                          ,budget
                          ,revenue
                          ,profit
                          ,popularity
                          ,count(distinct id) as movie_count
                          FROM genre_dat_final;""")

GENRE_DIM =  pysqldf("""SELECT
                          keygen_seq()
                          ,genres_derived as genre_name
                          ,orig_genre_name as orig_genre_name
                          FROM gen_final_dat
                          UNION ALL
                          SELECT
                          keygen_seq()
                          ,orig_genre as genre_name
                          ,null as orig_genre_name
                          FROM genre_dat_final
                          """)

PRODUCTION_COMP_DIM =  pysqldf("""SELECT
                          keygen_seq()
                          ,production_company_derived as production_company
                          FROM pc_final_dat
                          """)


# In[ ]:


# PC extracts

#budget per year
#revenue per year
#profit per year
sql ="""select release_year
    ,production_company
    ,sum(budget) as budget
    ,sum(revenue) as revenue,
    sum(profit) as profit
    from PRODUCTION_COMPANY_FACT where [PARM=release year]
    group by release_year,production_company"""


#releases by genre per year
sql = """
select release_year
    ,production_company
    ,count( distinct gd.genre_name) # should be key
    from PRODUCTION_COMPANY_FACT join GENRE_DIM on
    pcf.orig_genre_name = gd.orig_genre_name where [PARM=release year]
    group by release_year,production_company,genre
;"""


#average popularity of produced movies per year
sql = """
select release_year
    ,production_company
    ,sum(movie_count) movie_num
    ,sum(popularity) popular_num
    ,max(popularity/movie_num) average_popularity //could be inline view and outer select depending on system
    from PRODUCTION_COMPANY_FACT where [PARM=release year]
    group by release_year,production_company
;"""







# In[ ]:


# Genre extracts
#most popular genre by year

sql ="""

select * from (
    select
    release_year
    ,genre
    , rank(sum(popularity) over release_year) popular_rank
    from GENRE_FACT where [PARM=release year]
    group by release_year,genre) inner
    where popular_rank <= [PARM]"""
    


#budget by genre by year
#revenue by genre by year
#profit by genre by year
sql ="""select release_year
    ,genre
    ,sum(budget) as budget
    ,sum(revenue) as revenue,
    sum(profit) as profit
    from GENRE_FACT where [PARM=release year]
    group by release_year,genre"""

