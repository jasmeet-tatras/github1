import re
import time
import pandas as pd


DATABASE_USERNAME = "mysroot"
DATABASE_PASSWORD = "dG1XzfRVz9j0j&qA"
DATABASE_IP = "127.0.0.1"
DATABASE_NAME = "cssl_report_test"


"""
df_school=pd.read_csv('AP_SLAS_2019_Round2_School_Sub_Master_DB.csv')

#print(df_paper['Project'])

strt=time.time()

df_school['SchoolCode'] = df_school['SchoolCode'].astype('str')
df_school['SchoolCode'] = df_school['SchoolCode'].apply(lambda x:x.strip())
df_school['Project']=df_school['Project'].apply(lambda x:re.sub(r'(^\s|\s$)',r'',x))



#print(df_school['SchoolCode'],time.time()-strt)
print('g',end='')
print(re.sub(r'\s',r'','Andhra Pradesh SLAS'),end='')
print('g')"""



from sqlalchemy import create_engine, func
from sqlalchemy.exc import ProgrammingError, InterfaceError, DatabaseError
#from CSSLREPORT-BACKEND.config import DATABASE_IP, DATABASE_NAME, DATABASE_PASSWORD, DATABASE_USERNAME


FILE_CHECK_DESIRE_DATA_KEYS = ["school_submaster_csv", "paper_csv", "answer_csv","project_map_csv", "state_indicator",
                               "district_indicator","block_indicator","treatment_indicator","partner_indicator",
                               "area_indicator","round_year_detail"]
INGEST_PROJECT_DESIRE_DATA_KEYS = ["school_submaster_csv", "answer_csv", "paper_csv", "project_map_csv", "project_id",
                      "project_round_id", "round_year_detail", "Class", "partner", "block_indicator",
                      "project_type_id"]
PIPELINE_SERVICE_DESIRE_DATA_KEYS = ["performance_csv", "user_id", "project_id", "file_id", "round_year_detail", "Class"]
RESTRICTED_WORDS = ['NA', 'na', 'N/A', 'n/a']

# database credentials

DATABASE_USERNAME = "staging3m"
DATABASE_PASSWORD = "Jf93M0Za1SJ!mXkK"
DATABASE_IP = "103.243.56.137"
DATABASE_NAME = "cssl_report_test_prod"

query="'UPS182L3P23E1','UPS182L3P20E1','UPS182L3P06E1','UPS182M3P23E1','UPS182M3P20E1','UPS182M3P06E1'"

stmt="select Ques_no from answer_key where project_id =155 "

try:
    strttt=time.time()
    engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/{3}".format(DATABASE_USERNAME, DATABASE_PASSWORD,DATABASE_IP, DATABASE_NAME),pool_recycle=300, pool_size=5).connect()
    res = engine.execute(stmt)
    
    engine.close()
    df = pd.DataFrame(res.fetchall())

    df.columns = res.keys()
    print(df)
    print('df_len: ',len(df))
    print('df_columns: ',df.columns)
    print('sqltodf',time.time()-strttt,res)
    #df.to_csv('d_status.csv', index=False, columns=df.columns)
    """perofrmance_paper_code = df['Paper_Code'][0].split(",")
    query = str()
    for i in perofrmance_paper_code :
        query += '"' + i.strip() + '"'
        if(i != (perofrmance_paper_code[-1])):
            query += ','

    print(query)
    """

except ProgrammingError:
    raise
except InterfaceError:
    raise
except DatabaseError:
    raise
except:
    raise

"""

import cx_Oracle

"""
