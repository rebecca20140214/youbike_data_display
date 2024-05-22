import requests
import psycopg2
import json
from urllib.parse import urlparse
from dotenv import load_dotenv
import os
import schedule
import time
import streamlit as st
#from streamlit_autorefresh import st_autorefresh
#
# 加载环境变量
load_dotenv() 

# 从环境变量中解析数据库连接信息
database_url = os.getenv('POSTGRE_PASSWORD1')
result = urlparse(database_url)
conn_info = {
    'dbname': result.path[1:],
    'user': result.username,
    'password': result.password,
    'host': result.hostname,
    'port': result.port
}

# 获取 JSON 数据
def get_json_data(url):
    response = requests.get(url)
    response.encoding = 'utf-8'  # 设置编码为 UTF-8
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to download JSON data. Status code: {response.status_code}")
        return None

# 生成表创建 SQL 语句
def generate_create_table_sql(data):
    sample_record = data[0]
    columns = []
    
    for key, value in sample_record.items():
        if isinstance(value, int):
            column_type = 'INTEGER'
        elif isinstance(value, float):
            column_type = 'FLOAT'
        else:
            column_type = 'character varying(100) '
        
        columns.append(f"{key} {column_type}")
    
    columns_sql = ", ".join(columns)
    create_table_sql = f"CREATE TABLE IF NOT EXISTS tmp_youbike_auto ({columns_sql});"
    return create_table_sql

# 清空表
def truncate_table(conn_info, table_name):
    try:
        with psycopg2.connect(**conn_info) as conn:
            with conn.cursor() as cursor:
                truncate_sql = f"TRUNCATE TABLE {table_name};"
                cursor.execute(truncate_sql)
                conn.commit()
                print(f"Table {table_name} truncated successfully")
    except Exception as e:
        print(f"Error: {e}")

# 创建表并插入数据
def create_table_and_insert_data(conn_info, create_table_sql, data):
    try:
        with psycopg2.connect(**conn_info) as conn:
            with conn.cursor() as cursor:
                # 清空表
                truncate_table(conn_info, "tmp_youbike_auto")
                
                # 创建表
                cursor.execute(create_table_sql)
                conn.commit()

                # 插入数据
                for record in data:
                    keys = record.keys()
                    values = tuple(record.values())
                    insert_sql = f"INSERT INTO tmp_youbike_auto ({', '.join(keys)}) VALUES ({', '.join(['%s'] * len(values))})"
                    cursor.execute(insert_sql, values)
                
                conn.commit()
                print("Data inserted successfully")

    except Exception as e:
        print(f"Error: {e}")

def merge_data(conn_info, source_table, target_table):
    try:
        with psycopg2.connect(**conn_info) as conn:
            with conn.cursor() as cursor:
                merge_sql = f"""
                            INSERT INTO youbike_auto (sno, sna, sarea, mday, ar, sareaen, snaen, aren, act, srcupdatetime, updatetime, infotime, infodate, total, available_rent_bikes, latitude, longitude, available_return_bikes)
                                            SELECT sno, sna, sarea, mday, ar, sareaen, snaen, aren, act, srcupdatetime, updatetime, infotime, infodate, total, available_rent_bikes, latitude, longitude, available_return_bikes
                                            FROM tmp_youbike_auto 
                                            ON CONFLICT (sno) DO UPDATE
                                            SET    sna                =EXCLUDED.sna
                                            , sarea                   =EXCLUDED.sarea
                                            , mday                    =EXCLUDED.mday
                                            , ar                      =EXCLUDED.ar
                                            , sareaen                 =EXCLUDED.sareaen
                                            , snaen                   =EXCLUDED.snaen
                                            , aren                    =EXCLUDED.aren
                                            , act                     =EXCLUDED.act
                                            , srcupdatetime           =EXCLUDED.srcupdatetime
                                            , updatetime              =EXCLUDED.updatetime
                                            , infotime                =EXCLUDED.infotime
                                            , infodate                =EXCLUDED.infodate
                                            , total                   =EXCLUDED.total
                                            , available_rent_bikes    =EXCLUDED.available_rent_bikes
                                            , latitude                =EXCLUDED.latitude
                                            , longitude               =EXCLUDED.longitude
                                            , available_return_bikes  =EXCLUDED.available_return_bikes	;
					
                """
                cursor.execute(merge_sql)
                conn.commit()
                #print("Merge completed successfully")
                #st.success("JSON file downloaded successfully!")
                st.fre

    except Exception as e:
        print(f"Error: {e}")        

def download_and_update():
    url = 'https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json'
    json_data = get_json_data(url)

    if json_data:
        create_table_sql = generate_create_table_sql(json_data)
        create_table_and_insert_data(conn_info, create_table_sql, json_data)

    source_table = "tmp_youbike_auto"
    target_table = "youbike_auto"
    merge_data(conn_info, source_table, target_table)
    st.cache_resource.clear()

# 每5分钟执行一次
#schedule.every(5).minutes.do(download_and_update)

#while True:
    #time.sleep(1)



@st.cache_resource
def get_sarea() -> tuple:
    conn = psycopg2.connect(os.environ['POSTGRE_PASSWORD1'])
    with conn:
        with conn.cursor() as cursor:
            # 取出最新日期各站點資料
            sql = '''
                SELECT 行政區
                FROM 站點資訊_view
                GROUP BY 行政區
                order by 行政區
            '''
            cursor.execute(sql)
            allArea: tuple = cursor.fetchall()
            return allArea
    
    conn.close()

@st.cache_resource
def info_sarea(name: str):
    conn = psycopg2.connect(os.environ['POSTGRE_PASSWORD1'])
    with conn:
        with conn.cursor() as cursor:
            # 取出各區最新資料
            sql = '''
               	 SELECT 日期, b.站點名稱, 行政區, 站點地址, 總車輛, 可借, 可還,b.站點編號
                FROM youbike_view a
                JOIN 站點資訊_view b ON a.編號 = b.站點編號 and 行政區 = %s
               order by a.編號; 
            '''
            cursor.execute(sql, (name,))
            st.success(name)
            return cursor.fetchall()
    conn.close()


#開頁面就先更新資料庫
download_and_update()

col1, col2 = st.columns([1, 2])
data = [tuple1[0] for tuple1 in get_sarea()]
#print(data)
st.radio('選擇行政區:',data,key='sarea',horizontal=True)
area = st.session_state.sarea
data = info_sarea(name=area)
data1 = [{'日期':item[0],'站點':item[1],'可借':item[5],'可還':item[6],'總車輛':item[4]} for item in data]
#print(data1)
st.dataframe(data1)

if st.button("重新更新JSON檔"):
   st.success("JSON 資料更新中.....")   
   download_and_update()
   st.success("JSON 資料更新完成.....")   




