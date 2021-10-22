import os
import json
import datetime
import logging

import azure.functions as func

from .webscrape import WebScrapingRequest, WebScrapingDBSchema, WebScrapingProcess_LM01
import pyodbc

def main(mytimer: func.TimerRequest) -> None:
    
    # Load task list
    with open('job_to_be_done.json') as f:
        tasks = json.load(f)

    # Database conn
    conn_string = os.environ["ConnectionString"]
    print(conn_string)

    # Loop over tasks' list
    for k,v in tasks.items():

        # Handle request phase
        wsr = WebScrapingRequest(url=v.get('url'), headers=v.get('headers'))

        # make request
        wsr.get_request()
        r = wsr.get_output()

        # Handle transformation process phase
        wsp = WebScrapingProcess_LM01(input=r.get('products'), translation=v.get('translate'))

        # Run transformation
        wsp.runprocess()
        out_t = wsp.output()

        # Handle insert database schema transformation phase
        wsdb = WebScrapingDBSchema(input=out_t, schema=v.get('schema'))
        
        # Run transformation
        wsdb.process_schema()
        out_db = wsdb.get_output()

        # sql statement
        sql_stat = f"INSERT INTO {v.get('target_table')} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

        # Load to Database
        with pyodbc.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                cursor.fast_executemany = True
                cursor.executemany(sql_stat, wsdb.get_output())