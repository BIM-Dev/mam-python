import os
import smtplib
from email.message import EmailMessage
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from io import BytesIO
import traceback
import datetime
import psycopg2
from psycopg2.extras import DictCursor
connection_parameters = {
          # "host": "10.4.47.220",
          #   "port": "5432",
          #   "database": "DataAnalysis",
          #   "user": "postgres",
          #   "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
"user" : "postgres",
"password" : "docker",
"host": "10.4.46.229",
"port" : "5432",
"database": "generic_am_mam"
    }
schema = 'public'
class getUserService:
    @staticmethod
    def getUserList( env_info_code):
        try:
            conn = psycopg2.connect(**connection_parameters)
            cur = conn.cursor(cursor_factory=DictCursor)
            report_info_sql = f"""
                       SELECT first_name,last_name,email,title,location,business_unit
               	        FROM {schema}.env_info
               	    WHERE  code = '{env_info_code}'
                       """
            cur.execute(report_info_sql)
            report_info_data = cur.fetchall()
            print(report_info_data)
            # if report_info_data is not None:
            #     msg['Bcc'] = dict(report_info_data[0])['bcc_helper']