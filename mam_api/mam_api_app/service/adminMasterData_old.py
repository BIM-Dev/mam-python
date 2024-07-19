import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
import psycopg2.extras


class Mam_e_master_data_generator:
    def __init__(self, file_dir, master_data_sheet_name, line_colors_sheet_name, env_info_code, user, connection_parameters):
        self.file_dir = file_dir
        self.master_data_dir = master_data_sheet_name
        self.line_colors_dir = line_colors_sheet_name
        self.env_info_code = env_info_code
        self.user = user
        self.df_master_data = pd.read_excel(self.file_dir, sheet_name=self.master_data_dir, dtype=str)
        # 20240618 暂时过滤非Admin用户不可选的点位
        self.connection_parameters = connection_parameters
        if self.connection_parameters['host'] !='10.4.46.229':
            self.df_master_data = self.df_master_data[self.df_master_data['非Admin用户可选']=='是']
        self.df_master_data['站点编号'] = self.df_master_data['线路中文名称'] + self.df_master_data['站点中文名称']
        self.df_line_colors = pd.read_excel(self.file_dir, sheet_name=self.line_colors_dir, dtype=str)
        self.current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def generate_line(self, df_master_data, df_line_colors,user):
        df_result = df_master_data[['线路中文名称', '线路英文名称']].copy().drop_duplicates(inplace=False).rename(
            columns={'线路中文名称': 'name_cn', '线路英文名称': 'name_en'})
        df_result['code'] = df_result['name_cn']
        df_result['env_info_code'] = self.env_info_code
        df_result = pd.merge(df_result, df_line_colors, left_on='name_cn', right_on='线路中文名', how='left')
        df_result.rename(columns={'背景色': 'bg_color'}, inplace=True)
        df_result.rename(columns={'文字颜色': 'font_color'}, inplace=True)
        df_result = df_result[['code', 'name_cn', 'name_en', 'env_info_code', 'bg_color', 'font_color']]
        df_result['level_code'] = None
        df_result['creator'] = user
        df_result['modifier'] = user
        return df_result

    def generate_station(self, df_master_data):
        df_result = df_master_data[
            ['站点编号', '站点中文名称', '站点英文名称', '线路中文名称', '中文站点等级']].copy().drop_duplicates(
            inplace=False).rename(
            columns={'站点编号': 'code', '站点中文名称': 'name_cn', '站点英文名称': 'name_en',
                     '线路中文名称': 'line_code',
                     '中文站点等级': 'level_code'})
        df_result['env_info_code'] = self.env_info_code
        df_result['creator'] = self.user
        df_result['modifier'] = self.user
        return df_result

    def generate_asset(self, df_master_data):
        df_result = df_master_data[
            ['线路中文名称', '站点编号', '点位编码', '点位中文名称', '点位英文名称',
             '中文站点等级', '中文点位类别', '是否电子媒体', 'CPM价格']].copy().drop_duplicates(
            inplace=False).rename(
            columns={'点位编码': 'code', '点位中文名称': 'name_cn', '点位英文名称': 'name_en',
                     '线路中文名称': 'line_code', '站点编号': 'station_code', '中文站点等级': 'level_code',
                     '中文点位类别': 'category_code', 'CPM价格': 'cpm_price'})
        df_result['cpm_price'] = df_result['cpm_price'].replace({np.nan: None})
        # df_result = df_master_data[
        #     ['线路中文名称', '站点编号', '点位编码', '点位中文名称', '点位英文名称',
        #      '中文站点等级', '中文点位类别', '是否电子媒体']].copy().drop_duplicates(
        #     inplace=False).rename(
        #     columns={'点位编码': 'code', '点位中文名称': 'name_cn', '点位英文名称': 'name_en',
        #              '线路中文名称': 'line_code', '站点编号': 'station_code', '中文站点等级': 'level_code',
        #              '中文点位类别': 'category_code'})
        df_result['is_digital'] = df_result['是否电子媒体'].apply(lambda x: True if x == '是' else False)
        df_result.drop('是否电子媒体', axis=1, inplace=True)
        df_result['env_info_code'] = self.env_info_code
        df_result['is_active'] = True
        df_result['creator'] = self.user
        df_result['modifier'] = self.user
        return df_result

    def generate_level(self, df_master_data):
        df_result = df_master_data[
            ['站点等级序号', '中文站点等级', '英文站点等级']].copy().drop_duplicates(inplace=False).rename(
            columns={'中文站点等级': 'name_cn', '英文站点等级': 'name_en', '站点等级序号': 'rank_in_object'})
        df_result['code'] = df_result['name_cn']
        df_result['object'] = 'station'
        df_result['env_info_code'] = self.env_info_code
        df_result['creator'] = self.user
        df_result['modifier'] = self.user
        return df_result

    def generate_asset_category(self, df_master_data):
        df_result = df_master_data[
            ['中文点位类别', '英文点位类别']].copy().drop_duplicates(inplace=False).rename(
            columns={'中文点位类别': 'name_cn', '英文点位类别': 'name_en'})
        df_result['code'] = df_result['name_cn']
        df_result['env_info_code'] = self.env_info_code
        df_result['creator'] = self.user
        df_result['modifier'] = self.user
        return df_result

    def upload_data_to_db(self, connection_parameters, schema, delete_existed_rows, truncate_existed_table):
        df_line = self.generate_line(self.df_master_data, self.df_line_colors,self.user)
        df_station = self.generate_station(self.df_master_data)
        df_asset = self.generate_asset(self.df_master_data)
        df_level = self.generate_level(self.df_master_data)
        df_asset_category = self.generate_asset_category(self.df_master_data)
        mapping_name_df = {'line': df_line, 'station': df_station, 'asset': df_asset, 'level': df_level,
                           'asset_category': df_asset_category}
        conn = psycopg2.connect(**connection_parameters)
        cur = conn.cursor()
        for table_name in mapping_name_df:
            df = mapping_name_df[table_name]
            # print(table_name)
            placeholder_marks = ['%s' for x in df.columns]
            placeholder_marks = ','.join(placeholder_marks)
            # print(question_marks_str)
            columns = [x for x in df.columns]
            columns_str = ','.join(columns)
            data_tuples = [tuple(x) for x in df.values]
            # print(data_tuples)
            sql = f'insert into {schema}.{table_name}({columns_str},created_time,modified_time) values %s'

            if delete_existed_rows == True:
                delete_sql = f"delete from {schema}.{table_name} where env_info_code = '{self.env_info_code}'"
                # print(delete_sql)
                cur.execute(delete_sql)
            if truncate_existed_table == True:
                truncate_sql = f"truncate table {schema}.{table_name}"
                # print(truncate_sql)
                cur.execute(truncate_sql)
            # print(sql)
            # cur.execute(sql, data_tuples)
            psycopg2.extras.execute_values(cur, sql, data_tuples,
                                           template=f'({placeholder_marks},CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)')

            if table_name == 'asset':
                # print('validating assets...')
                update_asset_is_active_sql = f'''
                UPDATE {schema}.{table_name} a set is_active = FALSE
                WHERE a.env_info_code = '{self.env_info_code}' and 
                    a.code not in (
                    select distinct asset_code from {schema}.actual_impression	b
                    where b.env_info_code = '{self.env_info_code}'
                )
                '''

                cur.execute(update_asset_is_active_sql)
            conn.commit()
        self.insert_log(cur, schema)
        conn.close()


    def insert_log(self,cur,schema):
        # sql = f'insert into {schema}.admin_page_logs(env_info_code,operator,module,action,created_time,) values ({self.env_info_code},)'
        sql = f'''
            insert into {schema}.admin_page_logs(env_info_code,operator,module,action,created_time)
             values ('{self.env_info_code}','{self.user}','mam_e_master_data','upload_master_data',CURRENT_TIMESTAMP)
        '''
        cur.execute(sql)

    def create_tables_in_db(self, drop_existed_table):
        sql_env_info = f'''
            CREATE TABLE IF NOT EXISTS {schema}.env_info
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            code character varying(50) COLLATE pg_catalog."default",
            name_cn character varying(100) COLLATE pg_catalog."default",
            name_en character varying(100) COLLATE pg_catalog."default",
            creator character varying(100) COLLATE pg_catalog."default",
            modifier character varying(100) COLLATE pg_catalog."default",
            created_time timestamp without time zone,
            modified_time timestamp without time zone,
            CONSTRAINT env_info_pkey PRIMARY KEY (id)
        );
        '''

        sql_line = f'''
            CREATE TABLE IF NOT EXISTS {schema}.line
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            code character varying(50) COLLATE pg_catalog."default",
            name_cn character varying(100) COLLATE pg_catalog."default",
            name_en character varying(100) COLLATE pg_catalog."default",
            env_info_code character varying(50) COLLATE pg_catalog."default",
            bg_color character varying(20) COLLATE pg_catalog."default",
            font_color character varying(20) COLLATE pg_catalog."default",
            level_code character(50) COLLATE pg_catalog."default",
            creator character varying(100) COLLATE pg_catalog."default",
            modifier character varying(100) COLLATE pg_catalog."default",
            created_time timestamp without time zone,
            modified_time timestamp without time zone,
            CONSTRAINT line_pkey PRIMARY KEY (id)
        );
            '''

        sql_station = f'''
            CREATE TABLE IF NOT EXISTS {schema}.station
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            code character varying(50) COLLATE pg_catalog."default",
            name_cn character varying(100) COLLATE pg_catalog."default",
            name_en character varying(100) COLLATE pg_catalog."default",
            env_info_code character varying(50) COLLATE pg_catalog."default",
            line_code character varying(50) COLLATE pg_catalog."default",
            level_code character varying(50) COLLATE pg_catalog."default",
            creator character varying(100) COLLATE pg_catalog."default",
            modifier character varying(100) COLLATE pg_catalog."default",
            created_time timestamp without time zone,
            modified_time timestamp without time zone,
            CONSTRAINT station_pkey PRIMARY KEY (id)

        );
        '''

        sql_asset = f'''
            CREATE TABLE IF NOT EXISTS {schema}.asset
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            code character varying(50) COLLATE pg_catalog."default",
            name_cn character varying(100) COLLATE pg_catalog."default",
            name_en character varying(100) COLLATE pg_catalog."default",
            env_info_code character varying(50) COLLATE pg_catalog."default",
            line_code character varying(50) COLLATE pg_catalog."default",
            station_code character varying(50) COLLATE pg_catalog."default",
            level_code character varying(50) COLLATE pg_catalog."default",
            category_code character varying(50) COLLATE pg_catalog."default",
            is_digital bool,
            creator character varying(100) COLLATE pg_catalog."default",
            modifier character varying(100) COLLATE pg_catalog."default",
            created_time timestamp without time zone,
            modified_time timestamp without time zone,
            CONSTRAINT asset_pkey PRIMARY KEY (id)

        );
            '''

        sql_level = f'''
            CREATE TABLE IF NOT EXISTS {schema}.level
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            code character varying(50) COLLATE pg_catalog."default",
            name_cn character varying(100) COLLATE pg_catalog."default",
            name_en character varying(100) COLLATE pg_catalog."default",
            object character varying(10) COLLATE pg_catalog."default",
            env_info_code character varying(50) COLLATE pg_catalog."default",
            rank_in_object integer,
            creator character varying(100) COLLATE pg_catalog."default",
            modifier character varying(100) COLLATE pg_catalog."default",
            created_time timestamp without time zone,
            modified_time timestamp without time zone,
            CONSTRAINT level_pkey PRIMARY KEY (id)

        );
        '''

        sql_asset_category = f'''
            CREATE TABLE IF NOT EXISTS {schema}.asset_category
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            code character varying(50) COLLATE pg_catalog."default",
            name_cn character varying(100) COLLATE pg_catalog."default",
            name_en character varying(100) COLLATE pg_catalog."default",
            env_info_code character varying(50) COLLATE pg_catalog."default",
            creator character varying(100) COLLATE pg_catalog."default",
            modifier character varying(100) COLLATE pg_catalog."default",
            created_time timestamp without time zone,
            modified_time timestamp without time zone,
            CONSTRAINT asset_category_pkey PRIMARY KEY (id)

        );
            '''

        sql_estimated_impression = f'''
        CREATE TABLE IF NOT EXISTS {schema}.estimated_impression
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            env_info_code character varying(50) COLLATE pg_catalog."default",
            month integer,
            asset_code character varying(100) COLLATE pg_catalog."default",
            traffic numeric(20,4),
            exp_otc_flow numeric(10,4),
            exp_otc_dwell numeric(10,4),
            creator character varying(100) COLLATE pg_catalog."default",
            modifier character varying(100) COLLATE pg_catalog."default",
            created_time timestamp without time zone,
            modified_time timestamp without time zone,
            CONSTRAINT estimated_impression_pkey PRIMARY KEY (id)

        ); 
        '''

        sql_impression_report = f'''
            CREATE TABLE IF NOT EXISTS {schema}.impression_report
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            no character varying(100) COLLATE pg_catalog."default",
            env_info_code character varying(50) COLLATE pg_catalog."default",
            status_cn character varying(20) COLLATE pg_catalog."default",
            status_en character varying(20) COLLATE pg_catalog."default",
            total_impression numeric(20,4),
            campaign_name_cn character varying(100) COLLATE pg_catalog."default",
            campaign_name_en character varying(100) COLLATE pg_catalog."default",
            campaign_start_date date,
            campaign_end_date date,
            client_name_cn character varying(100) COLLATE pg_catalog."default",
            client_name_en character varying(100) COLLATE pg_catalog."default",
            client_logo_cn text,
            client_logo_en text,
            total_cost numeric(20,4),
            cpm numeric(20,4),
            env_company_id integer,
            owner character varying(100) COLLATE pg_catalog."default",
            is_active bool,
            creator character varying(100) COLLATE pg_catalog."default",
            modifier character varying(100) COLLATE pg_catalog."default",
            created_time timestamp without time zone,
            modified_time timestamp without time zone,
            CONSTRAINT impression_report_pkey PRIMARY KEY (id)
        );
        '''

        sql_impression_report_item = f'''
            CREATE TABLE IF NOT EXISTS {schema}.impression_report_item
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            impression_report_id integer,
            env_info_code character varying(50) COLLATE pg_catalog."default",
            date date,
            sot numeric(8,4),
            line_code character varying(50) COLLATE pg_catalog."default",
            line_name_cn character varying(100) COLLATE pg_catalog."default",
            line_name_en character varying(100) COLLATE pg_catalog."default",
            station_code character varying(50) COLLATE pg_catalog."default",
            station_name_cn character varying(100) COLLATE pg_catalog."default",
            station_name_en character varying(100) COLLATE pg_catalog."default",
            asset_code character varying(50) COLLATE pg_catalog."default",
            asset_name_cn character varying(100) COLLATE pg_catalog."default",
            asset_name_en character varying(100) COLLATE pg_catalog."default",
            impression numeric(20,4),
            creator character varying(100) COLLATE pg_catalog."default",
            modifier character varying(100) COLLATE pg_catalog."default",
            created_time timestamp without time zone,
            modified_time timestamp without time zone,
            CONSTRAINT impression_report_item_pkey PRIMARY KEY (id)
        );
        '''

        sql_env_company = f'''
            CREATE TABLE IF NOT EXISTS {schema}.env_company
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            env_info_code character varying(50) COLLATE pg_catalog."default",
            name_cn character varying(100) COLLATE pg_catalog."default",
            name_en character varying(100) COLLATE pg_catalog."default",
            logo text,
            full_name_cn character varying(100) COLLATE pg_catalog."default",
            full_name_en character varying(100) COLLATE pg_catalog."default",
            is_active bool,
            creator character varying(100) COLLATE pg_catalog."default",
            modifier character varying(100) COLLATE pg_catalog."default",
            created_time timestamp without time zone,
            modified_time timestamp without time zone,
            CONSTRAINT env_company_pkey PRIMARY KEY (id)
        );

        '''

        sql_impression_report_access = f'''
            CREATE TABLE IF NOT EXISTS {schema}.impression_report_access
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            env_info_code character varying(50) COLLATE pg_catalog."default",
            impression_report_id integer,
            impression_report_no character varying(100) COLLATE pg_catalog."default",
            passcode character varying(20) COLLATE pg_catalog."default",
            expiration_date date,
            is_active bool,
            creator character varying(100) COLLATE pg_catalog."default",
            modifier character varying(100) COLLATE pg_catalog."default",
            created_time timestamp without time zone,
            modified_time timestamp without time zone,
            CONSTRAINT impression_report_access_pkey PRIMARY KEY (id)
        );

        '''

        sql_error_code = f'''
        CREATE TABLE IF NOT EXISTS {schema}.error_code
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            code character varying(50) COLLATE pg_catalog."default",
            operation character varying(50) COLLATE pg_catalog."default",
            range character varying(50) COLLATE pg_catalog."default",
            type character varying(50) COLLATE pg_catalog."default",
            description character varying(1024) COLLATE pg_catalog."default",
            prompt_cn character varying(1024) COLLATE pg_catalog."default",
            prompt_en character varying(1024) COLLATE pg_catalog."default",
            CONSTRAINT error_code_pkey PRIMARY KEY (id)
        );
        '''

        sql_actual_impression = f'''
            CREATE TABLE IF NOT EXISTS {schema}.actual_impression
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            env_info_code character varying(50) COLLATE pg_catalog."default",
            date date,
            asset_code character varying(100) COLLATE pg_catalog."default",
            traffic numeric(20,4),
            exp_otc_flow numeric(10,4),
            exp_otc_dwell numeric(10,4),
            creator character varying(100) COLLATE pg_catalog."default",
            modifier character varying(100) COLLATE pg_catalog."default",
            created_time timestamp without time zone,
            modified_time timestamp without time zone,
            CONSTRAINT actual_impression_pkey PRIMARY KEY (id)
        ); 

        '''

        sql_impression_report_record = f'''
            CREATE TABLE IF NOT EXISTS {schema}.impression_report_record
        (
            id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
            env_info_code character varying(50) COLLATE pg_catalog."default",
            report_id integer,
            operation character varying(20) COLLATE pg_catalog."default",
            creator character varying(100) COLLATE pg_catalog."default",
            user character varying(100) COLLATE pg_catalog."default",   
            time timestamp without time zone,
            CONSTRAINT impression_report_record_pkey PRIMARY KEY (id)
        ); 

        '''

        conn = psycopg2.connect(**connection_parameters)
        cur = conn.cursor()
        sql_dict = {
            'env_info': sql_env_info,
            'line': sql_line,
            'station': sql_station,
            'asset': sql_asset,
            'level': sql_level,
            'asset_category': sql_asset_category,
            'estimated_impression': sql_estimated_impression,
            'impression_report': sql_impression_report,
            'impression_report_item': sql_impression_report_item,
            'env_company': sql_env_company,
            'impression_report_access': sql_impression_report_access,
            'error_code': sql_error_code,
            'actual_impression': sql_actual_impression,
            'impression_report_record': sql_impression_report_record
        }

        for table_name in sql_dict:
            sql = sql_dict[table_name]
            if drop_existed_table == True:
                cur.execute(f'drop table if exists {schema}.{table_name}')
            cur.execute(sql)
            conn.commit()
        conn.close()

    def export_excel_file(self, export_file_dir):
        with pd.ExcelWriter(export_file_dir) as writer:
            self.generate_line(self.df_master_data, self.df_line_colors).to_excel(writer, index=False,
                                                                                  sheet_name='line')
            self.generate_station(self.df_master_data).to_excel(writer, index=False, sheet_name='station')
            self.generate_asset(self.df_master_data).to_excel(writer, index=False, sheet_name='asset')
            self.generate_level(self.df_master_data).to_excel(writer, index=False, sheet_name='level')
            self.generate_asset_category(self.df_master_data).to_excel(writer, index=False, sheet_name='asset_category')

    def uploadFile(self,file):
        response = Response(result)
        return response



# if __name__ == '__main__':
#     # env_info_code = 'mam-test'
#     # user = 'zewen.liang@jcdecaux.com'
#     # # connection_parameters = {
#     # #     "host": "10.4.47.220",
#     # #     "port": "5432",
#     # #     "database": "DataAnalysis",
#     # #     "user": "postgres",
#     # #     "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
#     # # }
#     # connection_parameters = {
#     #     "host": "10.4.47.180",
#     #     "port": "5432",
#     #     "database": "generic_am_mam",
#     #     "user": "postgres",
#     #     "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
#     # }
#     # schema = 'public'
#     # table_name = 'actual_impression'
#     # conn = psycopg2.connect(**connection_parameters)
#     # cur = conn.cursor()
#     # actual_impression = pd.read_excel(r'C:\Users\zewen.liang\OneDrive - JCDECAUX\桌面\NEW_exported.xlsx', sheet_name='表3')
#     # print(actual_impression.head())
#     # actual_impression['creator'] = user
#     # actual_impression['modifier'] = user
#     # df = actual_impression
#     # # print(table_name)
#     # placeholder_marks = ['%s' for x in df.columns]
#     # placeholder_marks = ','.join(placeholder_marks)
#     # # print(question_marks_str)
#     # columns = [x for x in df.columns]
#     # columns_str = ','.join(columns)
#     # data_tuples = [tuple(x) for x in df.values]
#     # # print(data_tuples)
#     # sql = f'insert into {schema}.{table_name}({columns_str},created_time,modified_time) values %s'
#     #
#     # print(sql)
#     # # cur.execute(sql, data_tuples)
#     # psycopg2.extras.execute_values(cur, sql, data_tuples,
#     #                                template=f'({placeholder_marks},CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)')
#     # conn.commit()
#     # conn.close()
#     # exit()
#     # connection_parameters = {
#     #     "host": "10.4.47.220",
#     #     "port": "5432",
#     #     "database": "DataAnalysis",
#     #     "user": "postgres",
#     #     "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
#     # }
#     # Dev
#     # connection_parameters = {
#     #     "host": "10.4.46.229",
#     #     "port": "5432",
#     #     "database": "generic_am_mam",
#     #     "user": "postgres",
#     #     "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
#     # }
#     # Staging
#     connection_parameters = {
#          "host": "localhost",
#             "port": "5432",
#             "database": "generic_am_mam",
#             "user": "postgres",
#             "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
#     }
#     schema = 'public'
#     file_dir = r"C:\Users\zewen.liang\Downloads\MAM E版本升级需要的数据-新版20240617 1.xlsx"
#     master_data_sheet_name = '主数据模板'
#     line_colors_sheet_name = '线路颜色标识模板'
#     env_info_code = 'mam-shanghai'
#     user = 'zewen.liang@jcdecaux.com'
#     generator = Mam_e_master_data_generator(file_dir, master_data_sheet_name, line_colors_sheet_name, env_info_code,
#                                             user)
#     # export_file_dir = r'C:\Users\xiaoyu.yan\OneDrive - JCDECAUX\Desktop\Projects\MAM\MAM Enhancement\主数据\NEW_exported.xlsx'
#     # generator.export_excel_file(export_file_dir)
#     generator.upload_data_to_db(connection_parameters=connection_parameters, schema=schema, delete_existed_rows=True,
#                                 truncate_existed_table=False)

