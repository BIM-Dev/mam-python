
import pandas as pd
import psycopg2
class checkAdminMasterData:
    def file_check(file_dir, env_info_code):
        """
        :param file_dir:
        :return:
        """
        message = []
        # 检查是否两个模板都存在
        try:
            df_master_data = pd.read_excel(file_dir, sheet_name="主数据模板")
            df_master_data['站点编号'] = df_master_data['线路中文名称'] + df_master_data['站点中文名称']
        except:
            message.append({"step": "检查是否两个模板都存在",
                            "reason": "缺失数据表：主数据模板"})
            return message

        try:
            df_line_color = pd.read_excel(file_dir, sheet_name="线路颜色标识模板")
        except:
            message.append({"step": "检查是否两个模板都存在",
                            "reason": "缺失数据表：线路颜色标识模板"})
            return message

        # 检查“主数据模板”中线路数和“线路颜色标识模板”中线路数是否一致
        line_number_in_master_data = df_master_data['线路中文名称'].unique().size
        line_number_in_line_color = df_line_color['线路中文名'].unique().size
        if line_number_in_master_data != line_number_in_line_color:
            message.append({"step": "检查“主数据模板”中线路数和“线路颜色标识模板”中线路数是否一致",
                            "reason": "主数据模板与线路颜色标识模板线路数不一致：" + str(line_number_in_master_data) +"/"+ str(line_number_in_line_color)})

        # 检查主数据模板表
        # 检查线路中文名与英文名是否一一对应
        line_cn_en_number_in_master_data = df_master_data[['线路中文名称', '线路英文名称']].drop_duplicates().shape[0]
        # if line_number_in_master_data != line_cn_en_number_in_master_data:
        #     message.append({"step": "检查线路中文名与英文名是否一一对应",
        #                     "reason": "线路中文名与线路英文名不是一一对应"})
        if line_number_in_master_data != line_cn_en_number_in_master_data:
            df_temp = df_master_data[['线路中文名称', '线路英文名称']].drop_duplicates().groupby(['线路中文名称'])[
                '线路英文名称'].count().reset_index(name='线路英文名称数量')
            df_temp = df_temp[df_temp['线路英文名称数量'] > 1]
            if df_temp.shape[0] != 0:
                list_temp = list(df_temp['线路中文名称'].astype(str))
                message.append({"step": "检查线路中文名与英文名是否一一对应",
                                "reason": "以下线路中文名拥有多个线路英文名：" + ', '.join(list_temp)})
            else:
                df_temp = df_master_data[['线路中文名称', '线路英文名称']].drop_duplicates().groupby(['线路英文名称'])[
                    '线路中文名称'].count().reset_index(name='线路中文名称数量')
                df_temp = df_temp[df_temp['线路中文名称数量'] > 1]
                list_temp = list(df_temp['线路英文名称'].astype(str))
                message.append({"step": "检查线路中文名与英文名是否一一对应",
                                "reason": "以下线路英文名拥有多个线路中文名：" + ', '.join(list_temp)})
        # 检查站点编号与站点中文名称是否一一对应
        station_id_number = df_master_data['站点编号'].unique().size
        station_id_cn_number = df_master_data[['站点编号', '站点中文名称']].drop_duplicates().shape[0]
        if station_id_number != station_id_cn_number:
            message.append({"step": "检查站点编号与站点中文名称是否一一对应",
                            "reason": "站点编号与站点中文名称不是一一对应"})
        # 检查站点中文名称与站点英文名称是否一一对应
        station_cn_number = df_master_data['站点中文名称'].unique().size
        station_cn_en_number = df_master_data[['站点中文名称', '站点英文名称']].drop_duplicates().shape[0]
        # if station_cn_number != station_cn_en_number:
        #     message.append({"step": "检查站点中文名称与站点英文名称是否一一对应",
        #                     "reason": "站点中文名称与站点英文名称不是一一对应"})
        if station_cn_number != station_cn_en_number:
            df_temp = df_master_data[['站点中文名称', '站点英文名称']].drop_duplicates().groupby(['站点中文名称'])[
                '站点英文名称'].count().reset_index(name='站点英文名称数量')
            df_temp = df_temp[df_temp['站点英文名称数量'] > 1]
            if df_temp.shape[0] != 0:
                list_temp = list(df_temp['站点中文名称'].astype(str))
                message.append({"step": "检查站点中文名称与站点英文名称是否一一对应",
                                "reason": "以下站点中文名称拥有多个站点英文名称：" + ', '.join(list_temp)})
            else:
                df_temp = df_master_data[['站点中文名称', '站点英文名称']].drop_duplicates().groupby(['站点英文名称'])[
                    '站点中文名称'].count().reset_index(name='站点中文名称数量')
                df_temp = df_temp[df_temp['站点中文名称数量'] > 1]
                list_temp = list(df_temp['站点英文名称'].astype(str))
                message.append({"step": "检查站点中文名称与站点英文名称是否一一对应",
                                "reason": "以下站点英文名称拥有多个站点中文名称：" + ', '.join(list_temp)})
        # 检查站点中文名称是否拥有唯一站点等级
        station_namecn_level_number = df_master_data[['站点中文名称', '站点等级序号']].drop_duplicates().shape[0]
        if station_cn_number != station_namecn_level_number:
            df_temp = df_master_data[['站点中文名称', '站点等级序号']].drop_duplicates().groupby(['站点中文名称'])[
                '站点等级序号'].count().reset_index(name='站点等级序号数量')
            df_temp = df_temp[df_temp['站点等级序号数量'] > 1]
            print(station_cn_number, station_namecn_level_number)
            list_temp = list(df_temp['站点中文名称'].astype(str))
            message.append({"step": "检查站点中文名称是否拥有唯一站点等级",
                            "reason": "以下站点中文名称拥有多个站点等级：" + ', '.join(list_temp)})
        # 检查点位编码是否有重复
        df_temp = df_master_data[['点位编码']].groupby(['点位编码'])['点位编码'].count().reset_index(name='点位数量')
        df_temp = df_temp[df_temp['点位数量'] > 1]
        if df_temp.shape[0] != 0:
            list_temp = list(df_temp['点位编码'])
            message.append({"step": "检查点位编码是否有重复",
                            "reason": '以下点位出现多次：' + ', '.join(list_temp)})

        # 检查站点等级序号与中文站点等级是否一一对应
        level_id_number = df_master_data['站点等级序号'].unique().size
        level_id_cn_number = df_master_data[['站点等级序号', '中文站点等级']].drop_duplicates().shape[0]
        if level_id_number != level_id_cn_number:
            df_temp = df_master_data[['站点等级序号', '中文站点等级']].drop_duplicates().groupby(['站点等级序号'])[
                '中文站点等级'].count().reset_index(name='中文站点等级数量')
            df_temp = df_temp[df_temp['中文站点等级数量'] > 1]
            if df_temp.shape[0] != 0:
                list_temp = list(df_temp['站点等级序号'].astype(str))
                message.append({"step": "检查站点等级序号与中文站点等级是否一一对应",
                                "reason": "以下站点等级序号拥有多个中文站点等级：" + ', '.join(list_temp)})
            else:
                df_temp = df_master_data[['站点等级序号', '中文站点等级']].drop_duplicates().groupby(['中文站点等级'])[
                    '站点等级序号'].count().reset_index(name='站点等级序号数量')
                df_temp = df_temp[df_temp['站点等级序号数量'] > 1]
                list_temp = list(df_temp['中文站点等级'].astype(str))
                message.append({"step": "检查站点等级序号与中文站点等级是否一一对应",
                                "reason": "以下中文站点等级拥有多个站点等级序号：" + ', '.join(list_temp)})

        # 检查中文站点等级与英文站点等级是否一一对应
        level_cn_number = df_master_data['中文站点等级'].unique().size
        level_cn_en_number = df_master_data[['中文站点等级', '英文站点等级']].drop_duplicates().shape[0]
        if level_cn_number != level_cn_en_number:
            df_temp = df_master_data[['中文站点等级', '英文站点等级']].drop_duplicates().groupby(['中文站点等级'])[
                '英文站点等级'].count().reset_index(name='英文站点等级数量')
            df_temp = df_temp[df_temp['英文站点等级数量'] > 1]
            if df_temp.shape[0] != 0:
                list_temp = list(df_temp['中文站点等级'].astype(str))
                message.append({"step": "检查中文站点等级与英文站点等级是否一一对应",
                                "reason": "以下中文站点等级拥有多个英文站点等级：" + ', '.join(list_temp)})
            else:
                df_temp = df_master_data[['中文站点等级', '英文站点等级']].drop_duplicates().groupby(['英文站点等级'])[
                    '中文站点等级'].count().reset_index(name='中文站点等级数量')
                df_temp = df_temp[df_temp['中文站点等级数量'] > 1]
                list_temp = list(df_temp['英文站点等级'].astype(str))
                message.append({"step": "检查站点等级序号与中文站点等级是否一一对应",
                                "reason": "以下英文站点等级拥有多个中文站点等级：" + ', '.join(list_temp)})
        # 检查中文点位类别与英文点位类别是否一一对应
        category_cn_number = df_master_data['中文点位类别'].unique().size
        category_cn_en_number = df_master_data[['中文点位类别', '英文点位类别']].drop_duplicates().shape[0]
        if category_cn_number != category_cn_en_number:
            df_temp = df_master_data[['中文点位类别', '英文点位类别']].drop_duplicates().groupby(['中文点位类别'])[
                '英文点位类别'].count().reset_index(name='英文点位类别数量')
            df_temp = df_temp[df_temp['英文点位类别数量'] > 1]
            if df_temp.shape[0] != 0:
                list_temp = list(df_temp['中文点位类别'].astype(str))
                message.append({"step": "检查中文点位类别与英文点位类别是否一一对应",
                                "reason": "以下中文点位类别拥有多个英文点位类别：" + ', '.join(list_temp)})
            else:
                df_temp = df_master_data[['中文点位类别', '英文点位类别']].drop_duplicates().groupby(['英文点位类别'])[
                    '中文点位类别'].count().reset_index(name='中文点位类别数量')
                df_temp = df_temp[df_temp['中文点位类别数量'] > 1]
                list_temp = list(df_temp['英文点位类别'].astype(str))
                message.append({"step": "检查中文点位类别与英文点位类别是否一一对应",
                                "reason": "以下英文点位类别拥有多个中文点位类别：" + ', '.join(list_temp)})
        # 检查是否电子媒体是否仅包含是与否
        df_temp = df_master_data['是否电子媒体'].unique()
        set_temp = set(list(df_temp))

        if not set_temp.issubset({'是', '否'}):
            message.append({"step": "检查是否电子媒体是否仅包含是与否",
                            "reason": "是否电子媒体字段包含非法值"})

        # 检查站点中文名是否全部出现在现有的MAM L主数据中
        #local
        # connection_parameters = {
        #     "host": "localhost",
        #     "port": "5432",
        #     "database": "generic_am_mam",
        #     "user": "postgres",
        #     "password": "docker"
        # }
        # dev
        connection_parameters = {
            "host": "10.4.46.229",
            "port": "5432",
            "database": "generic_am_mam",
            "user": "postgres",
            "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
        }
        # staging
        # connection_parameters = {
        #     "host": "10.4.47.180",
        #     "port": "5432",
        #     "database": "generic_am_mam",
        #     "user": "postgres",
        #     "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
        # }
        conn = psycopg2.connect(**connection_parameters)
        cur = conn.cursor()
        sql = f"""
        SELECT distinct station_name_cn FROM public.station_traffic
        WHERE env_info_code = '{env_info_code}'
        """
        cur.execute(sql)
        db_stations = set([x[0] for x in cur.fetchall()])
        file_stations = set(df_master_data['站点中文名称'].tolist())
        extra_stations = file_stations - db_stations
        if len(extra_stations) != 0:
            message.append({"step": "检查上传文件中的站点中文名是否都已存在于数据库中",
                            "reason": "以下站点在现有数据库中不存在：" + ','.join(extra_stations)})



        return message


