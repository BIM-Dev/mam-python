import pandas as pd
import psycopg2



class checkAdminMasterData:
    def __init__(self, file, env_info_code):
        self.env_info_code = env_info_code
        self.file = file
        # 检查是否三个模板都存在

        # self.df_master_data = pd.read_excel(file, sheet_name="主数据模板")
        # self.df_line_color = pd.read_excel(file, sheet_name="线路颜色标识模板")
        # self.df_station_mapping = pd.read_excel(file, sheet_name="L&E站点匹配模板")
        # self.df_master_data = pd.merge(self.df_master_data, self.df_station_mapping,
        #                                    on=['线路中文名称', '站点中文名称'])
        # self.df_master_data['站点编号'] = self.df_master_data['L版station_id']
        # self.df_master_data = self.df_master_data.drop(columns=['L版station_id'])

    def _check_fields_existence(self, dataframe: pd.DataFrame, fields_list: list):
        df_columns = dataframe.columns
        status = True
        message = []
        if set(fields_list).issubset(set(df_columns)):
            status = True
            message = ''
        else:
            status = False
            missing_fields = set(fields_list) - set(df_columns)
            message.append({"step": "检查字段是否确实",
                            "reason":  "以下必需字段缺失，请检查文件是否包含这些字段：" + ", ".join(missing_fields) +  '' + '\n'})

        return status, message

    def check_3_templates_existence(self, file):
        status = True
        message = []
        try:
            df_master_data = pd.read_excel(file, sheet_name="主数据模板")
            # df_master_data['站点编号'] = df_master_data['线路中文名称'] + df_master_data['站点中文名称']
        except:
            status = False
            message.append({"step": "检查是否三个模板都存在",
                            "reason": "缺失数据表：主数据模板；"})
        try:
            df_line_color = pd.read_excel(file, sheet_name="线路颜色标识模板")
        except:
            status = False
            message.append({"step": "检查是否三个模板都存在",
                            "reason": "缺失数据表：线路颜色标识模板；"})
        try:
            df_station_mapping = pd.read_excel(file, sheet_name="L&E站点匹配模板")
        except:
            status = False
            message.append({"step": "检查是否三个模板都存在",
                            "reason": "缺失数据表：L&E站点匹配模板；"})
        if status == True:
            self.df_master_data = pd.read_excel(file, sheet_name="主数据模板")
            self.df_line_color = pd.read_excel(file, sheet_name="线路颜色标识模板")
            self.df_station_mapping = pd.read_excel(file, sheet_name="L&E站点匹配模板")
            # status, message = self._check_no_null_value_in_cloumns(self.df_master_data, list(set(self.df_master_data) - {'CPM价格'}))
            # if status != True:
            #     return status, message

            status, message = self._check_fields_existence(self.df_master_data,
                                                                 ['线路中文名称','线路英文名称','站点中文名称','站点英文名称','点位编码'
                                                                  ,'点位中文名称','点位英文名称','站点等级序号','英文站点等级','中文站点等级',
                                                                  '中文点位类别','英文点位类别','是否电子媒体','CPM价格','非Admin用户可选'])
            if status != True:
                return status, message

            self.df_master_data = pd.merge(self.df_master_data, self.df_station_mapping,
                                           on=['线路中文名称', '站点中文名称'])
            self.df_master_data['站点编号'] = self.df_master_data['L版station_id']
            self.df_master_data = self.df_master_data.drop(columns=['L版station_id'])
        return status, message

    # 检查主数据模板表除了CPM列 是否存在空值
    def _check_no_null_value_in_cloumns(self, dataframe: pd.DataFrame, fields_list: list):
        status = True
        message = []
        temp_df = dataframe[fields_list]
        columns_with_null = temp_df.columns[temp_df.isnull().any()].tolist()
        if len(columns_with_null) != 0:
            status = False
            message.append({"step": "检查是否有空值",
                            "reason": '以下字段有空值：' + ','.join(columns_with_null) + '，请根据实际情况填值；\n'})
        return status, message


    # 检查主数据模板表
    # 检查线路中文名与英文名是否一一对应
    def check_linecn_lineen_mapping(self, df_master_data: pd.DataFrame):
        status = True
        message = []
        line_cn_en_number_in_master_data = df_master_data[['线路中文名称', '线路英文名称']].drop_duplicates().shape[0]
        line_number_in_master_data = df_master_data['线路中文名称'].unique().size
        if line_number_in_master_data != line_cn_en_number_in_master_data:
            status = False
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
        return status, message

    # 检查站点编号与站点中文名称是否一一对应
    def check_stationcode_stationname_mapping(self, df_master_data: pd.DataFrame):
        status = True
        message = []
        station_id_number = df_master_data['站点编号'].unique().size
        station_id_cn_number = df_master_data[['站点编号', '站点中文名称']].drop_duplicates().shape[0]
        if station_id_number != station_id_cn_number:
            status = False
            message.append({"step": "检查站点编号与站点中文名称是否一一对应",
                            "reason": "站点编号与站点中文名称不是一一对应"})
        return status, message

    # 检查站点中文名称与站点英文名称是否一一对应
    def check_stationnamecn_stationnameen_mapping(self, df_master_data: pd.DataFrame):
        status = True
        message = []
        station_cn_number = df_master_data['站点中文名称'].unique().size
        station_cn_en_number = df_master_data[['站点中文名称', '站点英文名称']].drop_duplicates().shape[0]
        if station_cn_number != station_cn_en_number:
            status = False
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
        return status, message

    # 检查站点中文名称是否拥有唯一站点等级
    def check_stationnamecn_stationlevelname_mapping(self, df_master_data: pd.DataFrame):
        status = True
        message = []
        station_namecn_level_number = df_master_data[['站点中文名称', '站点等级序号']].drop_duplicates().shape[0]
        station_cn_number = df_master_data['站点中文名称'].unique().size
        if station_cn_number != station_namecn_level_number:
            status = False
            df_temp = df_master_data[['站点中文名称', '站点等级序号']].drop_duplicates().groupby(['站点中文名称'])[
                '站点等级序号'].count().reset_index(name='站点等级序号数量')
            df_temp = df_temp[df_temp['站点等级序号数量'] > 1]
            list_temp = list(df_temp['站点中文名称'].astype(str))
            message.append({"step": "检查站点中文名称是否拥有唯一站点等级",
                            "reason": "以下站点中文名称拥有多个站点等级：" + ', '.join(list_temp)})
        return status, message

    # 检查点位编码是否有重复
    def check_assetcode_uniqueness(self, df_master_data: pd.DataFrame):
        status = True
        message = []
        df_temp = df_master_data[['点位编码']].groupby(['点位编码'])['点位编码'].count().reset_index(name='点位数量')
        df_temp = df_temp[df_temp['点位数量'] > 1]
        if df_temp.shape[0] != 0:
            status = False
            list_temp = list(df_temp['点位编码'])
            message.append({"step": "检查点位编码是否有重复",
                            "reason": '以下点位出现多次：' + ', '.join(list_temp)})
        return status, message

    # 检查站点等级序号与中文站点等级是否一一对应
    def check_stationlevelcode_stationlevelnamecn_mapping(self, df_master_data: pd.DataFrame):
        status = True
        message = []
        level_id_number = df_master_data['站点等级序号'].unique().size
        level_id_cn_number = df_master_data[['站点等级序号', '中文站点等级']].drop_duplicates().shape[0]
        if level_id_number != level_id_cn_number:
            status = False
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
        return status, message

    # 检查中文站点等级与英文站点等级是否一一对应
    def check_stationlevelnamecn_stationlevelnameen_mapping(self, df_master_data: pd.DataFrame):
        status = True
        message = []
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
        return status, message

    # 检查中文点位类别与英文点位类别是否一一对应
    def check_assetcategorynamecn_assetcategorynameen_mapping(self, df_master_data: pd.DataFrame):
        status = True
        message = []
        category_cn_number = df_master_data['中文点位类别'].unique().size
        category_cn_en_number = df_master_data[['中文点位类别', '英文点位类别']].drop_duplicates().shape[0]
        if category_cn_number != category_cn_en_number:
            status = False
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
        return status, message

    # 检查是否电子媒体是否仅包含是与否
    def check_isdigital_yesnoonly(self, df_master_data: pd.DataFrame):
        status = True
        message = []
        df_temp = df_master_data['是否电子媒体'].unique()
        set_temp = set(list(df_temp))
        if not set_temp.issubset({'是', '否'}):
            status = False
            message.append({"step": "检查是否电子媒体是否仅包含是与否",
                            "reason": "是否电子媒体字段包含非法值"})
        return status, message

    # 检查L&E站点匹配模板
    # 检查站点编码是否全部出现在现有的MAM L主数据中
    def check_stationcode_existence(self, df_station_mapping: pd.DataFrame):
        status = True
        message = []
        # dev
        connection_parameters = {
            "host": "localhost",
            "port": "5432",
            "database": "generic_am_mam",
            "user": "postgres",
            "password": "docker"
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
        SELECT distinct station_code FROM public.station_traffic
        WHERE env_info_code = '{self.env_info_code}'
        """
        cur.execute(sql)
        db_stations = set([str(x[0]) for x in cur.fetchall()])
        file_stations = set(df_station_mapping['L版station_id'].astype(str).tolist())
        extra_stations = file_stations - db_stations
        if len(extra_stations) != 0:
            status = False
            message.append({"step": "检查上传文件中的站点编码是否都已存在于数据库中",
                            "reason": "以下站点编码在现有数据库中不存在：" + ','.join(extra_stations)})
        return status, message

    # 检查线路颜色标识模板
    # 检查颜色长度是否为7
    def check_color_length(self, df_line_color: pd.DataFrame):
        status = True
        message = []
        error_values_bgcolor = [x for x in df_line_color['背景色'].unique().tolist() if len(x) != 7]
        error_values_fontcolor = [x for x in df_line_color['文字颜色'].unique().tolist() if len(x) != 7]
        if len(error_values_bgcolor) != 0:
            status = False
            message.append({"step": "检查背景色代码长度是否为7",
                            "reason": "以下背景色代码长度不为7：" + ','.join(error_values_bgcolor)})
        if len(error_values_fontcolor) != 0:
            status = False
            message.append({"step": "检查文字颜色代码长度是否为7",
                            "reason": "以下文字颜色代码长度不为7：" + ','.join(error_values_fontcolor)})
        return status, message

    # 跨表检查
    # 检查“主数据模板“中的线路中文名和站点中文名组合是否与”L&E站点匹配模板“中的线路中文名和站点中文名组合一一对应
    def check_combination_linecn_stationcn(self, df_master_data: pd.DataFrame, df_station_mapping):
        status = True
        message = []
        df_master_data['linecn_stationcn'] = df_master_data['线路中文名称'] + df_master_data['站点中文名称']
        df_station_mapping['linecn_stationcn'] = df_station_mapping['线路中文名称'] + df_station_mapping['站点中文名称']
        master_data_combinations = set(df_master_data['linecn_stationcn'].unique().tolist())
        station_mapping_combinations = set(df_station_mapping['linecn_stationcn'].unique().tolist())
        if master_data_combinations != station_mapping_combinations:

            extra_combinations_master_data = master_data_combinations - station_mapping_combinations
            # extra_combinations_station_mapping = station_mapping_combinations - master_data_combinations
            if len(extra_combinations_master_data) != 0:
                status = False
                message.append({"step": "检查“主数据模板“中的线路中文名和站点中文名组合是否与”L&E站点匹配模板“中的线路中文名和站点中文名组合一一对应",
                                "reason": "”主数据模板“中以下线路站点组合未出现在”L&E站点匹配模板“中：" + ', '.join(extra_combinations_master_data) + '；'})
            # if len(extra_combinations_station_mapping) != 0:
            #     message.append({
            #                        "step": "检查“主数据模板“中的线路中文名和站点中文名组合是否与”L&E站点匹配模板“中的线路中文名和站点中文名组合一一对应",
            #                        "reason": "”L&E站点匹配模板“中以下线路站点组合未出现在”主数据模板“中：" + ', '.join(
            #                            extra_combinations_station_mapping) + '；'})

        return status, message

    # 检查“主数据模板”中线路数和“线路颜色标识模板”中线路是否一一对应
    def check_line_mapping(self, df_master_data: pd.DataFrame, df_line_color: pd.DataFrame):
        status = True
        message = []
        lines_master_data = set(df_master_data['线路中文名称'].unique().tolist())
        lines_line_color = set(df_line_color['线路中文名'].unique().tolist())
        if lines_master_data != lines_line_color:
            status = False
            extra_lines_master_data = lines_master_data - lines_line_color
            extra_lines_line_color = lines_line_color - lines_master_data
            if len(extra_lines_master_data) != 0:
                message.append({"step": "检查“主数据模板”中线路数和“线路颜色标识模板”中线路是否一一对应",
                                "reason": "”主数据模板“中以下线路未出现在”线路颜色标识模板“中：" + ', '.join(
                                    extra_lines_master_data) + '；'})
            if len(extra_lines_line_color) != 0:
                message.append({
                                   "step": "检查“主数据模板”中线路数和“线路颜色标识模板”中线路是否一一对应",
                                   "reason": "”线路颜色标识模板“中以下线路未出现在”主数据模板“中：" + ', '.join(
                                       extra_lines_line_color) + '；'})
        return status, message

    def one_key_run(self):
        result = []
        sub_status, sub_message = self.check_3_templates_existence(self.file)

        if sub_status != True:
            result.append({'sequence': 1,
                           'status': sub_status,
                           'message': sub_message})
            return result
        status, message = self._check_fields_existence(self.df_master_data,
                                                       ['线路中文名称', '线路英文名称', '站点中文名称','站点英文名称','点位编码'
                                                        '点位中文名称', '点位英文名称', '站点等级序号','中文站点等级','英文站点等级',
                                                        '中文点位类别','英文点位类别','是否电子媒体','CPM价格','非Admin用户可选'])
        if status != True:
            result.append({'sequence': 1,
                           'status': '检查字段',
                           'message': message})
            return result

        # 检查主数据模板表
        process_master_data = [
            self.check_Column_notnull,
                                self.check_linecn_lineen_mapping,
                               self.check_stationcode_stationname_mapping,
                               self.check_stationnamecn_stationnameen_mapping,
                               self.check_stationnamecn_stationlevelname_mapping,
                               self.check_assetcode_uniqueness,
                               self.check_stationlevelcode_stationlevelnamecn_mapping,
                               self.check_stationlevelnamecn_stationlevelnameen_mapping,
                               self.check_assetcategorynamecn_assetcategorynameen_mapping,
                               self.check_isdigital_yesnoonly
                               ]
        dataframe = self.df_master_data
        sequence = 0
        for function in process_master_data:
            sequence += 1
            sub_status, sub_message = function(dataframe)
            result.append({'sequence': str(sequence),
                           'status': sub_status,
                           'message': sub_message})
        # 检查L&E站点匹配模板
        process_station_mapping = [self.check_stationcode_existence]
        dataframe = self.df_station_mapping
        for function in process_station_mapping:
            sequence += 1
            sub_status, sub_message = function(dataframe)
            result.append({'sequence': str(sequence),
                           'status': sub_status,
                           'message': sub_message})

        # 检查线路颜色标识模板
        process_line_color = [self.check_color_length]
        dataframe = self.df_line_color
        for function in process_line_color:
            sequence += 1
            sub_status, sub_message = function(dataframe)
            result.append({'sequence': str(sequence),
                           'status': sub_status,
                           'message': sub_message})

        # 跨表检查
        sequence += 1
        sub_status, sub_message = self.check_combination_linecn_stationcn(self.df_master_data, self.df_station_mapping)
        result.append({'sequence': str(sequence),
                       'status': sub_status,
                       'message': sub_message})
        sequence += 1
        sub_status, sub_message = self.check_line_mapping(self.df_master_data, self.df_line_color)
        result.append({'sequence': str(sequence),
                       'status': sub_status,
                       'message': sub_message})

        return result



# if __name__ == '__main__':
#     file_dir = r"C:\Users\zewen.liang\Downloads\MAM E版本升级需要的数据-新版20240626v2.xlsx"
#     env_info_code = 'mam-shanghai'
#     checker = checkAdminMasterData(file_dir, env_info_code)
#     res = checker.one_key_run()
#     for item in res:
#         print('步骤：' + str(item['sequence']))
#         print('状态：' + str(item['status']))
#         if not item['status']:
#             print('原因：' + str(item['message']))
