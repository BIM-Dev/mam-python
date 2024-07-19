import pandas as pd



class MamLInputFilesCheck:
    def __init__(self):
        # self.environment = environment
        pass

    def _check_fields_existence(self, dataframe: pd.DataFrame, fields_list: list):
        df_columns = dataframe.columns
        if set(fields_list).issubset(set(df_columns)):
            status = True
            message = ''
        else:
            status = False
            missing_fields = set(fields_list) - set(df_columns)
            message = "以下必需字段缺失，请检查文件是否包含这些字段：" + ", ".join(missing_fields) + \
                      '，您可以在此处查看每个文件的必需字段：https://jcdecaux-my.sharepoint.com/:o:/p/zewen_liang/EqcrGAwFx7hGjE5cWl838bkB3bkf9h3dGWEynL39q-Oc7g?e=HV5Idf；' + '\n'
        return status, message

    def _check_unique_keys(self, dataframe: pd.DataFrame, constraint_keys: list):
        df_temp = dataframe[constraint_keys]
        df_temp.reset_index(names='id', inplace=True)
        df_temp_2 = df_temp.groupby(constraint_keys)['id'].count().reset_index(name='count')
        df_temp_3 = df_temp_2[df_temp_2['count']>1].copy()
        df_temp_3['combined'] = df_temp_3[constraint_keys].apply(lambda row: ','.join(row.values.astype(str)), axis=1)
        if df_temp_3.shape[0] == 0:
            status = True
            message = ''
        else:
            status = False
            message = '以下(' + ','.join(constraint_keys) + ')存在多个值：(' + '), ('.join(df_temp_3['combined'].tolist()) + ")；\n"
        return status, message

    def _check_digital_columns(self, dataframe: pd.DataFrame, fields_list: list):
        status = True
        message = ''
        for item in fields_list:
            non_digit_values = []
            for value in dataframe[item]:
                if (not str(value).replace('.', '').isnumeric()) and (str(value) != 'nan'):
                    # print(str(value), str(value).replace('.', ''), '不是数字！')
                    status = False
                    non_digit_values += [str(value)]
            if len(non_digit_values) != 0:
                message += item + '列存在非数字值：' + ','.join(set(non_digit_values)) + '；\n'
        return status, message

    def _check_no_null_value_in_cloumns(self, dataframe: pd.DataFrame, fields_list: list):
        status = True
        message = ''
        temp_df = dataframe[fields_list]
        columns_with_null = temp_df.columns[temp_df.isnull().any()].tolist()
        if len(columns_with_null) != 0:
            status = False
            message = '以下字段有空值：' + ','.join(columns_with_null) + '，请根据实际情况填0或no等值；\n'
        return status, message

    def check_station_dictionary(self, file):
        status, message = self._check_fields_existence(file, ['station_id', 'station_name'])
        if status:
            status_1, message_1 = self._check_unique_keys(file, ['station_id'])
            status_2, message_2 = self._check_unique_keys(file, ['station_name'])
            status_3, message_3 = self._check_digital_columns(file, ['station_id'])
            status_4, message_4 = self._check_no_null_value_in_cloumns(file, file.columns)
            status = status_1 and status_2 and status_3 and status_4
            message = message_1 + message_2 + message_3 + message_4
        return status, message

    def check_lines_stations(self, file):
        status, message = self._check_fields_existence(file, ['metro_line', 'station_from', 'station_to', 'stations_combination'])
        if status:
            status_1, message_1 = self._check_digital_columns(file, ['metro_line'])
            status_2, message_2 = self._check_no_null_value_in_cloumns(file, file.columns)
            status = status_1 and status_2
            message = message_1 + message_2
            # 判断始末站是否与stations_combination中一致，及判断stations_combination中是否有重复
            for index, row in file.iterrows():
                metro_line = str(row['metro_line'])
                station_from = str(row['station_from'])
                station_to = str(row['station_to'])
                stations_combination = str(row['stations_combination']).split(',')
                if station_from != stations_combination[0]:
                    status = False
                    message += metro_line + '的station_from字段不等于stations_combination的第一项；\n'
                if station_to != stations_combination[-1]:
                    status = False
                    message += metro_line + '的station_to字段不等于stations_combination的最后一项；\n'
                station_duplicates = list(set([item for item in stations_combination if stations_combination.count(item) > 1]))
                if len(station_duplicates) > 0:
                    status = False
                    message += metro_line + '的stations_combination中出现重复项: ' + ', '.join(station_duplicates) + '；\n'
        return status, message

    def check_departure_arrival_pax(self, file):
        status, message = self._check_fields_existence(file, ['station_id', 'year', 'month', 'departure', 'arrival'])
        if status:
            status_1, message_1 = self._check_unique_keys(file, ['station_id', 'year', 'month'])
            status_2, message_2 = self._check_digital_columns(file, ['station_id', 'year', 'month', 'departure', 'arrival'])
            status_3, message_3 = self._check_no_null_value_in_cloumns(file, file.columns)
            status = status_1 and status_2 and status_3
            message = message_1 + message_2 + message_3
            # 判断是否每个station_id都出现了12次
            df_temp = file[['station_id', 'month']]
            df_temp_2 = df_temp.groupby('station_id')['month'].count().reset_index(name='count')
            df_temp_3 = df_temp_2[df_temp_2['count'] != 12]
            # print(df_temp_3)
            if df_temp_3.shape[0] != 0:
                status = False
                message += '以下station_id的数据不为12个月：' + ', '.join(df_temp_3['station_id'].astype(str).tolist()) + \
                           '，必须保证每个站点均包含12个月的数据，否则跑出的数会没有缺失月份的数据；\n'
        return status, message

    def check_interchange_pax(self, file):
        status, message = self._check_fields_existence(file, ['station_id', 'year', 'month', 'interchange'])
        if status:
            status_1, message_1 = self._check_unique_keys(file, ['station_id', 'year', 'month'])
            status_2, message_2 = self._check_digital_columns(file, ['station_id', 'year', 'month', 'interchange'])
            status_3, message_3 = self._check_no_null_value_in_cloumns(file, file.columns)
            status = status_1 and status_2 and status_3
            message = message_1 + message_2 + message_3
            # 判断是否每个station_id都出现了12次
            df_temp = file[['station_id', 'month']]
            df_temp_2 = df_temp.groupby('station_id')['month'].count().reset_index(name='count')
            df_temp_3 = df_temp_2[df_temp_2['count'] != 12]
            # print(df_temp_3)
            if df_temp_3.shape[0] != 0:
                status = False
                message += '以下station_id的数据不为12个月：' + ', '.join(
                    df_temp_3['station_id'].astype(str).tolist()) + '，必须保证每个站点均包含12个月的数据，否则跑出的数会没有缺失月份的数据；\n'
        return status, message

    def check_metro_hours(self, file):
        status, message = self._check_fields_existence(file, ['hour'])
        if status:
            status_1, message_1 = self._check_unique_keys(file, ['hour'])
            status_2, message_2 = self._check_no_null_value_in_cloumns(file, file.columns)
            status = status_1 and status_2
            message = message_1 + message_2
            # 以下判断整点数
            hour_field_list = set(file['hour'].tolist())
            error_hours = set([str(x) for x in hour_field_list if not str(x).endswith('00')])
            if len(error_hours) != 0:
                status = False
                message += 'hour字段中以下值不是整点数：' + ','.join(error_hours) + '请确保所有值都是整点数；\n'
        return status, message

    def check_networks(self, file):
        status, message = self._check_fields_existence(file, ['network_id', 'network_name', 'rent_rate',
                                                              'rent_period_days', 'share_of_time'])
        if status:
            status_1, message_1 = self._check_unique_keys(file, ['network_id'])
            status_2, message_2 = self._check_unique_keys(file, ['network_name'])
            status_3, message_3 = self._check_digital_columns(file, ['rent_rate',
                                                              'rent_period_days', 'share_of_time'])
            status_4, message_4 = self._check_no_null_value_in_cloumns(file, file.columns)
            status = status_1 and status_2 and status_3 and status_4
            message = message_1 + message_2 + message_3 + message_4
            # 判断是否有sot大于1:
            df_temp = file[(file['share_of_time'] > 1) & (file['share_of_time'] != 999)]
            if df_temp.shape[0] != 0:
                status = False
                message += '以下network_id的sot大于1：' + ', '.join(
                    df_temp['network_id'].astype(str).tolist()) + '；\n'
            if file.shape[0] == 0:
                status = False
                message += 'networks中至少有一条数据，inventory中也要至少对应新增一列；\n'
        return status, message

    def check_inventory(self, file):
        status, message = self._check_fields_existence(file, ['asset_id', 'station_id', 'metro_line',
                                                              'asset_type', 'is_available', 'is_digital',
                                                              'is_loop', 'nb_slots', 'slot_duration_sec',
                                                              'surface_m2', 'latitude', 'longitude'])
        if status:
            status_1, message_1 = self._check_unique_keys(file, ['asset_id'])
            status_2, message_2 = self._check_digital_columns(file, ['metro_line', 'nb_slots', 'slot_duration_sec', 'surface_m2'])
            # slot_duration_sec不能为空，至少填个0
            status_3, message_3 = self._check_no_null_value_in_cloumns(file, ['asset_id', 'station_id', 'metro_line',
                                                              'asset_type', 'is_available', 'is_digital',
                                                              'is_loop', 'nb_slots', 'slot_duration_sec',
                                                              'surface_m2'])
            status = status_1 and status_2 and status_3
            message = message_1 + message_2 + message_3
            # 判断is_available, is_digital, is_loop是否有除yes和no以外的值
            set_temp_is_available = set(file['is_available'].unique().tolist())
            set_temp_is_digital = set(file['is_digital'].unique().tolist())
            set_temp_is_loop = set(file['is_loop'].unique().tolist())
            if not set_temp_is_available.issubset({'yes', 'no'}):
                status = False
                message += 'is_available字段包含除yes, no外的值(' + ','.join(set_temp_is_available-{'yes', 'no'}) + ')；\n'
            if not set_temp_is_digital.issubset({'yes', 'no'}):
                status = False
                message += 'is_digital字段包含除yes, no外的值(' + ','.join(set_temp_is_digital-{'yes', 'no'}) + ')；\n'
            if not set_temp_is_loop.issubset({'yes', 'no'}):
                status = False
                message += 'is_loop字段包含除yes, no外的值(' + ','.join(set_temp_is_loop-{'yes', 'no'}) + ')；\n'
            # 检查电子点位是否nb_slots>0且slot_duration_sec>0
            temp_df_is_digital_true = file[file['is_digital'] == 'yes']
            set_is_digital_true_slot_duration_sec_is_0 = set(temp_df_is_digital_true[temp_df_is_digital_true['slot_duration_sec']==0]['asset_id'].unique().tolist())
            if len(set_is_digital_true_slot_duration_sec_is_0) != 0:
                status = False
                message += '以下点位为电子点位，但是slot_duration_sec为0：' + ','.join(set_is_digital_true_slot_duration_sec_is_0) + '；\n'
            set_is_digital_true_nb_slots_is_0 = set(temp_df_is_digital_true[temp_df_is_digital_true['nb_slots']==0]['asset_id'].unique().tolist())
            if len(set_is_digital_true_nb_slots_is_0) != 0:
                status = False
                message += '以下点位为电子点位，但是nb_slots为0：' + ','.join(
                    set_is_digital_true_nb_slots_is_0) + '；\n'

        return status, message

    def check_assets_operating_hours(self, file):
        status, message = self._check_fields_existence(file, ['asset_id', 'hour_from', 'hour_to'])
        if status:
            status_1, message_1 = self._check_unique_keys(file, ['asset_id'])
            status_2, message_2 = self._check_no_null_value_in_cloumns(file, file.columns)
            status = status_1 and status_2
            message = message_1 + message_2
            # 以下判断整点数
            for item in ['hour_from', 'hour_to']:
                hour_field_list = set(file[item].tolist())
                error_hours = set([str(x) for x in hour_field_list if not str(x).endswith('00')])
                if len(error_hours) != 0:
                    status = False
                    message += item + '字段中以下值不是整点数：' + ','.join(error_hours) + '请确保所有值都是整点数；\n'
        return status, message

    def check_assets_vs_subpaths(self, file):
        status, message = self._check_fields_existence(file, ['subpath_name', 'asset_id', 'city'])
        if status:
            status_1, message_1 = self._check_unique_keys(file, ['subpath_name', 'asset_id'])
            status_2, message_2 = self._check_no_null_value_in_cloumns(file, file.columns)
            status = status_1 and status_2
            message = message_1 + message_2
            # 判断city是否为唯一值
            set_temp_city = set(file['city'].unique().tolist())
            if len(set_temp_city) > 1:
                status = False
                message += 'city字段存在多个值：' + ','.join(set_temp_city) + '；\n'
            # 判断city是否为environment
            # if file['city'].tolist()[0] != self.environment:
            #     status = False
            #     message += 'city字段不等于当前environment，请将小火车的input文件夹命名为' + self.environment + '并重跑小火车；\n'

        return status, message

    def check_nodes(self, file):
        status, message = self._check_fields_existence(file, ['node_id', 'type', 'station_id', 'city'])
        if status:
            status_1, message_1 = self._check_unique_keys(file, ['node_id', 'station_id'])
            status_2, message_2 = self._check_digital_columns(file, ['station_id'])
            status_3, message_3 = self._check_no_null_value_in_cloumns(file, ['node_id', 'station_id', 'city'])
            status = status_1 and status_2 and status_3
            message = message_1 + message_2 + message_3
            # 判断city是否为唯一值
            set_temp_city = set(file['city'].unique().tolist())
            if len(set_temp_city) > 1:
                status = False
                message += 'city字段存在多个值：' + ','.join(set_temp_city) + '；\n'
            # 判断city是否为environment
            # if file['city'].tolist()[0] != self.environment:
            #     status = False
            #     message += 'city字段不等于当前environment，请将小火车的input文件夹命名为' + self.environment + '并重跑小火车；\n'

        return status, message

    def check_subpaths(self, file):
        status, message = self._check_fields_existence(file, ['node_id_source', 'node_id_destination', 'direction',
                                                              'subpath_weight', 'station_id', 'zone',
                                                              'city'])
        if status:
            status_1, message_1 = self._check_unique_keys(file, ['node_id_source', 'node_id_destination', 'direction', 'station_id'])
            status_2, message_2 = self._check_digital_columns(file, ['station_id'])
            status_3, message_3 = self._check_no_null_value_in_cloumns(file, list(set(file.columns) - {'zone'}))
            status = status_1 and status_2 and status_3
            message = message_1 + message_2 + message_3
            # 判断city是否为唯一值
            set_temp_city = set(file['city'].unique().tolist())
            if len(set_temp_city) > 1:
                status = False
                message += 'city字段存在多个值：' + ','.join(set_temp_city) + '；\n'
            # 判断city是否为environment
            # if file['city'].tolist()[0] != self.environment:
            #     status = False
            #     message += 'city字段不等于当前environment，请将小火车的input文件夹命名为' + self.environment + '并重跑小火车；\n'

        return status, message

    def check_dwell_time(self, file):
        status, message = self._check_fields_existence(file, ['zone', 'direction', 'part_of_week',
                                                              'part_of_day', 'hour', 'station_id',
                                                              'metro_line', 'dwell_time_sec'])
        if status:
            status_1, message_1 = self._check_digital_columns(file, ['station_id', 'metro_line', 'dwell_time_sec'])
            status_2, message_2 = self._check_no_null_value_in_cloumns(file, list(set(file.columns)-{'part_of_week',
                                                                                                'part_of_day', 'hour'}))
            status = status_1 and status_2
            message = message_1 + message_2
        return status, message

    def cross_check(self, df_station_dictionary, df_lines_stations, df_departure_arrival_pax,
                         df_interchange_pax, df_metro_hours, df_networks,
                         df_inventory, df_assets_opearting_hours, df_assets_vs_subpaths,
                         df_nodes, df_subpaths, df_dwell_time):
        table_mapping_dict = {'station_dictionary': df_station_dictionary, 'lines_stations': df_lines_stations,
                              'departure_arrival_pax': df_departure_arrival_pax, 'interchange_pax': df_interchange_pax,
                              'metro_hours': df_metro_hours, 'networks': df_networks,
                              'inventory': df_inventory, 'assets_operating_hours': df_assets_opearting_hours,
                              'assets_vs_subpaths': df_assets_vs_subpaths, 'nodes': df_nodes,
                              'subpaths': df_subpaths, 'dwell_time': df_dwell_time}

        # 判断各文件中的station是否在station_dictionary的station_id列中
        def get_extra_or_missing_stations():
            status = True
            message = ''
            stations_master = set(df_station_dictionary['station_id'].astype(str).tolist())
            # 以下是查找各表中，多出来（不存在于station_dictionary中）的station_id
            for item in ['departure_arrival_pax', 'interchange_pax', 'inventory', 'nodes', 'subpaths', 'dwell_time']:
                target_stations = set(table_mapping_dict[item]['station_id'].astype(str).tolist())
                extra_stations = target_stations - stations_master
                if len(extra_stations) != 0:
                    status = False
                    message += item + '表的station_id列中以下项未出现在station_dictionary中：' + ','.join(extra_stations) + \
                               '，station_dictionary表是站点的主数据表，所有的用到的station_id必须出现在此表中；\n'
            # 以下是查找各表中，遗漏掉（存在于station_dictionary中但不存在于目标表中）的station_id
            for item in []:
                target_stations = set(table_mapping_dict[item]['station_id'].astype(str).tolist())
                missing_stations = stations_master - target_stations
                if len(missing_stations) != 0:
                    status = False
                    message += item + '表的station_id列中缺失以下项：' + ','.join(
                        missing_stations) + \
                               '，station_dictionary表是站点的主数据表，这些station_id出现在主数据表中但未出现在此表中；\n'

            # 处理lines_stations中的stations_combination列
            all_stations_in_stations_combination = set([])
            for index, row in df_lines_stations.iterrows():
                stations_combination = set(str(row['stations_combination']).split(','))
                all_stations_in_stations_combination.update(stations_combination)
            extra_stations = all_stations_in_stations_combination - stations_master
            if len(extra_stations) != 0:
                status = False
                message += 'lines_stations表的stations_combination列中以下项未出现在station_dictionary中：' + ','.join(
                    extra_stations) + '，station_dictionary表是站点的主数据表，所有的用到的station_id必须出现在此表中；\n'
            missing_stations = stations_master - all_stations_in_stations_combination
            if len(missing_stations) != 0:
                status = False
                message += 'lines_stations表的stations_combination列中缺失以下项：' + ','.join(
                    missing_stations) + '，station_dictionary表是站点的主数据表，这些station_id出现在主数据表中但未出现在此表中；\n'
            return status, message
        status_1, message_1 = get_extra_or_missing_stations()

        # 判断各文件中的line是否在lines_stations的metro_line列中
        def get_extra_or_missing_lines():
            status = True
            message = ''
            lines_master = set(df_lines_stations['metro_line'].astype(str).tolist())
            # 以下是查找各表中，多出来（不存在于lines_stations中）的metro_line
            for item in ['inventory', 'dwell_time']:
                target_lines = set(table_mapping_dict[item]['metro_line'].astype(str).tolist())
                extra_lines = target_lines - lines_master
                if len(extra_lines) != 0:
                    status = False
                    message += item + '表的metro_line列中以下项未出现在lines_stations中：' + ','.join(extra_lines) + \
                               '，lines_stations是线路的主数据表，所有用到的metro_line必须出现在此表中；\n'
            # 以下是查找各表中，遗漏掉（存在于station_dictionary中但不存在于目标表中）的station_id
            for item in []:
                target_lines = set(table_mapping_dict[item]['metro_line'].astype(str).tolist())
                missing_lines = lines_master - target_lines
                if len(missing_lines) != 0:
                    status = False
                    message += item + '表的metro_line列中缺失以下项：' + ','.join(missing_lines) + \
                               '，lines_stations是线路的主数据表，这些metro_line出现在主数据表中，但未出现在此表中；\n'
            return status, message
        status_2, message_2 = get_extra_or_missing_lines()

        def get_extra_assets():
            status = True
            message = ''
            assets_master = set(df_inventory['asset_id'].astype(str).tolist())
            for item in ['assets_operating_hours', 'assets_vs_subpaths']:
                target_assets = set(table_mapping_dict[item]['asset_id'].astype(str).tolist())
                extra_assets = target_assets - assets_master
                if len(extra_assets) != 0:
                    status = False
                    message += item + '表的asset_id列中以下项未出现在inventory中：' + ','.join(
                        extra_assets) + '，inventory是点位的主数据表，所有用到的asset_id必须出现在此表中；\n'
            return status, message
        status_3, message_3 = get_extra_assets()

        def check_inventory_networks_columns():
            status = True
            message = ''
            networks_master = set(df_networks['network_id'].astype(str).tolist())
            networks_columns = set(df_inventory.columns) - set(['asset_id', 'station_id', 'metro_line',
                                                              'asset_type', 'is_available', 'is_digital',
                                                              'is_loop', 'nb_slots', 'slot_duration_sec',
                                                              'surface_m2', 'latitude', 'longitude'])
            extra_networks = networks_columns - networks_master
            missing_networks = networks_master - networks_columns
            if len(extra_networks) != 0:
                status = False
                message += 'inventory表中以下列未出现在networks表中：' + ','.join(extra_networks) + \
                           '，inventory表的所有network列必须和networks表中的network_name一一对应；\n'
            if len(missing_networks) != 0:
                status = False
                message += 'networks表中以下项未出现在inventory表的列中：' + ','.join(missing_networks) + \
                           '，networks表的network_name字段中的每一项必须和inventory表的所有network列一一对应；\n'

            return status, message
        status_4, message_4 = check_inventory_networks_columns()


        status = status_1 and status_2 and status_3 and status_4
        message = message_1 + message_2 + message_3 + message_4
        return status, message

    def local_one_key_run(self, df_station_dictionary, df_lines_stations, df_departure_arrival_pax,
                         df_interchange_pax, df_metro_hours, df_networks,
                         df_inventory, df_assets_opearting_hours, df_assets_vs_subpaths,
                         df_nodes, df_subpaths, df_dwell_time):
        print('正在检查station_dictionary...')
        status, message = self.check_station_dictionary(df_station_dictionary)
        print('ok' if status else message)
        print('正在检查lines_stations...')
        status, message = self.check_lines_stations(df_lines_stations)
        print('ok' if status else message)
        print('正在检查departure_arrival_pax...')
        status, message = self.check_departure_arrival_pax(df_departure_arrival_pax)
        print('ok' if status else message)
        print('正在检查interchange_pax...')
        status, message = self.check_interchange_pax(df_interchange_pax)
        print('ok' if status else message)
        print('正在检查metro_hours...')
        status, message = self.check_metro_hours(df_metro_hours)
        print('ok' if status else message)
        print('正在检查networks...')
        status, message = self.check_networks(df_networks)
        print('ok' if status else message)
        print('正在检查inventory...')
        status, message = self.check_inventory(df_inventory)
        print('ok' if status else message)
        print('正在检查assets_operating_hours...')
        status, message = self.check_assets_operating_hours(df_assets_opearting_hours)
        print('ok' if status else message)
        print('正在检查assets_vs_subpaths...')
        status, message = self.check_assets_vs_subpaths(df_assets_vs_subpaths)
        print('ok' if status else message)
        print('正在检查nodes...')
        status, message = self.check_nodes(df_nodes)
        print('ok' if status else message)
        print('正在检查subpaths...')
        status, message = self.check_subpaths(df_subpaths)
        print('ok' if status else message)
        print('正在跨文件交叉检查...')
        status, message = self.cross_check(df_station_dictionary, df_lines_stations, df_departure_arrival_pax,
                                              df_interchange_pax, df_metro_hours, df_networks,
                                              df_inventory, df_assets_opearting_hours, df_assets_vs_subpaths,
                                              df_nodes, df_subpaths)
        print('ok' if status else message)

# if __name__ == '__main__':
#     checker = MamLInputFilesCheck(environment='mam-nanjing')

    # # 检查station_dictionary
    # df = pd.read_csv(r"C:\Users\zewen.liang\OneDrive - JCDECAUX\桌面\数据\stations_dictionary.csv", sep=";")
    # status, message = checker.check_station_dictionary(df)
    # print('检查station_dictionary', status, message)
    #
    # # 检查line_station
    # df = pd.read_csv(r"C:\Users\zewen.liang\OneDrive - JCDECAUX\桌面\数据\line_stations.csv", sep=";")
    # status, message = checker.check_line_stations(df)
    # print('检查line_station', status, message)
    #
    # # 检查station_pax
    # df = pd.read_csv(r"C:\Users\zewen.liang\OneDrive - JCDECAUX\桌面\数据\station_pax.csv", sep=";")
    # status, message = checker.check_station_pax(df)
    # print('检查station_pax', status, message)
    #
    # # 检查interchange_pax
    # df = pd.read_csv(r"C:\Users\zewen.liang\OneDrive - JCDECAUX\桌面\数据\interchange_pax.csv", sep=";")
    # status, message = checker.check_interchange_pax(df)
    # print('检查interchange_pax', status, message)
    #
    # # 检查metro_hours
    # df = pd.read_csv(r"C:\Users\zewen.liang\OneDrive - JCDECAUX\桌面\数据\metro_hours.csv", sep=";")
    # status, message = checker.check_metro_hours(df)
    # print('检查metro_hours', status, message)
    #
    # # 检查networks
    # df = pd.read_csv(r"C:\Users\zewen.liang\OneDrive - JCDECAUX\桌面\数据\networks.csv", sep=";")
    # status, message = checker.check_networks(df)
    # print('检查networks', status, message)
    #
    # # 检查inventory
    # df = pd.read_csv(r"C:\Users\zewen.liang\OneDrive - JCDECAUX\桌面\数据\inventory.csv", sep=";")
    # status, message = checker.check_inventory(df)
    # print('检查inventory', status, message)
    #
    # # 检查assets_operating_hours
    # df = pd.read_csv(r"C:\Users\zewen.liang\OneDrive - JCDECAUX\桌面\数据\assets_operating_hours.csv", sep=";")
    # status, message = checker.check_assets_operating_hours(df)
    # print('检查assets_operating_hours', status, message)
    #
    # # 检查assets_vs_subpaths
    # df = pd.read_csv(r"C:\Users\zewen.liang\Downloads\mam_paths_file_creator\mam_paths_file_creator\mam-nanjing_out\assets_vs_subpaths.csv", sep=";")
    # status, message = checker.check_assets_vs_subpaths(df)
    # print('检查assets_vs_subpaths', status, message)
    #
    # # 检查nodes
    # df = pd.read_csv(
    #     r"C:\Users\zewen.liang\Downloads\mam_paths_file_creator\mam_paths_file_creator\mam-nanjing_out\nodes.csv",
    #     sep=";")
    # status, message = checker.check_nodes(df)
    # print('检查nodes', status, message)
    #
    # # 检查subpaths
    # df = pd.read_csv(
    #     r"C:\Users\zewen.liang\Downloads\mam_paths_file_creator\mam_paths_file_creator\mam-nanjing_out\subpaths.csv",
    #     sep=";")
    # status, message = checker.check_subpaths(df)
    # print('检查subpaths', status, message)

    # # 跨文件检查
    # path = r"C:\Users\zewen.liang\Downloads\南京数据更新\\"
    # df_station_dictionary = pd.read_csv(path + "stations_dictionary.csv", sep=";")
    # df_line_stations = pd.read_csv(path + "line_stations.csv", sep=";")
    # df_station_pax = pd.read_csv(path + "station_pax.csv", sep=";")
    # df_interchange_pax = pd.read_csv(path + "interchange_pax.csv", sep=";")
    # df_metro_hours = pd.read_csv(path + "metro_hours.csv", sep=";")
    # df_networks = pd.read_csv(path + "networks.csv", sep=";")
    # df_inventory = pd.read_csv(path + "inventory.csv", sep=";")
    # df_assets_operating_hours = pd.read_csv(path + "assets_operating_hours.csv", sep=";")
    # df_assets_vs_subpaths = pd.read_csv(path + "assets_vs_subpaths.csv", sep=";")
    # df_nodes = pd.read_csv(path + "nodes.csv", sep=";")
    # df_subpaths = pd.read_csv(path + "subpaths.csv", sep=";")
    # # status, message = checker.cross_check(df_station_dictionary, df_line_stations, df_station_pax,
    # #                                          df_interchange_pax, df_metro_hours, df_networks,
    # #                                          df_inventory, df_assets_operating_hours, df_assets_vs_subpaths,
    # #                                          df_nodes, df_subpaths)
    # #
    # # print(status, message)
    # checker.local_one_key_run(df_station_dictionary, df_line_stations, df_station_pax,
    #                                          df_interchange_pax, df_metro_hours, df_networks,
    #                                          df_inventory, df_assets_operating_hours, df_assets_vs_subpaths,
    #                                          df_nodes, df_subpaths)