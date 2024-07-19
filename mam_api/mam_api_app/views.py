from rest_framework.response import Response
from rest_framework import views
from django.http import HttpResponse

from mam_api_app.service.login_service import LoginService

from mam_api_app.service.mail_service import MailService
from mam_api_app.service.generateImpressionReport import generateReport

from mam_api_app.utils.auth_util import AuthUtil

from .serializers import SerializerLogin
from .serializers import SerializerLastForecast
from .serializers import SerializerAvailableYearMonth
from django.http import FileResponse
import logging
import pandas as pd
from mam_api_app.service.checkAdminMasterData import checkAdminMasterData
from mam_api_app.service.mamLInputFilesCheck import MamLInputFilesCheck
from mam_api_app.service.convertor import Convertor
from mam_api_app.service.sendMamLFile import MailMasterDataService
from mam_api_app.service.adminUser import MamPermissionGrantByBu
import os

from .service.adminMasterData import Mam_e_master_data_generator

logger = logging.getLogger()

# decorator
def check_authority_decorator(view_func):
    def wrapper(request, *args, **kwargs):
         # Call the check_authority method with the request
        if AuthUtil.check_authority(request): # If it returns True, proceed with the original view function
            return view_func(request, *args, **kwargs)
        else: # If it returns False, return a 401 response
            return HttpResponse(status=401)
    return wrapper

class generateIR(views.APIView):
    def post(self, request):
        report_dir = generateReport().one_key_generate_report(51, request.data['language'], request.data['env_info_code'])
        # print(report_dir)
        results = {
            'email': report_dir,
        }
        file = open(report_dir, 'rb')
        name = report_dir[2:]
        response = FileResponse(file)
        # 增加headers
        response['Content-Type'] = 'application/octet-stream'
        response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
        response['Content-Disposition'] = "attachment; filename={}".format(name)
        return response


class AutoLogin(views.APIView):
    def post(self, request):
        #cookie = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ4aWFveXUueWFuQGpjZGVjYXV4LmNvbSIsImV4cCI6MTY5MzgzODYyOX0.I8j7g0T8kcZbUUKHeCPaFAEDdWsKqZY7T-4aoCcLo0HyQj_X4dHWZcOoBb9v4EvRQmYahZJA0t_CYXqxp8pdhg'
        cookie = request.COOKIES.get('accessToken', 'Anonymous')
        flag, authority, email = LoginService.auto_login(cookie)
        results = {
            'flag': flag,
            'authority': SerializerLogin(instance=authority).data if flag else '',
            'email': email,
        }
        response = Response(results)
        return response


class LdapLogin(views.APIView):
    def post(self, request):
        account = request.data
        result = LoginService.login_check(account)
        response = Response(result)
        return response


class Login(views.APIView):
    def post(self, request):
        account = request.data
        logger.info(account['email'] + ' login action')
        check_account = LoginService.login_check(account)
        results = SerializerLogin(instance=check_account)
        response = Response(results.data)
        return response

class SendMail(views.APIView):
    def post(self, request):
        # email = request.data['email']
        # content = request.data['content']
        # if request.data['language'] == 'cn':
        #     result = MailService.send_notice_cn(email, content, request,  request.data['env'])
        # else:
        #     result = MailService.send_notice_en(email, content, request,  request.data['env'])
        response = Response('result')
        return response

class getUserList(views.APIView):
    def post(self, request):
        response = Response('')
        return response

class checkMasterData(views.APIView):
    def post(self, request):

        file = request.data['file']

        # result = checkAdminMasterData.file_check(file,request.data['environment'])
        # if result == []:
        #     result_return = {"status":"success"}
        # else:
        #     result_return = {"status": 'error', "step": [i['step'] for i in result if i.get('step')], 'description': [i['reason'] for i in result if i.get('reason')]}
        # return Response(result_return)
        checker = checkAdminMasterData(file, request.data['environment'])
        res = checker.one_key_run()
        result_return = {"status": "success",'step':[],'description':[]}
        for item in res:
            if not item['status']:
                result_return['status'] = 'error'
                result_return['step'].append(item['message'][0]['step'])
                result_return['description'].append(item['message'][0]['reason'])

        return Response(result_return)

class updateMasterDate(views.APIView):
    def post(self, request):
        file = request.data['file']
        # Dev
        connection_parameters = {
            "host": "localhost",
            "port": "5432",
            "database": "generic_am_mam",
            "user": "postgres",
            "password": "docker"
        }
        # DBUSER = "postgres"
        # DBPASSWORD = "aa4f8ef88f3b1e65aee010a79e69d71b"
        # DBHOST = "localhost"
        # DBPORT = "5432"
        schema = 'public'
        file_dir = file
        master_data_sheet_name = '主数据模板'
        line_colors_sheet_name = '线路颜色标识模板'
        station_mapping_sheet_name = 'L&E站点匹配模板'
        env_info_code = request.data['environment']
        user = request.data['email']
        generator = Mam_e_master_data_generator(file_dir, master_data_sheet_name, line_colors_sheet_name,
                                                station_mapping_sheet_name, connection_parameters, env_info_code, user)
        # export_file_dir = r'C:\Users\xiaoyu.yan\OneDrive - JCDECAUX\Desktop\Projects\MAM\MAM Enhancement\主数据\NEW_exported.xlsx'
        # generator.export_excel_file(export_file_dir)
        generator.upload_data_to_db(connection_parameters=connection_parameters, schema=schema,
                                    delete_existed_rows=True,
                                    truncate_existed_table=False)
        return Response('')

class uniqueCheckMamFile(views.APIView):
    def post(self, request):
            file = request.data['file']
            file_module = request.data['module']
            file_type = request.data['type']
            print(file)
            print(file_module)
            print(file_type)
            status = True
            checker = MamLInputFilesCheck()

            # # 检查station_dictionary
            if file_module == 'station_dictionary':
                if file_type == 'csv':
                    df = pd.read_csv(file, sep=";")
                elif file_type == 'xlsx' or file_type == 'xls':
                    df = pd.read_excel(file)
                status, message = checker.check_station_dictionary(df)

            elif file_module == 'line_station':
                if file_type == 'csv':
                    df = pd.read_csv(file, sep=";")
                elif file_type == 'xlsx' or file_type == 'xls':
                    df = pd.read_excel(file)
                status, message = checker.check_lines_stations(df)

            elif file_module == 'station_pax':
                if file_type == 'csv':
                    df = pd.read_csv(file, sep=";")
                elif file_type == 'xlsx' or file_type == 'xls':
                    df = pd.read_excel(file )
                status, message = checker.check_departure_arrival_pax(df)

            elif file_module == 'interchange_pax':
                if file_type == 'csv':
                    df = pd.read_csv(file, sep=";")
                elif file_type == 'xlsx' or file_type == 'xls':
                    df = pd.read_excel(file)
                status, message = checker.check_interchange_pax(df)

            elif file_module == 'metro_hours':
                if file_type == 'csv':
                    df = pd.read_csv(file, sep=";")
                elif file_type == 'xlsx' or file_type == 'xls':
                    df = pd.read_excel(file)
                status, message = checker.check_metro_hours(df)

            elif file_module == 'networks':
                if file_type == 'csv':
                    df = pd.read_csv(file, sep=";")
                elif file_type == 'xlsx' or file_type == 'xls':
                    df = pd.read_excel(file)
                status, message = checker.check_networks(df)

            elif file_module == 'inventory':
                if file_type == 'csv':
                    df = pd.read_csv(file, sep=";")
                elif file_type == 'xlsx' or file_type == 'xls':
                    df = pd.read_excel(file)
                status, message = checker.check_inventory(df)

            elif file_module == 'asset_operating_hours':
                if file_type == 'csv':
                    df = pd.read_csv(file, sep=";")
                elif file_type == 'xlsx' or file_type == 'xls':
                    df = pd.read_excel(file)
                status, message = checker.check_assets_operating_hours(df)

            elif file_module == 'assets_vs_subpaths':
                if file_type == 'csv':
                    df = pd.read_csv(file, sep=";")
                elif file_type == 'xlsx' or file_type == 'xls':
                    df = pd.read_excel(file)
                status, message = checker.check_assets_vs_subpaths(df)

            elif file_module == 'nodes':
                if file_type == 'csv':
                    df = pd.read_csv(file, sep=";")
                elif file_type == 'xlsx' or file_type == 'xls':
                    df = pd.read_excel(file)
                status, message = checker.check_nodes(df)

            elif file_module == 'subpaths':
                if file_type == 'csv':
                    df = pd.read_csv(file, sep=";")
                elif file_type == 'xlsx' or file_type == 'xls':
                    df = pd.read_excel(file)
                status, message = checker.check_subpaths(df)

            elif file_module == 'dwel':
                if file_type == 'csv':
                    df = pd.read_csv(file, sep=";")
                elif file_type == 'xlsx' or file_type == 'xls':
                    df = pd.read_excel(file)
                status, message = checker.check_dwell_time(df)

            if status == True:
                result_return = {"status": "success"}
            else:
                result_return = {"status": 'error', "message": message}

            return Response(result_return)

class globalCheckMamFile(views.APIView):
    def post(self, request):
            station_dictionary = request.data['station_dictionary']
            line_station = request.data['line_station']
            station_pax = request.data['station_pax']
            interchange_pax = request.data['interchange_pax']
            metro_hours = request.data['metro_hour']
            networks = request.data['networks']
            inventory = request.data['inventory']
            asset_operating_hours = request.data['asset_operating_hours']
            assets_vs_subpaths = request.data['assets_vs_subpaths']
            nodes = request.data['nodes']
            subpaths = request.data['subpaths']
            dwell_time = request.data['dwell_time']

            if station_dictionary.name.split('.')[len(station_dictionary.name.split('.')) - 1] == 'csv':
                df_station_dictionary = pd.read_csv(station_dictionary, sep=";")
            else:
                df_station_dictionary = pd.read_excel(station_dictionary)

            if line_station.name.split('.')[len(line_station.name.split('.')) - 1] == 'csv':
                df_line_station = pd.read_csv(line_station, sep=";")
            else:
                df_line_station = pd.read_excel(line_station)

            if station_pax.name.split('.')[len(station_pax.name.split('.')) - 1] == 'csv':
                df_station_station_pax = pd.read_csv(station_pax, sep=";")
            else:
                df_station_station_pax = pd.read_excel(station_pax)

            if interchange_pax.name.split('.')[len(interchange_pax.name.split('.')) - 1] == 'csv':
                df_interchange_pax = pd.read_csv(interchange_pax, sep=";")
            else:
                df_interchange_pax = pd.read_excel(interchange_pax)

            if metro_hours.name.split('.')[len(metro_hours.name.split('.')) - 1] == 'csv':
                df_metro_hours = pd.read_csv(metro_hours, sep=";")
            else:
                df_metro_hours = pd.read_excel(metro_hours)

            if networks.name.split('.')[len(networks.name.split('.')) - 1] == 'csv':
                df_networks = pd.read_csv(networks, sep=";")
            else:
                df_networks = pd.read_excel(networks)

            if inventory.name.split('.')[len(inventory.name.split('.')) - 1] == 'csv':
                df_inventory = pd.read_csv(inventory, sep=";")
            else:
                df_inventory = pd.read_excel(inventory)

            if asset_operating_hours.name.split('.')[len(asset_operating_hours.name.split('.')) - 1] == 'csv':
                df_asset_operating_hours = pd.read_csv(asset_operating_hours, sep=";")
            else:
                df_asset_operating_hours = pd.read_excel(asset_operating_hours)

            if assets_vs_subpaths.name.split('.')[len(assets_vs_subpaths.name.split('.')) - 1] == 'csv':
                df_assets_vs_subpaths = pd.read_csv(assets_vs_subpaths, sep=";")
            else:
                df_assets_vs_subpaths = pd.read_excel(assets_vs_subpaths)

            if nodes.name.split('.')[len(nodes.name.split('.')) - 1] == 'csv':
                df_nodes = pd.read_csv(nodes, sep=";")
            else:
                df_nodes = pd.read_excel(nodes)

            if subpaths.name.split('.')[len(subpaths.name.split('.')) - 1] == 'csv':
                df_subpaths = pd.read_csv(subpaths, sep=";")
            else:
                df_subpaths = pd.read_excel(subpaths)

            if dwell_time.name.split('.')[len(dwell_time.name.split('.')) - 1] == 'csv':
                df_dwell_time = pd.read_csv(dwell_time, sep=";")
            else:
                df_dwell_time = pd.read_excel(dwell_time)
            status = True
            checker = MamLInputFilesCheck()
            status, message = checker.cross_check(df_station_dictionary, df_line_station, df_station_station_pax,
                                                     df_interchange_pax, df_metro_hours, df_networks,
                                                     df_inventory, df_asset_operating_hours, df_assets_vs_subpaths,
                                                     df_nodes, df_subpaths, df_dwell_time)
            if status == True:
                result_return = {"status": "success"}
            else:
                result_return = {"status": 'error', "message": message}

            return Response(result_return)

class sendMamLFile(views.APIView):
    def post(self, request):
        os.makedirs(os.getcwd() + '/MAM_L_File/' + request.data['environment'] + '/' + request.data['time'] + '/跑数文件')
        station_dictionary = request.data['station_dictionary']
        line_station = request.data['line_station']
        station_pax = request.data['station_pax']
        interchange_pax = request.data['interchange_pax']
        metro_hours = request.data['metro_hour']
        networks = request.data['networks']
        inventory = request.data['inventory']
        asset_operating_hours = request.data['asset_operating_hours']
        assets_vs_subpaths = request.data['assets_vs_subpaths']
        nodes = request.data['nodes']
        subpaths = request.data['subpaths']
        dwell_time = request.data['dwell_time']

        modules = [station_dictionary, line_station, station_pax, interchange_pax, metro_hours,
                   networks, inventory, asset_operating_hours, assets_vs_subpaths, nodes,
                   subpaths, dwell_time]
        for item in modules:  # 第二个实例
            f = open(os.getcwd() + '/MAM_L_File/' + request.data['environment'] + '/' + request.data[
                'time'] + '/跑数文件/' + item.name, 'wb')
            f.write(item.read())
            f.close()
        convertor = Convertor()
        folder_to_be_convert = os.getcwd() + '/MAM_L_File/' + request.data['environment'] + '/' + request.data[
            'time'] + '/跑数文件'
        convertor.batch_generate_csv_pack_and_excel_pack(folder_to_be_convert)

        input_path = os.getcwd() + '/MAM_L_File/' + request.data['environment'] + '/' + request.data[
            'time'] + '/跑数文件 csv版本'
        output_path = os.getcwd() + '/MAM_L_File/' + request.data['environment'] + '/' + request.data[
            'time'] + '/跑数文件 csv版本.zip'
        convertor.zipDir(input_path, output_path)


        input_path = os.getcwd() + '/MAM_L_File/' + request.data['environment'] + '/' + request.data[
            'time'] + '/跑数文件 excel版本'
        output_path = os.getcwd() + '/MAM_L_File/' + request.data['environment'] + '/' + request.data[
            'time'] + '/跑数文件 excel版本.zip'
        convertor.zipDir(input_path, output_path)
        MailMasterDataService.send_notice(request.data['environment'], request.data['time'],
                                          request.data['email'])

        return Response('result_return')

class insertOneUser2(views.APIView):
    def post(self, request):
        connection_parameters = {
            "host": "localhost",
            "port": "5432",
            "database": "data_portal",
            "user": "postgres",
            "password": "docker"
        }
        # dev
        # connection_parameters = {
            #     "host": "10.4.46.229",
            #     "port": "5432",
            #     "database": "data_portal",
            #     "user": "postgres",
            #     "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
            # }
            # production
            # connection_parameters = {
            #     "host": "10.4.102.102",
            #     "port": "5432",
            #     "database": "data_portal",
            #     "user": "data_portal",
            #     "password": "a7W3htXkhMDJD2Sn"
            # }

        mpgbb = MamPermissionGrantByBu(operator=request.data['user'], env_info_code=request.data['environment'], schema='public',
                                           connection_parameters=connection_parameters)
        a = [{'email':request.data['email'], 'group':request.data['environment']+'-'+request.data['role']}]
        print(a)
        response = mpgbb.admin_batch_add_group_for_users(a)
        print(response)
        if response[0]['status'] == 'Succeeded.':

            return Response({'status': 200})
        else:
            return Response({'status': response[0]['status']})

class getUser(views.APIView):
    def post(self, request):
        connection_parameters = {
            "host": "localhost",
            "port": "5432",
            "database": "data_portal",
            "user": "postgres",
            "password": "docker"
        }
        mpgbb = MamPermissionGrantByBu(operator='', env_info_code=request.data['environment'],
                                       schema='public',
                                       connection_parameters=connection_parameters)
        print('test')
        response = mpgbb.getUsersList()
        print(response)
        return Response(response)
        # getUserList

class insertOneUser(views.APIView):
    def post(self, request):
        connection_parameters = {
            "host": "localhost",
            "port": "5432",
            "database": "data_portal",
            "user": "postgres",
            "password": "docker"
        }
        # dev
        # connection_parameters = {
            #     "host": "10.4.46.229",
            #     "port": "5432",
            #     "database": "data_portal",
            #     "user": "postgres",
            #     "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
            # }
            # production
            # connection_parameters = {
            #     "host": "10.4.102.102",
            #     "port": "5432",
            #     "database": "data_portal",
            #     "user": "data_portal",
            #     "password": "a7W3htXkhMDJD2Sn"
            # }

        mpgbb = MamPermissionGrantByBu(operator=request.data['user'], env_info_code=request.data['environment'], schema='public',
                                           connection_parameters=connection_parameters)
        response = mpgbb.insertOnerUser(request.data['email'],request.data['environment']+'-'+request.data['role'],request.data['user'])
        print(response)
        if response == []:

            return Response({'status': 200})
        else:
            return Response({'status': response[0]['status']})

class uploadUserBatch(views.APIView):
    def post(self, request):
        file = request.data['file']
        if file.name.split('.')[len(file.name.split('.')) - 1] == 'csv':
            df = pd.read_csv(file, sep=";")
        else:
            df = pd.read_excel(file)
        print(df)
        connection_parameters = {
            "host": "localhost",
            "port": "5432",
            "database": "data_portal",
            "user": "postgres",
            "password": "docker"
        }
        # dev
        # connection_parameters = {
        #     "host": "10.4.46.229",
        #     "port": "5432",
        #     "database": "data_portal",
        #     "user": "postgres",
        #     "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
        # }
        # production
        # connection_parameters = {
        #     "host": "10.4.102.102",
        #     "port": "5432",
        #     "database": "data_portal",
        #     "user": "data_portal",
        #     "password": "a7W3htXkhMDJD2Sn"
        # }

        mpgbb = MamPermissionGrantByBu(operator='lucy.wu@jcdecaux.com', env_info_code=request.data['environment'],
                                       schema='public',
                                       connection_parameters=connection_parameters)

        return Response({''})

class updateUserRole(views.APIView):
    def post(self, request):
        connection_parameters = {
            "host": "localhost",
            "port": "5432",
            "database": "data_portal",
            "user": "postgres",
            "password": "docker"
        }
        mpgbb = MamPermissionGrantByBu(operator=request.data['user'], env_info_code=request.data['environment'],
                                       schema='public',
                                       connection_parameters=connection_parameters)
        response = mpgbb.updateUserRole(request.data['email'],  request.data['role'][0]
                                        )
        if response[0]['status'] == 200:

            return Response({'status': 200, 'first_name': response[0]['first_name'],'last_name': response[0]['last_name'],
                             'title':response[0]['title'], 'location': response[0]['location'], 'business_unit':response[0]['business_unit'],
                             'company':response[0]['company']})
        else:
            return Response({'status': response[0]['status']})

class deleteUserRole(views.APIView):
    def post(self, request):
        connection_parameters = {
            "host": "localhost",
            "port": "5432",
            "database": "data_portal",
            "user": "postgres",
            "password": "docker"
        }
        mpgbb = MamPermissionGrantByBu(operator=request.data['user'], env_info_code=request.data['environment'],
                                       schema='public',
                                       connection_parameters=connection_parameters)
        response = mpgbb.deleteUserRole(request.data['email']
                                        )
        if response == []:

            return Response({'status': 200})
        else:
            return Response({'status': response[0]['status']})