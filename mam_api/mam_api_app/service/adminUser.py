import pandas as pd
import psycopg2
import json
from psycopg2.extras import RealDictCursor
from ldap3 import Server, Connection, ALL, SUBTREE


class MamPermissionGrantByBu:
    def __init__(self, operator: str, env_info_code: str, schema: str,connection_parameters: dict = None):
        self.operator = operator
        self.env_info_code = env_info_code
        self.schema = 'public'
        if connection_parameters is None:
            self.connection_parameters = {
                "host": "localhost",
                "port": "5432",
                "database": "generic_am_mam",
                "user": "postgres",
                "password": "docker"
            }
        else:
            self.connection_parameters = connection_parameters
        if schema is None:
            self.schema = 'public'
        else:
            self.schema = schema

    def _execute_sql_return_dict(self, sql: str):
        conn = psycopg2.connect(**self.connection_parameters)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql)
        db_data = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        return db_data

    def retrieve_all_users_in_current_env(self):
        sql = f"""
        SELECT A.id, A.first_name, A.last_name, A.title, A.email, A.location, 
            A.business_unit, A.company
            FROM {self.schema}.base_user	A
        INNER JOIN (
            SELECT user_id, group_id FROM {self.schema}.user_group
        ) B
        ON A.id = B.user_id
        INNER JOIN (
            SELECT group_id,environment_id FROM {self.schema}.group_environment
        ) C
        ON B.group_id = C.group_id
        INNER JOIN (
            SELECT id, code FROM {self.schema}.environment
        ) D
        ON C.environment_id = D.id
        WHERE upper(D.code) = upper('{self.env_info_code}')
        """
        response = self._execute_sql_return_dict(sql)
        list_response = list(response)
        print(f"{len(response)} rows retrieved for environment {self.env_info_code}.")
        return list_response

    def _check_user_existence(self, email: str):
        sql = f"""
        SELECT is_enabled FROM {self.schema}.base_user
        WHERE upper(email) = upper('{email}')
        """
        response = self._execute_sql_return_dict(sql)
        print(f"{len(response)} row(s) retrieved for {email}.")
        if len(response) == 0:
            return False
        else:
            return True

    def _update_user_information_and_enable(self, first_name: str, last_name: str, title: str,
                                            location: str, business_unit: str, company: str, timezone: str, email: str):
        sql = f"""
        UPDATE {self.schema}.base_user SET
        first_name = '{first_name}', last_name = '{last_name}', title = '{title}', 
        updated_at = CURRENT_TIMESTAMP, location = '{location}', business_unit = '{business_unit}',
        company = '{company}', is_enabled = TRUE, timezone = '{timezone}'
        WHERE upper(email) = upper('{email}')
        RETURNING id
        """
        response = self._execute_sql_return_dict(sql)
        print(f"{email}'s profile has been updated and force enabled. user_id: {response[0]['id']}")

    def _update_user_information(self, first_name: str, last_name: str, title: str,
                                 location: str, business_unit: str, company: str, timezone: str, email: str):
        sql = f"""
         UPDATE {self.schema}.base_user SET
         first_name = '{first_name}', last_name = '{last_name}', title = '{title}', 
         updated_at = CURRENT_TIMESTAMP, location = '{location}', business_unit = '{business_unit}',
         company = '{company}', timezone = '{timezone}'
         WHERE upper(email) = upper('{email}')
         RETURNING id
         """
        response = self._execute_sql_return_dict(sql)
        print(f"{email}'s profile has been updated. user_id: {response[0]['id']}")

    def _insert_new_user(self, first_name: str, last_name: str, title: str, email: str, location: str,
                         business_unit: str, company: str, timezone: str):
        sql = f"""
        INSERT INTO {self.schema}.base_user (
        first_name, last_name, title, created_at, updated_at, email, location, business_unit, company, is_enabled, 
        timezone, access_key
        ) VALUES (
        '{first_name}', '{last_name}', '{title}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '{email}', '{location}',
         '{business_unit}', '{company}', true, '{timezone}', '[]'
        )
        RETURNING id
        """
        response = self._execute_sql_return_dict(sql)
        print(f"new user {email} has been created. user_id: {response[0]['id']}")

    def _acquire_userid_by_email(self, email: str):
        sql = f"""
        SELECT id FROM base_user
        WHERE upper(email) = upper('{email}')
        """
        response = self._execute_sql_return_dict(sql)
        print(f"{email}'s user_id: {response[0]['id']}")
        return response[0]['id']

    def _delete_all_group_in_current_env_for_user(self, email: str, env_info_code: str):
        #     delete current env's all groups for the user
        user_id = self._acquire_userid_by_email(email)
        sql = f"""
        DELETE FROM {self.schema}.user_group
        WHERE user_id = {user_id} and group_id in (
        SELECT B.group_id FROM {self.schema}.group_environment B
        INNER JOIN (
            SELECT id FROM {self.schema}.environment
            WHERE code = '{env_info_code}'
        ) C
        ON B.environment_id = C.id
        )
        RETURNING 'DONE'
        """
        response = self._execute_sql_return_dict(sql)
        print(f"all groups under environment {env_info_code} have been removed for user {email}.")
        return response

    def _acquire_groupid_by_groupcode(self, group_code: str):
        sql = f"""
                SELECT id FROM base_group
                WHERE upper(code) = upper('{group_code}')
                """
        response = self._execute_sql_return_dict(sql)
        print(f"{group_code}'s group_id: {response[0]['id']}")
        return response[0]['id']

    def _add_group_for_user(self, group: str, email: str):
        user_id = self._acquire_userid_by_email(email)
        group_id = self._acquire_groupid_by_groupcode(group)
        sql = f"""
        INSERT INTO {self.schema}.user_group (user_id, group_id) VALUES (
        {user_id}, {group_id}
        )
        RETURNING 'DONE'
        """
        response = self._execute_sql_return_dict(sql)
        print(f"group {group} have been added for user {email}.")
        return response

    def _acquire_account_info(self, email: str):
        first_name = ''
        last_name = ''
        title = ''
        location = ''
        business_unit = ''
        company = ''
        server_ip = '10.179.8.88'
        username = 'svc_ldapqry@asia.jcdecaux.org'
        password = 'x8^WkI=c'
        # Define the search base
        # search_base = 'OU=Business Users,OU=Users,OU=CHINA,DC=asia,DC=jcdecaux,DC=org'
        search_base = 'DC=asia,DC=jcdecaux,DC=org'
        server = Server(server_ip, get_info=ALL)
        conn = Connection(server, user=username, password=password)
        if not conn.bind():
            print('Error in bind', conn.result)
            success_flag = False
            message = 'Failed, error connection.'
        else:
            # Perform the search operation
            conn.search(search_base, f'(mail={email})', search_scope=SUBTREE,
                        attributes=["mail", "sn", "givenName", "title", "l", "company", "department"])

            # Print the entries
            results = list(conn.entries)

            # Unbind the connection
            conn.unbind()
            if len(results) > 1:
                success_flag = False
                message = 'Failed, two or more accounts retrieved.'
            elif len(results) == 0:
                success_flag = False
                message = 'Failed, no account found.'
            else:
                success_flag = True
                message = 'Succeeded.'
                first_name = results[0]['givenName']
                last_name = results[0]['sn']
                title = results[0]['title']
                email = results[0]['mail']
                location = results[0]['l']
                business_unit = results[0]['department']
                company = results[0]['company']
                if first_name is None: first_name = 'unknown'
                if last_name is None: last_name = 'unknown'
                if title is None: title = 'unknown'
                if location is None: location = 'unknown'
                if business_unit is None: business_unit = 'unknown'
                if company is None: company = 'unknown'

        return success_flag, message, first_name, last_name, title, email, location, business_unit, company

    def _check_whether_current_operator_can_grant_the_group(self, operator, group):
        if self._check_whether_user_has_group(group='datacorp-full-admin', email=operator):
            return True
        elif self._check_whether_user_has_group(group='mam-shanghai-admin', email=operator) and \
                group in ('mam-shanghai-admin', 'mam-shanghai-manager', 'mam-shanghai-reader'):
            return True
        elif self._check_whether_user_has_group(group='mam-beijing-admin', email=operator) and \
                group in ('mam-beijing-admin', 'mam-beijing-manager', 'mam-beijing-reader'):
            return True
        elif self._check_whether_user_has_group(group='mam-chongqing-admin', email=operator) and \
                group in ('mam-chongqing-admin', 'mam-chongqing-manager', 'mam-chongqing-reader'):
            return True
        elif self._check_whether_user_has_group(group='mam-nanjing-admin', email=operator) and \
                group in ('mam-nanjing-admin', 'mam-nanjing-manager', 'mam-nanjing-reader'):
            return True
        elif self._check_whether_user_has_group(group='mam-tianjin-admin', email=operator) and \
                group in ('mam-tianjin-admin', 'mam-tianjin-manager', 'mam-tianjin-reader'):
            return True
        elif self._check_whether_user_has_group(group='mam-suzhou-admin', email=operator) and \
                group in ('mam-suzhou-admin', 'mam-suzhou-manager', 'mam-suzhou-reader'):
            return True
        else:
            return False

    def _check_whether_user_has_group(self, group: str, email: str):
        user_id = self._acquire_userid_by_email(email)
        group_id = self._acquire_groupid_by_groupcode(group)
        sql = f"""
                SELECT user_id, group_id FROM {self.schema}.user_group
                WHERE user_id = {user_id} and group_id = {group_id}
                """
        response = self._execute_sql_return_dict(sql)
        if len(response) == 0:
            print(f"user {email} doesn't have group {group} permission.")
            return False
        else:
            print(f"user {email} has group {group} permission.")
            return True

    def admin_batch_add_group_for_users(self, email_group_list: list):
        """
        :param email_group_list:[,{"email": "zewen.liang@jcdecaux.com", "group": "mam-shanghai-reader"}, {...} ...]
        :return:
        """
        response = []
        for item in email_group_list:
            # 1. check whether current operator can operate target group
            if not self._check_whether_current_operator_can_grant_the_group(self.operator, item['group']):
                response.append({'email': item['email'],
                                 'status': f"Failed, current operator has no permission to operate group {item['group']}'."})
            else:
                # 2. check whether target user existence.
                if not self._check_user_existence(email=item['email']):
                    # 2.1 if not existed, create new user.
                    success_flag, message, first_name, last_name, title, email, location, business_unit, company = self._acquire_account_info(
                        item['email'])
                    if success_flag:
                        timezone = 'Asia/Shanghai'
                        self._insert_new_user(first_name, last_name, title, email, location, business_unit, company,
                                              timezone)
                    else:
                        response.append({'email': item['email'], 'status': f"Failed, reason: {message}"})
                        return response
                else:
                    # 2.2 if target user existed, update his/her information
                    success_flag, message, first_name, last_name, title, email, location, business_unit, company = self._acquire_account_info(
                        item['email'])
                    if success_flag:
                        timezone = 'Asia/Shanghai'
                        self._update_user_information_and_enable(first_name, last_name, title, location, business_unit,
                                                                 company, timezone, email)
                    else:
                        response.append({'email': item['email'], 'status': f"Failed, reason: {message}"})
                # 3. check whether target user has target group already.
                if self._check_whether_user_has_group(group=item['group'], email=item['email']):
                    response.append({'email': item['email'],
                                     'status': f"Failed, user '{item['email']}' has group {item['group']} already."})
                else:
                    self._add_group_for_user(group=item['group'], email=item['email'])
                    response.append({'email': item['email'], 'status': f"Succeeded."})
        return response

    def admin_batch_remove_all_groups_for_users(self, email_list: list):
        """
        :param email_list:  [{"email": "zewen.liang@jcdecaux.com"}, {...}, ...]
        :return:
        """
        response = []
        for item in email_list:
            if self._check_user_existence(item['email']):
                self._delete_all_group_in_current_env_for_user(email=item['email'], env_info_code=self.env_info_code)
                response.append({'email': item['email'], 'status': 'Succeeded.'})
            else:
                response.append({'email': item['email'], 'status': f"Failed, user {item['email']} doesn't exist."})
        return response

    def admin_batch_refresh_info_for_users(self, email_list: list):
        """
        :param email_list: [{"email": "zewen.liang@jcdecaux.com"}, {...}, ...]
        :return:
        """
        response = []
        for item in email_list:
            if not self._check_user_existence(item['email']):
                response.append({'email': item['email'], 'status': f"user item['email'] does not exist."})
            else:
                success_flag, message, first_name, last_name, title, email, location, business_unit, company = self._acquire_account_info(
                    item['email'])
                self._update_user_information(first_name=first_name, last_name=last_name,
                                              title=title, location=location, business_unit=business_unit,
                                              company=company, timezone='Asia/Shanghai',
                                              email=item['email'])
                response.append({'email': item['email'], 'status': f"user {item['email']} has been updated."})
        return response

    def refresh_all_users_info(self):
        sql = f"""
                SELECT email FROM {self.schema}.base_user
                WHERE is_enabled = true
                """
        response = self._execute_sql_return_dict(sql)
        for item in response:
            success_flag, message, first_name, last_name, title, email, location, business_unit, company = self._acquire_account_info(
                item['email'])
            if success_flag:
                self._update_user_information(first_name=first_name, last_name=last_name,
                                              title=title, location=location, business_unit=business_unit,
                                              company=company, timezone='Asia/Shanghai',
                                              email=item['email'])
            else:
                print('skip user:', item['email'], 'as unexists.')

    def check_former_colleagues(self):
        users = self.retrieve_all_users_in_current_env()
        invalid_users = []
        for user in users:
            response = (self._acquire_account_info(user['email']))
            if response[0] == False:
                invalid_users.append(dict(user))
        return invalid_users

    def getUsersList(self):
        conn = psycopg2.connect(**self.connection_parameters)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        report_info_sql =f"""
                       SELECT concat(b.last_name,' ',b.first_name)name,b.title position,b.email,b.business_unit department, c.code permission_code
                       FROM {self.schema}.user_group a
                       inner join (SELECT last_name, first_name, title,email,business_unit, id from {self.schema}.base_user) b
                       ON a.user_id = b.id
                       INNER JOIN (SELECT code, id From  {self.schema}.base_group) c
                       ON a.group_id = c.id
                       WHERE code Like '"""+str(self.env_info_code)+"""%'
                       """
        cur.execute(report_info_sql)
        report_info_data = cur.fetchall()
        df = pd.DataFrame(report_info_data)
        print(df)
        if df.empty != True :

            data=df.groupby(['name', 'position','email', 'department'], dropna=False)['permission_code'].apply(lambda x:x.str.cat(sep=';')).reset_index()
            data['permission_code'] = data['permission_code'].apply(lambda x: x.split(';'))
            db_user = data.to_json(orient='records')
            db_user = json.loads(db_user)
        else:
            db_user = []

        return db_user

    def insertOnerUser(self,email,role,user):
        response = []

        # 1. check whether current operator can operate target group
        if not self._check_whether_current_operator_can_grant_the_group(self.operator, role):
                response.append({'email': email,
                                 'status': f"Failed, current operator has no permission to operate group {role}'."})
                return response
        else:
            # 2. check whether target user existence.
            if not self._check_user_existence(email=email):
                # 2.1 if not existed, create new user.
                success_flag, message, first_name, last_name, title, email, location, business_unit, company = self._acquire_account_info(
                       email)
                if success_flag:
                    timezone = 'Asia/Shanghai'

                    self._insert_new_user(first_name, last_name, title, email, location, business_unit, company,
                                              timezone)
                    self._add_group_for_user(group=role, email=email)
                else:
                    response.append({'email': email, 'status': f"Failed, reason: {message}"})
                    return response
            else:
                if not self._check_user_existence(email):
                    response.append({'email': email, 'status': f"用户item['email']不存在！"})
                    return response
                else:
                    success_flag, message, first_name, last_name, title, email, location, business_unit, company = self._acquire_account_info(
                        email)
                    self._update_user_information(first_name=first_name, last_name=last_name,
                                                  title=title, location=location, business_unit=business_unit,
                                                  company=company, timezone='Asia/Shanghai',
                                                  email=email)
                    # 2.2 check user has role in this environement
                success_flag = self._check_user_role_environment_existence(
                        email,self.env_info_code)
                print(success_flag)
                if success_flag:
                    # True:  exist
                    response.append({'email': email, 'status': f"Failed, reason: {email} 账号信息已存在!"})

                else:
                    # False: not exist
                    success_flag = self._add_group_for_user(role, email)

                  
        return response
    def uploadUserBatch(self, df_user):
        for item in df_user:
            result = self.insertOnerUser(item['email'],item['role'],'')
            print(result)


        return ''

    def updateUserRole(self,email,role):
        response = [{'status':200}]
        # 1. check whether current operator can operate target group
        if not self._check_whether_current_operator_can_grant_the_group(self.operator, role):
            response.append({'email': email,
                             'status': f"Failed, current operator has no permission to operate group {role}'."})
            return response
        else:

            if not self._check_user_existence(email):
                response.append({'email': email, 'status': f"用户item['email']不存在！"})
                return response
            else:
                success_flag, message, first_name, last_name, title, email, location, business_unit, company = self._acquire_account_info(
                    email)
                self._update_user_information(first_name=first_name, last_name=last_name,
                                              title=title, location=location, business_unit=business_unit,
                                              company=company, timezone='Asia/Shanghai',
                                              email=email)
                response = [{'status': 200,'first_name':str(first_name), 'last_name':str(last_name),'title':str(title), 'location': str(location), 'business_unit':str(business_unit),'company':str(company)}]
            user_id = self._acquire_userid_by_email(email)
            group_id = self._acquire_groupid_by_groupcode(role)
            sql = f"""
                   DELETE FROM {self.schema}.user_group
                    WHERE user_id = {user_id}
                      RETURNING 'DONE'
                  """
            response_2 = self._execute_sql_return_dict(sql)
            response_2 = self._add_group_for_user(role, email)
            return response

    def deleteUserRole(self, email ):
        response = []
        # 1. check whether current operator can operate target group
        if not self._check_whether_current_operator_can_grant_the_group(self.operator, self.env_info_code +'-admin'):
            response.append({'email': email,
                             'status': f"Failed, current operator has no permission to operate group {role}'."})
            return response
        else:
            user_id = self._acquire_userid_by_email(email)

            sql = f"""
                          DELETE FROM {self.schema}.user_group
                           WHERE user_id = {user_id}
                             RETURNING 'DONE'
                         """
            response_2 = self._execute_sql_return_dict(sql)

            return response





    def _check_user_role_environment_existence(self, email: str, environment:str):
        sql = f"""
          SELECT is_enabled,a.id FROM {self.schema}.base_user a
          inner join (SELECT user_id, group_id from {self.schema}.user_group) b
                       ON a.id = b.user_id
           inner join (SELECT code, id from  {self.schema}.base_group) c
           ON c.id = b.group_id
          WHERE upper(email) = upper('{email}') and c.code Like'"""+str(environment)+"""%'
          """
        response = self._execute_sql_return_dict(sql)
        # print(f"{len(response)} row(s) retrieved for {email}.")
        print(response)
        if len(response) == 0:
            return False
        else:
            return True




# if __name__ == '__main__':
#     # 案例：通过excel批量授权（此excel只有一列，理应有两列：email, group）
#     # staging
#     connection_parameters = {
#         "host": "10.4.47.180",
#         "port": "5432",
#         "database": "data_portal",
#         "user": "postgres",
#         "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
#     }
#     # dev
#     # connection_parameters = {
#     #     "host": "10.4.46.229",
#     #     "port": "5432",
#     #     "database": "data_portal",
#     #     "user": "postgres",
#     #     "password": "aa4f8ef88f3b1e65aee010a79e69d71b"
#     # }
#     # production
#     # connection_parameters = {
#     #     "host": "10.4.102.102",
#     #     "port": "5432",
#     #     "database": "data_portal",
#     #     "user": "data_portal",
#     #     "password": "a7W3htXkhMDJD2Sn"
#     # }
#
#     mpgbb = MamPermissionGrantByBu(operator='zewen.liang@jcdecaux.com', env_info_code='mam-beijing',
#                                    connection_parameters=connection_parameters)
#     response = mpgbb.admin_batch_add_group_for_users(
#         [{'email': 'wayne.qi@jcdecaux.com', 'group': 'mam-tianjin-reader'},
#          {'email': 'crystal.zhang@jcdecaux.com', 'group': 'mam-tianjin-reader'},
#          {'email': 'william.wen@jcdecaux.com', 'group': 'mam-tianjin-reader'},
#          {'email': 'lily.fu@jcdecaux.com', 'group': 'mam-tianjin-reader'},
#          {'email': 'cynthia.zhang@jcdecaux.com', 'group': 'mam-tianjin-reader'},
#          {'email': 'gary.song@jcdecaux.com', 'group': 'mam-tianjin-reader'},
#          {'email': 'wendy.zhang@jcdecaux.com', 'group': 'mam-tianjin-reader'},
#          {'email': 'raihan.zhou@jcdecaux.com', 'group': 'mam-tianjin-reader'},
#          ])
#
#     # mpgbb = MamPermissionGrantByBu(operator='zewen.liang@jcdecaux.com', env_info_code='mam-shanghai')
#     # potential_left_users = mpgbb.check_former_colleagues()
#     # print(potential_left_users)
#     # mpgbb.refresh_all_users_info()
#     # exit()
#
#     # 所有功能步骤
#     # 实例化对象，指定操作人及操作环境
#     mpgbb = MamPermissionGrantByBu(operator='zewen.liang@jcdecaux.com', env_info_code='mam-shanghai')
#     # Case 0: 根据IT域信息，刷新所有用户信息。
#     # Case 1: 批量刷新一批用户的信息。不存在的用户将会被忽略，disable的用户不会被enable。
#     response = mpgbb.admin_batch_refresh_info_for_users([{'email': 'zewen.liang@jcdecaux.com'}])
#     # Case 2: 批量移除一批用户在当前环境下的所有组。不存在的用户将会被忽略，disable的用户的组也会被移除，但该用户不会被enable。
#     response = mpgbb.admin_batch_remove_all_groups_for_users([{'email': 'zewen.liang@jcdecaux.com'}])
#     # Case 3: 批量为一批用户添加组。不存在的用户将会被自动创建，disable的用户会自动enable并保留disable前的组。
#     response = mpgbb.admin_batch_add_group_for_users(
#         [{'email': 'zewen.liang@jcdecaux.com', 'group': 'mam-shanghai-reader'}])
#     print(response)
