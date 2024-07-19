from ldap3 import Server, Connection, ALL
import traceback
import jwt
from django.conf import settings

from mam_api_app.models.user import User
from rest_framework_simplejwt.tokens import SlidingToken

sso_server = settings.SSO_SERVER


class LoginService:

    @staticmethod
    def auto_login(token):
        try:
            secret = 'Qa35FRwTTZ36jy5S3zVHLpyFRcFeRc2g'
            info = jwt.decode(token, secret, 'HS512')
            email = info['sub']
            user_authority = {
                'is_register_user': False,
                'view_beijing': False,
                'view_shanghai': False,
                'view_chengdu': False,
                'view_chongqing': False,
                'sliding_token': 'slidingToken',
            }
            user = User.objects.get(email=email)
            user_authority['is_register_user'] = True
            user_authority['view_beijing'] = user.view_beijing
            user_authority['view_shanghai'] = user.view_shanghai
            user_authority['view_chengdu'] = user.view_chengdu
            user_authority['view_chongqing'] = user.view_chongqing
            return True, user_authority, email
        except:
            return False, {}, ''

    @staticmethod
    def ldap_login(postdata):
        # server = Server('sha001ns001.asia.jcdecaux.org', get_info=ALL)
        server = Server(sso_server, get_info=ALL)

        email = postdata['email']
        password = postdata['password']
        login_email = "adjcdasia\\" + email.split('@')[0]
        conn = Connection(server, login_email, password)
        flag = conn.bind()
        return flag

    @staticmethod
    def login_check(postdata):
        # server = Server('sha001ns001.asia.jcdecaux.org', get_info=ALL)
        server = Server(sso_server, get_info=ALL)

        email = postdata['email']
        password = postdata['password']

        user_authority = {
            'is_register_user': False,
            'view_beijing': False,
            'view_shanghai': False,
            'view_chengdu': False,
            'view_chongqing': False,
            'sliding_token': 'slidingToken',
        }

        login_email = "adjcdasia\\" + email.split('@')[0]
        conn = Connection(server, login_email, password)
        flag = conn.bind()
        if flag:
            try:
                user = User.objects.get(email=email)
                user_authority['is_register_user'] = True
                user_authority['view_beijing'] = user.view_beijing
                user_authority['view_shanghai'] = user.view_shanghai
                user_authority['view_chengdu'] = user.view_chengdu
                user_authority['view_chongqing'] = user.view_chongqing
            except:
                traceback.print_exc()

            # token = SlidingToken.for_user(postdata)
            # user_authority['sliding_token'] = str(token)

        return user_authority
