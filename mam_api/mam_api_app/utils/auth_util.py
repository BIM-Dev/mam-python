from mam_api_app.models.user import User

class AuthUtil:

    @staticmethod
    def check_authority(obj):
        request = obj.request
        airport = request.data['airport']
        try:
            user = User.objects.get(email=request.data['email'])
            if airport in ['PEK', 'PKX']:
                return user.view_beijing
            elif airport in ['PVG', 'SHA']:
                return user.view_shanghai
            elif airport in ['CTU', 'TFU']:
                return user.view_chengdu
            elif airport in ['CKG']:
                return user.view_chongqing
            else:
                return False
        except:
            return False
