from django.urls import path

from mam_api_app.views import Login
from mam_api_app.views import LdapLogin
from mam_api_app.views import AutoLogin
from mam_api_app.views import SendMail, generateIR, checkMasterData, updateMasterDate, uniqueCheckMamFile, globalCheckMamFile, sendMamLFile, insertOneUser, getUser, updateUserRole,deleteUserRole,uploadUserBatch


urlpatterns = [
    path('auto_login/', AutoLogin.as_view(), name='auto_login'),
    path('ldap_login/', LdapLogin.as_view(), name='ldap_login'),
    path('log_in/', Login.as_view(), name='login'),
    path('send_mail/', SendMail.as_view(), name='send_mail'),
    path('generate_ir/',generateIR.as_view(), name='generate_ir'),
    path('checkMasterData/', checkMasterData.as_view(), name = 'check_master_data'),
    path('updateMasterData/', updateMasterDate.as_view(), name='update_master_data'),
    path('uniqueCheckMamLfile/', uniqueCheckMamFile.as_view(), name = 'unique_check_mam_file'),
    path('globalCheckMamLfile/', globalCheckMamFile.as_view(), name = 'global_check_mam_file'),
    path('sendMamLFile/', sendMamLFile.as_view(), name = 'send_MamLFile'),
    path('insertOneUser/',insertOneUser.as_view(), name = 'insert_oner_user'),
    path('getUserList/',getUser.as_view(), name="get_user_list"),
    path('updateUserRole/', updateUserRole.as_view(), name='updateUserRole'),
    path('deleteUserRole/',deleteUserRole.as_view(), name="deleteUserRole"),
    path('uploadUserBatch/',uploadUserBatch.as_view(), name="upload_user_batch")
]
