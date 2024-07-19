"""
WSGI config for mam_api project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.2/howto/deployment/wsgi/
"""

import os
import socket

from django.core.wsgi import get_wsgi_application

socket_name = socket.gethostname()
if "220" in socket_name:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mam_api.settings.staging')
elif "243" in socket_name:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mam_api.settings.prod')
else:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mam_api.settings.dev')


application = get_wsgi_application()
