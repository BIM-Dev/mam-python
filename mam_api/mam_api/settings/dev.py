from .base import *
# Database
# https://docs.djangoproject.com/en/2.2/ref/settings/#databases

# DBHOST = "10.4.46.229"
DBHOST = "localhost"
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'generic_am_mam',
        'USER': 'postgres',
        'PASSWORD': 'aa4f8ef88f3b1e65aee010a79e69d71b',
        'HOST': '10.4.46.229',
        'PORT': '5432',
    }
}

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['127.0.0.1','localhost']

CORS_ORIGIN_WHITELIST = (
    'http://127.0.0.1:8080',
    'http://localhost:8080',
    'http://127.0.0.1:8081',
    'http://localhost:8081',
    'http://localhost:4200'
)
