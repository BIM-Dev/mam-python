from .base import *
# Database
# https://docs.djangoproject.com/en/2.2/ref/settings/#databases

DBHOST = "localhost"

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'traffic_forecaster',
        'USER': 'postgres',
        'PASSWORD': 'aa4f8ef88f3b1e65aee010a79e69d71b',
        'HOST': 'localhost',
        'PORT': '5432',
    }
}

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['10.4.47.243','bam-staging.jcdecauxchina.com.cn']

CORS_ORIGIN_WHITELIST = (
    'https://bam-staging.jcdecauxchina.com.cn',
    'http://10.4.46.107',
    'https://10.4.46.107',
)
