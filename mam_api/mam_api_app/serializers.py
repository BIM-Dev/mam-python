from rest_framework import serializers


class SerializerLogin(serializers.Serializer):
    is_register_user = serializers.BooleanField()
    view_beijing = serializers.BooleanField()
    view_shanghai = serializers.BooleanField()
    view_chengdu = serializers.BooleanField()
    view_chongqing = serializers.BooleanField()
    sliding_token = serializers.CharField()


class SerializerLastForecast(serializers.Serializer):
    date = serializers.CharField()
    terminal = serializers.CharField()
    flight_type = serializers.CharField()
    current_forecast = serializers.IntegerField()
    last_forecast = serializers.IntegerField()
    last_forecast_fix = serializers.IntegerField()
    actual_data = serializers.IntegerField()


class SerializerAvailableYearMonth(serializers.Serializer):
    yearmonth = serializers.IntegerField()

