from django.db import models


class ForecastResult(models.Model):
    date = models.DateTimeField(max_length=64)
    pax = models.FloatField(max_length=32)
    airport = models.CharField(max_length=32)
    terminal = models.CharField(max_length=32)
    flight_type = models.CharField(max_length=32)
    direction = models.CharField(max_length=32)
    forecast_type = models.CharField(max_length=32)

    def __str__(self):
        return self.pax

    class Meta:
        managed = False
