from django.db import models

class User(models.Model):
    email = models.EmailField(max_length=100)
    view_beijing = models.BooleanField()
    view_shanghai = models.BooleanField()
    view_chengdu = models.BooleanField()
    view_chongqing = models.BooleanField()

    class Meta:
        db_table = "user"
