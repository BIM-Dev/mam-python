from django.db import models


class TaskList(models.Model):
    id = models.AutoField(primary_key=True)
    status = models.CharField(max_length=20)
    airport = models.CharField(max_length=20)
    terminal = models.CharField(max_length=20)
    direction = models.CharField(max_length=20)
    flight_type = models.CharField(max_length=20)
    config = models.CharField(max_length=1000)
    trainset_start_yearmonth = models.IntegerField()
    trainset_end_yearmonth = models.IntegerField()
    created_time = models.DateTimeField()
    creator = models.CharField(max_length=50)
    start_time = models.DateTimeField()
    end_time = models.DateTimeField()
    approval_status = models.CharField(max_length=20)
    approval_time = models.DateTimeField()
    approver = models.CharField(max_length=50)

    def __str__(self):
        return self.id

    class Meta:
        db_table = "task_list"