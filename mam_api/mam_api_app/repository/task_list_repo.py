from django.db import connection
import datetime

from mam_api_app.utils.sql_util import SqlUtil
from mam_api_app.models.task_list import TaskList


class TaskListRepo:

    @staticmethod
    def approve_task_list(tasks, email):
        tasks = TaskList.objects.filter(id__in=tasks)
        # Update the status of the queryset to 'approve'
        tasks.update(approval_status='approved', approval_time=datetime.datetime.now(), approver=email)
