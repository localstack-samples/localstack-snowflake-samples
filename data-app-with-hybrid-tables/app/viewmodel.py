from flask import url_for
from typing import List

from model import Label, Task, User


class HomeViewModel:
    def __init__(self, 
                users:List[User],
                user_id:int,
                items_overdue_count:int, 
                items_today_count:int, 
                items_upcoming_count:int,
                items_overdue:List[Task],
                items_today:List[Task],
                items_upcoming:List[Task],
                label_stats:List[Label],
                label:str=None
                ) -> None:
        
        self.items_today_count:int = items_today_count
        self.items_overdue_count:int = items_overdue_count
        self.items_upcoming_count:int = items_upcoming_count

        self.items_today_percent:int = items_today_count / max(1, items_today_count + items_overdue_count + items_upcoming_count)
        self.items_overdue_percent:int = items_overdue_count / max(1, items_today_count + items_overdue_count + items_upcoming_count)
        self.items_upcoming_percent:int = items_upcoming_count / max(1, items_today_count + items_overdue_count + items_upcoming_count)
        
        self.items_today:List[Task] = items_today
        self.items_overdue:List[Task] = items_overdue
        self.items_upcoming:List[Task] = items_upcoming
        
        self.labels = label_stats
        self.top_labels = [label_stats[_] for _ in label_stats]
        self.top_labels.sort(key=lambda l:l.index)
        self.top_labels = self.top_labels[:6]

        self.users = users
        for user in users:
            user.active = user.id == user_id
            if user.active:
                self.current_user = user

        self.label = label

class ItemViewModel:
    def __init__(self, 
                users:List[User],
                user_id:int,
                label_stats:List[Label],
                task:Task
                ) -> None:

        self.labels = label_stats
        self.users = users         
        for user in users:
            user.active = user.id == user_id
            self.current_user = user
    
        self.task = task

        if task.id == -1:
            self.form_action = url_for('update', id=None)
        else:
            self.form_action = url_for('update', id=task.id)
