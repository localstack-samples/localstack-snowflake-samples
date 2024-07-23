from typing import List
from snowflake.snowpark import Row
import datetime

class User():
    def __init__(self, id = 0, login:str = None, name:str = None) -> None:
        self.id:int = id
        self.login:str = login
        self.name:str = name
        self.active:bool = False

    @staticmethod
    def loadRow(row:Row):
        obj = User(
            row["ID"], 
            row["LOGIN"],
            row["NAME"])
        return obj

class Status():
    def __init__(self, index = 0, title = None, count = 0, percent = 0.0) -> None:
        self.index = index
        self.title:str = title
        self.count:int = count
        self.percent:float = percent

    @staticmethod
    def loadRow(row:Row):
        obj = Status(
            row["INDEX"] if "INDEX" in row else 1, 
            row["STATUS"],
            row["COUNT"],
            row["PERCENT"] if "PERCENT" in row else 0.0)
        return obj
    
class Label():
    def __init__(self, index = 0, title = None, count = 0, percent = 0.0) -> None:
        self.index = index
        self.title:str = title
        self.count:int = count
        self.percent:float = percent

    @staticmethod
    def loadRow(row:Row):
        obj = Label(
            row["INDEX"], 
            row["LABEL"],
            row["COUNT"],
            row["PERCENT"])
        return obj

class Task():
    
    def __init__(self, id:int, title:str, due_date:datetime, days_due:int, complete:bool, labels:List[Label], status:str=None) -> None:
        self.id:int = id
        self.title:str = title
        self.due_date:datetime = due_date
        self.due_in_days = days_due
        self.complete:bool = complete
        self.labels:List[Label] = labels
        self.status = status

    def get_label_titles(self) -> str:
        return ', '.join([_.title.title()  for _ in self.labels])

    def get_label_list(self) -> str:
        return ','.join([_.title  for _ in self.labels])
    
    def get_label_array(self) -> List[str]:
        return [_.title  for _ in self.labels]
    
    @staticmethod
    def loadRow(row:Row, labels:List[Label]=[]):

        if "LABELS" in row:
            label_strs = row["LABELS"].split(',')
            labels_resolved = [_ for _ in labels if _.title in label_strs]
        else:
            labels_resolved = []
        obj = Task(
            row['ID'], 
            row["ITEM"],
            row["DATE_DUE"],
            row["DAYS_DUE"] if "DAYS_DUE" in row else 0,
            row["COMPLETE"] if "COMPLETE" in row else None,
            labels_resolved,
            row["STATUS"] if "STATUS" in row else None
            )
        
        return obj