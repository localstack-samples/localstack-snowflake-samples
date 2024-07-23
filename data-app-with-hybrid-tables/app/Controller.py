from model import Label, Task, User, Status
from typing import List, Dict
from snowflake.snowpark import Session
import datetime
from functools import wraps
import time



class Controller:

    def measure(func):
        @wraps(func)
        def measure_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            #print(f'controller: {func.__name__} latency {total_time:.4f} seconds')
            return result
        return measure_wrapper

    def __init__(self, session:Session) -> None:
        self.session = session

    def loginUser(self, user_name:str, password:str) -> User:
        sql = "SELECT id, login, name FROM HYBRID_TASK_APP_DB.DATA.USER WHERE LOWER(login) = ? AND HPASSWORD = ?;"
        data_items = self.session.sql(sql, params=[user_name.lower(), password]).collect()
        if len(data_items) == 1:
            user = User.loadRow(data_items[0])
            return user

    @measure
    def load_label_stats(self, user_id) -> Dict[str, Label]:

        data_items = self.session.sql("SELECT index, count, label, percent FROM HYBRID_TASK_APP_DB.DATA.LABEL_DETAIL WHERE owner_id = ?;", params=[user_id]).collect()
        label_stats = {_.LABEL:Label.loadRow(_) for _ in data_items}
        return label_stats

    @measure
    def load_users(self) -> List[User]:
        data_items = self.session.sql("SELECT id, login, name FROM HYBRID_TASK_APP_DB.DATA.USER;").collect()
        users = [User.loadRow(_) for _ in data_items]
        return users
    
    @measure
    def load_items(self, user_id:int, label:str=None) -> List[Task]:

        if label:
            sql = """
                    SELECT id, item, date_due, days_due, status
                    FROM HYBRID_TASK_APP_DB.DATA.TASK_DETAIL t
                    INNER JOIN HYBRID_TASK_APP_DB.DATA.TASK_LABEL l ON t.id = l.task_id
                    WHERE owner_id = ?
                    AND l.label = LOWER(?)
                    AND t.complete = FALSE;
                    """
            data_items = self.session.sql(sql, [user_id, label]).collect()
        else:
            sql = """
                    SELECT id, item, date_due, days_due, status
                    FROM HYBRID_TASK_APP_DB.DATA.TASK_DETAIL t
                    WHERE owner_id = ?
                    AND t.complete = FALSE;
                    """
            data_items = self.session.sql(sql, [user_id]).collect()

        items_all = [Task.loadRow(_) for _ in data_items]

        return items_all
    

    @measure
    def load_items_overview(self, user_id:int, label:str=None) -> List[Task]:

        if label:
            sql = """
                    SELECT t.id, t.item, LISTAGG(l.label,',') as labels, t.date_due, t.days_due, t.status, t.owner_id, t.index
                    FROM (
                        SELECT t.id, t.item , t.date_due, t.days_due, t.status, owner_id
                        , row_number() over(PARTITION BY t.status, t.owner_id ORDER BY t.days_due ASC) as index
                        FROM HYBRID_TASK_APP_DB.DATA.TASK_DETAIL t
                        INNER JOIN HYBRID_TASK_APP_DB.DATA.TASK_LABEL l ON t.id = l.task_id
                        WHERE OWNER_ID = ?
                        AND l.label = ?
                        AND t.complete = FALSE
                    ) t
                    INNER JOIN HYBRID_TASK_APP_DB.DATA.TASK_LABEL l ON t.id = l.task_id
                    WHERE index <= 5 
                    GROUP BY ALL
                    ORDER BY index ASC;
                    """
            data_items = self.session.sql(sql, [user_id, label.lower()]).collect()
        else:
            sql = """
                    SELECT t.id, t.item, LISTAGG(l.label,',') as labels, t.date_due, t.days_due, t.status, t.owner_id, t.index
                    FROM (
                        SELECT t.id, t.item , t.date_due, t.days_due, t.status, owner_id
                        , row_number() over(PARTITION BY t.status, t.owner_id ORDER BY t.days_due ASC) as index
                        FROM HYBRID_TASK_APP_DB.DATA.TASK_DETAIL t
                        WHERE OWNER_ID = 1
                        AND t.complete = FALSE
                    ) t
                    LEFT OUTER JOIN TASK_LABEL l ON t.id = l.task_id
                    WHERE index <= 5 
                    GROUP BY ALL
                    ORDER BY index ASC;
                    """
            data_items = self.session.sql(sql, [user_id]).collect()

        items_all = [Task.loadRow(_) for _ in data_items]

        return items_all

    @measure
    def load_items_stats(self, user_id:int, label:str=None) -> Dict[str, Status]:

        if label:
            sql = "SELECT status, count FROM LABEL_STATUS_DETAIL WHERE owner_id = ? AND LABEL = ?;"
            data_items = self.session.sql(sql, [user_id, label]).collect()
        else:
            sql = "SELECT status, count  FROM STATUS_DETAIL WHERE owner_id = ?;"
            data_items = self.session.sql(sql, [user_id]).collect()
        items_all = {_.STATUS:Status.loadRow(_) for _ in data_items}
        all_stats = {**{_:Status(0, _, 0) for _ in ['overdue', 'today', 'upcoming']}, **items_all}
        return all_stats

    @staticmethod
    def get_tasks_by_status(tasks:List[Task]) -> tuple[List[Task], List[Task], List[Task]]:
        data_today=[_ for _ in tasks if _.status=='today']
        data_overdue=[_ for _ in tasks if _.status=='overdue']
        data_upcoming=[_ for _ in tasks if _.status=='upcoming']
        return (data_overdue, data_today, data_upcoming)

    @staticmethod
    def get_tasks_for_overview(tasks:tuple[List[Task], List[Task], List[Task]]) -> List[int]:
        return [_[:5] for _ in tasks]
    
    @staticmethod
    def get_task_ids_for_overview(tasks:tuple[List[Task], List[Task], List[Task]]) -> List[int]:
        task_ids = []
        for tasks in tasks:
            task_ids = task_ids + [_.id for _ in tasks]
        return task_ids

    @measure
    def load_task_labels(self, task_ids:List[int]) -> Dict[str, Label]:

        task_ids_list = ','.join(['?' for _ in task_ids])
        if len(task_ids) == 0:
            return {}
        sql = f"""
                SELECT task_id, LISTAGG(label,',') as labels 
                FROM HYBRID_TASK_APP_DB.DATA.TASK_LABEL 
                WHERE task_id IN ({task_ids_list}) 
                GROUP BY task_id;
                """
        data_items = self.session.sql(sql, params=task_ids).collect()
        labels = {_.TASK_ID:', '.join(_.LABELS.split(',')) for _ in data_items}
        return labels

    @staticmethod
    def map_labels(task:Task, all_task_labels:Dict[int, str], all_labels:Dict[str, Label]):

        labels = []
        if task.id in all_task_labels:
            task_labels = all_task_labels[task.id]
        else:
            task_labels = ''
        for task_label in [_.strip().lower() for _ in task_labels.split(',')]:            
            if task_label in all_labels:
                label = all_labels[task_label]
                labels.append(label)
            else:
                label = Label(100, task_label, 1, 0)
                labels.append(label)

        labels.sort(key=lambda l: l.index)
        task.labels = labels
        return task
    
    @measure
    def load_task(self, user_id:int, task_id:int, all_labels:List[Label]) -> Task:
        sql = "SELECT id, owner_id, item, date_due FROM HYBRID_TASK_APP_DB.DATA.TASK WHERE id = ?;"
        data_items = self.session.sql(sql, params=[task_id]).collect()
        if len(data_items) < 1:
            return None

        task_data_item = data_items[0]
        owner_id = task_data_item.OWNER_ID
        if user_id != owner_id:
            return None
        
        task = Task.loadRow(task_data_item)

        sql = "SELECT task_id, LISTAGG(label,',') as labels FROM HYBRID_TASK_APP_DB.DATA.TASK_LABEL WHERE task_id = ? GROUP BY task_id;"
        data_items = self.session.sql(sql,params=[task.id]).collect()
        if len(data_items) == 1:
            task_labels:str = {int(task_id): data_items[0].LABELS}
        else:
            task_labels:str = {int(task_id):''}
        task = Controller.map_labels(task, task_labels, all_labels)
        return task
    
    @measure
    def create_task(self, user_id:int, item:str, date_due:datetime.datetime, updated_labels:List[str]) -> None:
        if len(updated_labels) == 0:
            # No need for a transaction here, single statement and table execution
            self.session.sql(f"INSERT INTO HYBRID_TASK_APP_DB.DATA.TASK (ITEM, DATE_DUE, OWNER_ID) VALUES (?, ?, ?);", params=[item, ("TIMESTAMP_NTZ", date_due), user_id]).collect()
            return -1
        else:
            try:
                self.session.sql(f"BEGIN TRANSACTION;").collect()
                task_id = self.session.sql(f"SELECT HYBRID_TASK_APP_DB.DATA.TASK_ID_SEQ.nextval id").collect()[0].ID
                self.session.sql(f"INSERT INTO HYBRID_TASK_APP_DB.DATA.TASK (ID, ITEM, DATE_DUE, OWNER_ID) VALUES (?, ?, ?, ?);", params=[task_id, item, ("TIMESTAMP_NTZ", date_due), user_id]).collect()
                for label in updated_labels:
                    self.session.sql(f"INSERT INTO HYBRID_TASK_APP_DB.DATA.TASK_LABEL (TASK_ID, LABEL) VALUES (?, LOWER(?));", params=[task_id, label]).collect()            
                self.session.sql(f"COMMIT;").collect()
                return task_id
            except Exception as e:
                print(e)
                self.session.sql(f"ROLLBACK;").collect()

    @measure
    def update_task(self, task_id:int, item:str, date_due:datetime.datetime, updated_labels:List[str], existing_labels:List[str]) -> None:        
        try:
            self.session.sql(f"BEGIN TRANSACTION;").collect()        
            items = self.session.sql(f"UPDATE HYBRID_TASK_APP_DB.DATA.TASK SET item=?, date_due=? WHERE id = ?;", params=[item, date_due, task_id]).collect()
            for label in [_.strip().lower() for _ in updated_labels if _ not in existing_labels]:
                self.session.sql(f"INSERT INTO HYBRID_TASK_APP_DB.DATA.TASK_LABEL (TASK_ID, LABEL) VALUES (?, LOWER(?));", params=[task_id, label]).collect()    
            for label in [_.strip().lower() for _ in existing_labels if _ not in updated_labels]:
                self.session.sql(f"DELETE FROM HYBRID_TASK_APP_DB.DATA.TASK_LABEL WHERE TASK_ID = ? AND LABEL = ?;", params=[task_id, label]).collect()    
            self.session.sql(f"COMMIT;").collect()
        except Exception as e:
            print(e)
            self.session.sql(f"ROLLBACK;").collect()

    @measure
    def delete_task(self, task_id:int) -> None:
        try:
            self.session.sql(f"BEGIN TRANSACTION;").collect()
            items = self.session.sql(f"DELETE FROM HYBRID_TASK_APP_DB.DATA.TASK_LABEL WHERE TASK_ID = ?;", params=[task_id]).collect()
            items = self.session.sql(f"DELETE FROM HYBRID_TASK_APP_DB.DATA.TASK WHERE ID = ?;", params=[task_id]).collect()
            self.session.sql(f"COMMIT;").collect()
        except Exception as e:
            print(e)
            self.session.sql(f"ROLLBACK;").collect()

    @measure
    def complete_task(self, task_id:int) -> None:
        # No need for a transaction here, single statement and table execution
        #session.sql(f"BEGIN TRANSACTION;").collect()
        items = self.session.sql(f"UPDATE HYBRID_TASK_APP_DB.DATA.TASK SET complete=TRUE WHERE id = ?;", params=[task_id]).collect()
        #session.sql(f"COMMIT;").collect()