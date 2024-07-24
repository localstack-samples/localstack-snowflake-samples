-- taken from: https://quickstarts.snowflake.com/guide/build_a_data_application_with_hybrid_tables/index.html

-- USE ROLE ACCOUNTADMIN;

-- Create role HYBRID_TASK_APP_ROLE
--CREATE OR REPLACE ROLE HYBRID_TASK_APP_ROLE;
--GRANT ROLE HYBRID_TASK_APP_ROLE TO ROLE ACCOUNTADMIN ;

-- Create HYBRID_QUICKSTART_WH warehouse
--CREATE OR REPLACE WAREHOUSE HYBRID_TASK_APP_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
--GRANT OWNERSHIP ON WAREHOUSE HYBRID_TASK_APP_WH TO ROLE HYBRID_TASK_APP_ROLE;
--GRANT CREATE DATABASE ON ACCOUNT TO ROLE HYBRID_TASK_APP_ROLE;

-- Use role and create HYBRID_QUICKSTART_DB database and schema.
CREATE OR REPLACE DATABASE HYBRID_TASK_APP_DB;
--GRANT OWNERSHIP ON DATABASE HYBRID_TASK_APP_DB TO ROLE HYBRID_TASK_APP_ROLE;
CREATE OR REPLACE SCHEMA DATA;
--GRANT OWNERSHIP ON SCHEMA HYBRID_TASK_APP_DB.DATA TO ROLE HYBRID_TASK_APP_ROLE;

-- Use role
--USE ROLE HYBRID_TASK_APP_ROLE;

-- Set step context use HYBRID_DB_USER_(USER_NUMBER) database and DATA schema
USE DATABASE HYBRID_TASK_APP_DB;
USE SCHEMA DATA;


CREATE OR REPLACE HYBRID TABLE "USER" (
    id INT AUTOINCREMENT,
    login VARCHAR NOT NULL,
    name VARCHAR,
    hpassword VARCHAR NOT NULL,
    primary key (id)
);

CREATE OR REPLACE SEQUENCE TASK_ID_SEQ;

CREATE OR REPLACE HYBRID TABLE "TASK" (
    id INT DEFAULT TASK_ID_SEQ.nextval,
	item VARCHAR(16777216),
    complete BOOLEAN DEFAULT FALSE,
    date_due TIMESTAMP DEFAULT NULL,
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    date_modified TIMESTAMP DEFAULT NULL,
    date_completed TIMESTAMP DEFAULT NULL,
    owner_id INT,
	primary key (id) rely,
    foreign key (owner_id) references "USER"(id),
    index idx001_owner_id(owner_id),
    index idx002_complete(complete)
);

CREATE OR REPLACE HYBRID TABLE TASK_LABEL (
    task_id INT NOT NULL,
    label VARCHAR,
    primary key (task_id, label),
    foreign key (task_id) references "TASK"(id) ,
    index idx001_label(label)
);

CREATE OR REPLACE VIEW TASK_DETAIL AS
SELECT
      t.id
    , t.item
    , t.complete
    , t.date_due
    , DATEDIFF('day', current_date(), date_trunc('day', t.date_due)) as days_due
    , iff(t.complete, 'completed', iff(date_trunc('day', t.date_due) = current_date(), 'today', iff(date_trunc('day', t.date_due) < current_date, 'overdue', 'upcoming'))) status
    , t.owner_id
FROM TASK t
;

CREATE OR REPLACE VIEW LABEL_DETAIL AS
SELECT
      owner_id
    , row_number() over(PARTITION BY owner_id ORDER BY COUNT(*) DESC) as index
    , count(*) as count
    , lower(l.label) as label
    , count(*) / sum(count(*)) over(PARTITION BY owner_id) as percent
FROM TASK t
INNER JOIN TASK_LABEL l ON l.task_id = t.id
WHERE t.complete = FALSE
GROUP BY ALL
ORDER BY 3 DESC;

CREATE OR REPLACE VIEW STATUS_DETAIL AS
SELECT
      owner_id
    , row_number() over(PARTITION BY owner_id ORDER BY COUNT(*) DESC) as index
    , t.status
    , count(*) as count
     , count(*) / sum(count(*)) over(PARTITION BY owner_id) as percent
FROM TASK_DETAIL t
WHERE t.complete = FALSE
GROUP BY ALL
ORDER BY 3 DESC;

CREATE OR REPLACE VIEW LABEL_STATUS_DETAIL AS
SELECT
      owner_id
    , label
    , row_number() over(PARTITION BY owner_id, label ORDER BY COUNT(*) DESC) as index
    , t.status
    , count(*) as count
FROM TASK_DETAIL t
INNER JOIN TASK_LABEL l ON l.task_id = t.id
WHERE t.complete = FALSE
GROUP BY ALL
ORDER BY 4 DESC;

-- insert data

INSERT INTO "USER" (login, name, hpassword) VALUES ('first_user','First Demo User','hybrid@123');
INSERT INTO "USER" (login, name, hpassword) VALUES ('second_user','Second Demo User','hybrid@123');
INSERT INTO "USER" (login, name, hpassword) VALUES ('third_user','Third Demo User','hybrid@123');

create or replace function create_fake_task(seed int, labels string, userids string)
returns VARIANT
language python
runtime_version = '3.10'
handler = 'create'
packages = ('faker')
as
$$

from faker import Faker
import random
import datetime
from datetime import timedelta
from collections import OrderedDict

fake = Faker()

def create(row_number=-1, label_elements='default_label', userids=''):

    random.seed(row_number)

    today = datetime.datetime.today()
    item = fake.sentence(nb_words=6, variable_nb_words=True)
    date_created = fake.date_time_between(start_date='-1y')
    date_due = date_created + timedelta(random.randint(1,60))
    date_modified = date_created + timedelta(random.randint(1,100))
    date_completed = date_created + timedelta(random.randint(1, 100))
    completed = (date_completed <= today)
    if not completed:
        date_completed = None
    if (date_modified > today):
        date_modified = today

    label_elements_weighted = [(_[0], int(_[1])) for _ in [f'{_}:1'.split(':') for _ in label_elements.split(',')]]
    labels = fake.random_elements(elements=OrderedDict(label_elements_weighted), unique=True, length=random.randint(1, min(4, len(label_elements_weighted))))
    owner_id = fake.random_element(elements=(userids.split(',')))

    return {
        "item": item,
        "completed": completed,
        "date_due": date_due,
        "date_created": date_created,
        "date_modified": date_modified,
        "date_completed": date_completed,
        "labels": labels,
        "owner_id": owner_id
    }

$$;

CREATE OR REPLACE HYBRID TABLE sample_tasks (
    id INT DEFAULT TASK_ID_SEQ.nextval,
	item VARCHAR(16777216),
    complete BOOLEAN DEFAULT FALSE,
    date_due TIMESTAMP DEFAULT NULL,
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    date_modified TIMESTAMP DEFAULT NULL,
    date_completed TIMESTAMP DEFAULT NULL,
    owner_id INT,
    labels VARIANT,
	primary key (id)
)
AS
SELECT
      id
    , data:"item"::varchar as item
    , data:"completed"::boolean as complete
    , data:"date_due"::timestamp as date_due
    , data:"date_created"::timestamp as date_created
    , data:"date_modified"::timestamp as date_modified
    , data:"date_completed"::timestamp as date_completed
    , data:"owner_id"::int as owner_id
    , data:"labels"::variant as labels
FROM
(
    SELECT
        id
        , create_fake_task(id, 'work:8,personal:6,urgent:1,hobbies:4,family:5', (SELECT LISTAGG(id,',') FROM "USER")) as data
    FROM
    (
        SELECT
            TASK_ID_SEQ.nextval as id
        FROM TABLE(GENERATOR(ROWCOUNT => 1500)) v
    )
)
ORDER BY 1;

INSERT INTO "TASK" (
      id
    , item
    , complete
    , date_due
    , date_created
    , date_modified
    , date_completed
    , owner_id )
  SELECT
      id
    , item
    , complete
    , date_due
    , date_created
    , date_modified
    , date_completed
    , owner_id
  FROM sample_tasks;

INSERT INTO TASK_LABEL (
      task_id
    , label )
SELECT
    t.id as task_id
  , l.VALUE::VARCHAR as label
FROM sample_tasks t,
LATERAL FLATTEN (input=>labels) l;


-- clean up tmp constructs
-- DROP TABLE SAMPLE_TASKS;
-- DROP FUNCTION create_fake_task(int, string, string);
