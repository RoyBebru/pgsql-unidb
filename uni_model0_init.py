#!/usr/bin/env python3

from __future__ import annotations

import asyncio
from aiologger import Logger
from configparser import ConfigParser
from datetime import date
from faker import Faker
import os
from pathlib import Path
import platform
import random

from sqlalchemy.exc import IntegrityError, ObjectNotExecutableError
from sqlalchemy.exc import ProgrammingError, DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


from uni_model0 import Base, Subject, Teacher, Student, TeacherSubject, \
                        StudentSubject, Grade


fake = Faker("uk_UA")

TEACHER_DEGREE = ['проф.','д-р.','к.ф-м.н','PhD','к.т.н']

def excm(msg: str):
    return "{{{ " + "..... EXCEPTION ....." + os.linesep + msg + os.linesep + "}}}"

async def insert_fake_objects(async_session: async_sessionmaker[AsyncSession]) -> None:
    async with async_session() as session:
        try:
            async with session.begin():
                obj_list = []

                fake_subjects = [   "Компесаторна негентропія"
                                ,   "Девіаторна алгебра"
                                ,   "SOLID'ософія"
                                ,   "Мультиарний аналіз"
                                ,   "Хаотичний синтез"
                                ,   "Археологія Абсурдології"
                                ,   "Квантова Алхімія"
                                ,   "Предикативна Квінциляція"
                                ]
                obj_list.extend([ Subject(title=sb) for sb in fake_subjects ])

                # Fake Teachers
                fake_teachers = [
                    f"{random.choice(TEACHER_DEGREE)} "
                    f"{fake.first_name()} {fake.last_name()}"
                    for _ in range(5)
                ]
                obj_list.extend([ Teacher(fullname=tr) for tr in fake_teachers ])

                # Fake Students
                num_of_students = random.randint(30, 50)
                fake_students = [ f"{fake.last_name()}, {fake.first_name()}"
                                  for _ in range(num_of_students) ]
                obj_list.extend([ Student(fullname=st)
                                  for st in fake_students ])
                # Assign teachers to subjects
                subjects1 = list(range(1,len(fake_subjects)+1))
                subjects2 = []
                for _ in range(len(fake_subjects)-len(fake_teachers)):
                    n = random.randint(0, len(subjects1)-1)
                    subjects2.append(subjects1.pop(n))
                teacher_subjects = list(zip(subjects1, range(1,len(fake_teachers)+1)))
                teacher_subjects.extend(
                    list(zip(subjects2,
                        random.choices(range(1,len(fake_teachers)+1),k=len(subjects2)))))
                obj_list.extend([ TeacherSubject(subject_id=sid, teacher_id=tid)
                                  for sid, tid in teacher_subjects ])

                # Assign students to subjects
                # Each student must listen any 5 subjects
                SUBJECTS_PER_STUDENT = 5
                student_subjects = []
                for i in range(1, num_of_students+1):
                    subjects1 = list(range(1,len(fake_subjects)+1))
                    for _ in range(len(fake_subjects) - SUBJECTS_PER_STUDENT):
                        subjects1.pop(random.randint(0, len(subjects1)-1))
                    student_subjects.extend(zip(subjects1, [i] * SUBJECTS_PER_STUDENT))
                obj_list.extend([ StudentSubject(subject_id=sid, student_id=did)
                                  for sid, did in student_subjects ])

                # Assign grades
                STUDY_DAYS = 30 * 3
                ord_today = date.today().toordinal()
                grades = []
                for student_id in range(1, num_of_students+1):
                    for _ in range(random.randint(1,20)):
                        # date_of
                        while True:
                            date_of = date.fromordinal(ord_today -
                                                       random.randint(0,STUDY_DAYS-1))
                            if date_of.weekday() < 5:
                                break # not Saturday and not Sunday
                        # subject_id
                        subject_id = student_subjects[
                            student_id * SUBJECTS_PER_STUDENT -
                            random.randint(1,SUBJECTS_PER_STUDENT)][0]
                        # real life grades distribution
                        grade = random.randint(1,100)
                        if grade < 10:
                            grade = 2
                        elif grade < 25:
                            grade = 3
                        elif grade < 60:
                            grade = 4
                        else:
                            grade = 5
                        # what teacher who can conduct lecture on this subject
                        teacher_ids = [ t_id for s_id, t_id in teacher_subjects
                                             if s_id == subject_id ]
                        teacher_id = random.choice(teacher_ids)
                        grades.append((date_of
                                       , grade
                                       , student_id
                                       , subject_id
                                       , teacher_id))

                obj_list.extend([ Grade(date_of=df
                                        , grade=gd
                                        , student_id=did
                                        , subject_id=sid
                                        , teacher_id=tid)
                                  for df,gd,did,sid,tid in grades ])
                # Add all to DB
                session.add_all(obj_list)

        except IntegrityError as e:
            await logger.error(excm(str(e)))

async def async_init() -> None:
    engine = create_async_engine(
        f"postgresql+asyncpg://{CONF_PSUSER}:{CONF_PSPASS}"
        f"@{CONF_PSHOST}:{CONF_PSPORT}/{CONF_PSNAME}",
        echo=CONF_DGECHO,
    )
    # async_sessionmaker: a factory for new AsyncSession objects.
    # expire_on_commit - don't expire objects after transaction commit
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with engine.begin() as conn:
            # Delete all tables from database
            await conn.exec_driver_sql("DROP SCHEMA public CASCADE;")
            await conn.exec_driver_sql("CREATE SCHEMA public;")
            await conn.exec_driver_sql("GRANT ALL ON SCHEMA public TO postgres;")
            await conn.exec_driver_sql("GRANT ALL ON SCHEMA public TO public;")
            # Create new tables corresponded to model
            await conn.run_sync(Base.metadata.create_all)
    except (ObjectNotExecutableError, ProgrammingError, DBAPIError) as e:
        await logger.warning(excm(str(e)))
    except ConnectionRefusedError as e:
        await logger.error(excm(str(e)))
        return

    await insert_fake_objects(async_session)

    # for AsyncEngine created in function scope, close and
    # clean-up pooled connections
    await engine.dispose()


def overview_config():
    global CONF_PSNAME, CONF_PSHOST, CONF_PSPORT, CONF_PSUSER, CONF_PSPASS
    global CONF_DGECHO
    try:
        conf = ConfigParser()
        confpathfile = Path(__file__)
        confpathfile = confpathfile.parent / "config.ini"
        conf.read(confpathfile)
        conf_postgresql, conf_debug = "", ""
        for s in conf.sections():
            ss = s.strip().upper()
            if "POSTGRESQL" == ss:
                conf_postgresql = s
            elif "DEBUG" == ss:
                conf_debug = s
        if not conf_postgresql or not conf_debug:
            raise SyntaxError("Absent needed sections")
        CONF_PSNAME, CONF_PSHOST, CONF_PSPORT, CONF_PSUSER, CONF_PSPASS = \
            conf.get(conf_postgresql, "NAME"), \
            conf.get(conf_postgresql, "HOST"), \
            conf.get(conf_postgresql, "PORT"), \
            conf.get(conf_postgresql, "USER"), \
            conf.get(conf_postgresql, "PASS")
        CONF_DGECHO = conf.get(conf_debug, "ECHO")
        if CONF_DGECHO.isdigit() and int(CONF_DGECHO):
            CONF_DGECHO = True
        else:
            CONF_DGECHO = False
    except Exception as e:
        print(f"Config file ('{confpathfile}'): {str(e)}")
        exit(1)

if __name__ == "__main__":
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    overview_config()
    logger = Logger.with_default_handlers(name='NoPrintLogger')
    asyncio.run(async_init())
