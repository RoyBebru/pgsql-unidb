#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
from aiologger import Logger
from configparser import ConfigParser
from datetime import datetime, date
from faker import Faker
import os
from pathlib import Path
import platform
import random

from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy import delete
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError, ObjectNotExecutableError
from sqlalchemy.exc import ProgrammingError, DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

uni_model = __import__("uni-model")
Base = getattr(uni_model, "Base")
Subject = getattr(uni_model, "Subject")
Group = getattr(uni_model, "Group")
Teacher = getattr(uni_model, "Teacher")
Student = getattr(uni_model, "Student")
TeacherSubject = getattr(uni_model, "TeacherSubject")
StudentSubject = getattr(uni_model, "StudentSubject")
Grade = getattr(uni_model, "Grade")

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

                fake_groups = [ "GOIT-31", "TOGI-32", "TIGO-33" ]
                obj_list.extend([ Group(codename=gr) for gr in fake_groups ])

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
                obj_list.extend([ Student(fullname=st,
                                          group_id=random.randint(1,len(fake_groups)))
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

async def async_init(fill_with_fakes: bool = True) -> None:
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

    if fill_with_fakes:
        await insert_fake_objects(async_session)

    # for AsyncEngine created in function scope, close and
    # clean-up pooled connections
    await engine.dispose()

### Commnad Line Option Handlers ###

async def opt_cS(session: AsyncSession, arg_list: list):
    """Create SUBJECT
    """
    subject = " ".join(arg_list).split()
    subject = " ".join(subject)
    await logger.info(f"Create Subject '{subject}'")
    if subject == "":
        raise ValueError("Absent Subject name")
    session.add(Subject(title=subject))

async def opt_cG(session: AsyncSession, arg_list: list):
    """Create GROUP
    """
    group = " ".join(arg_list).split()
    group = " ".join(group)
    await logger.info(f"Create Group '{group}'")
    if group == "":
        raise ValueError("Absent Group name")
    session.add(Group(codename=group))

async def opt_cs(session: AsyncSession, arg_list: list):
    """Create STUDENT GROUP SUBJECT1 SUBJECT2 ...
    """
    if len(arg_list) < 3:
        await logger.error("Usage: --cs STUDENT GROUP SUBJECT1 ...")
        return
    student = arg_list[0].split()
    student = " ".join(student)
    if student.find(",") < 0:
        await logger.error("Student must have ',' between lastname and name")
        return
    group = arg_list[1]
    stmt = select(Group.id) \
            .select_from(Group) \
            .where(Group.codename.ilike(group))
    result = await session.execute(stmt)
    try:
        group_id, = result.first()
    except Exception:
        await logger.error(f"No such Group '{group}'")
        return

    stmt = insert(Student) \
            .values(fullname=student, group_id=group_id)
    result = await session.execute(stmt)
    student_id, = result.inserted_primary_key

    for subject in arg_list[2:]:
        stmt = select(Subject.id) \
                .select_from(Subject) \
                .where(Subject.title.ilike(subject))
        result = await session.execute(stmt)
        try:
            subject_id, = result.first()
        except Exception:
            await logger.error(f"No such Subject '{subject}'")
            await session.rollback()
            return

        session.add(StudentSubject(  student_id=student_id
                                   , subject_id=subject_id))

async def opt_cT(session: AsyncSession, arg_list: list):
    """Create TEACHER SUBJECT1 SUBJECT2 ...
    """
    if len(arg_list) < 2:
        await logger.error("Usage: --cT TEACHER SUBJECT1 ...")
        return
    teacher = arg_list[0].split()
    teacher = " ".join(teacher)

    for dg in TEACHER_DEGREE:
        if teacher.lower().startswith(dg.lower()):
            break
    else:
        await logger.error("Teacher name must start with one of "
                           f"degree '{', '.join(TEACHER_DEGREE)}'")
        return

    try:
        stmt = insert(Teacher) \
                .values(fullname=teacher)
        result = await session.execute(stmt)
        teacher_id, = result.inserted_primary_key
    except Exception as e:
        await logger.error(f"Teacher '{teacher}' cannot be added: {str(e)}")
        return

    for subject in arg_list[1:]:
        stmt = select(Subject.id) \
                .select_from(Subject) \
                .where(Subject.title.ilike(subject))
        result = await session.execute(stmt)
        try:
            subject_id, = result.first()
        except Exception:
            await logger.error(f"No such Subject '{subject}'")
            await session.rollback()
            return

        session.add(TeacherSubject(  teacher_id=teacher_id
                                   , subject_id=subject_id))

async def opt_cg(session: AsyncSession, arg_list: list):
    """Create YYYY-MM-DD STUDENT 2..5 TEACHER SUBJECT
    """
    try:
        date_of = datetime.strptime(arg_list[0], r"%Y-%m-%d").date()
    except ValueError:
        await logger.error(f"Wrong Date '{arg_list[0]}': must be like 2023-07-01. " +
                           os.linesep +
                           "Usage: --cg DATE STUDENT 2..5 TEACHER SUBJECT")
        return

    student = arg_list[1].split()
    student = " ".join(student)
    stmt = select(Student.id) \
            .select_from(Student) \
            .where(Student.fullname.ilike(student))
    result = await session.execute(stmt)
    try:
        student_id, = result.first()
    except Exception:
        await logger.error(f"No such student '{student}'")
        return

    try:
        grade = int(arg_list[2])
    except ValueError:
        await logger.error(f"Wrong Grade '{grade}'. " + os.linesep +
                           "Usage: --cg DATE STUDENT 2..5 TEACHER SUBJECT")
        return

    teacher = arg_list[3].split()
    teacher = " ".join(teacher)
    stmt = select(Teacher.id) \
            .select_from(Teacher) \
            .where(Teacher.fullname.ilike(teacher))
    result = await session.execute(stmt)
    try:
        teacher_id, = result.first()
    except Exception:
        await logger.error(f"No such Teacher '{teacher}'")
        return

    subject = arg_list[4].split()
    subject = " ".join(subject)
    stmt = select(Subject.id) \
            .select_from(Subject) \
            .where(Subject.title.ilike(subject))
    result = await session.execute(stmt)
    try:
        subject_id, = result.first()
    except Exception:
        await logger.error(f"No such Subject '{subject}'")
        return

    session.add(Grade(  date_of=date_of 
                      , grade=grade
                      , student_id=student_id
                      , teacher_id=teacher_id
                      , subject_id=subject_id))

async def opt_rS(session: AsyncSession, arg_list: list):
    """Read *SUBJECT*SAMPLE*
    """
    subject = " ".join(arg_list).split()
    subject = " ".join(subject).replace(r"*", r"%")
    if subject == "":
        subject = r"%"
    stmt = select(Subject.id, Subject.title) \
            .select_from(Subject) \
            .where(Subject.title.ilike(subject))
    result = await session.execute(stmt)
    for id, subject in result:
        await logger.info("%2d | %s" % (id, subject))

async def opt_rG(session: AsyncSession, arg_list: list):
    """Read *GROUP*SAMPLE*
    """
    group = " ".join(arg_list).split()
    group = " ".join(group).replace(r"*", r"%")
    if group == "":
        group = r"%"
    stmt = select(Group.id, Group.codename) \
            .select_from(Group) \
            .where(Group.codename.ilike(group))
    result = await session.execute(stmt)
    for id, group in result:
        await logger.info("%2d | %s" % (id, group))

async def opt_rs(session: AsyncSession, arg_list: list):
    """Read *STUDENT*SAMPLE*
    """
    student = " ".join(arg_list).split()
    student = " ".join(student).replace(r"*", r"%")
    if student == "":
        student = r"%"
    stmt = select(Student.id, Group.codename, Student.fullname) \
            .select_from(Student) \
            .join(Group) \
            .where(Group.codename.ilike(student))
    result = await session.execute(stmt)
    for id, group, student in result:
        await logger.info("%2d | %7s | %-s" % (id, group, student))

async def opt_rT(session: AsyncSession, arg_list: list):
    """Read *TEACHER*SAMPLE*
    """
    teacher = " ".join(arg_list).split()
    teacher = " ".join(teacher).replace(r"*", r"%")
    if teacher == "":
        teacher = r"%"
    stmt = select(Teacher.id, Teacher.fullname) \
            .select_from(Teacher) \
            .where(Teacher.fullname.ilike(teacher))
    result = await session.execute(stmt)
    for id, teacher in result:
        await logger.info("%2d | %-s" % (id, teacher))

async def opt_rg(session: AsyncSession, arg_list: list):
    """Read *STUDENT_OR_TEACHER_OR_SUBJECT*SAMPLE* | DATE
    """
    arg = " ".join(arg_list).split()
    arg = " ".join(arg).replace(r"*", r"%")

    try:
        date_of = datetime.strptime(arg, r"%Y-%m-%d").date()
        # Date
        stmt = select(  Grade.id
                      , Grade.date_of
                      , Teacher.fullname
                      , Student.fullname
                      , Subject.title
                      , Grade.grade) \
                .select_from(Grade) \
                .join(Teacher) \
                .join(Student) \
                .join(Subject) \
                .where(Grade.date_of == date_of)
    except ValueError:
        for degree in TEACHER_DEGREE:
            if arg.lower().startswith(degree.lower()):
                # Teacher
                print(f"teacher '{arg}'")
                stmt = select(  Grade.id
                              , Grade.date_of
                              , Teacher.fullname
                              , Student.fullname
                              , Subject.title
                              , Grade.grade) \
                        .select_from(Grade) \
                        .join(Teacher) \
                        .join(Student) \
                        .join(Subject) \
                        .where(Teacher.fullname.ilike(arg))
                break
        else:
            if arg.find(',') > 0:
                # Student
                stmt = select(  Grade.id
                              , Grade.date_of
                              , Teacher.fullname
                              , Student.fullname
                              , Subject.title
                              , Grade.grade) \
                        .select_from(Grade) \
                        .join(Teacher) \
                        .join(Student) \
                        .join(Subject) \
                        .where(Student.fullname.ilike(arg))
            else:
                # Subject
                stmt = select(  Grade.id
                              , Grade.date_of
                              , Teacher.fullname
                              , Student.fullname
                              , Subject.title
                              , Grade.grade) \
                        .select_from(Grade) \
                        .join(Teacher) \
                        .join(Student) \
                        .join(Subject) \
                        .where(Subject.title.ilike(arg))

    result = await session.execute(stmt)
    for id, date_of, teacher, student, subject, grade in result:
        await logger.info("%3d | %10s | %25s | %25s | %25s | %s" %
                          (id, date_of, teacher, student, subject, grade))

async def opt_uS(session: AsyncSession, arg_list: list):
    """Update *SUBJECT*SAMPLE* NEW_SUBJECT_NAME"""
    subject = arg_list[0].split()
    subject = " ".join(subject).replace(r"*", r"%")
    new_subject = arg_list[1].split()
    new_subject = " ".join(new_subject)

    stmt = update(Subject) \
            .where(Subject.title.ilike(subject)) \
            .values(title=new_subject)
            # .returning(Subject.id, Subject.title)
    try:
        result = await session.execute(stmt)
    except Exception:
        await logger.info("Error updating: there are duplicates!")
        await session.rollback()
        return
    # (id, title), = result
    await logger.info(f"Update {result.rowcount} entry(-ies)")

async def opt_uG(session: AsyncSession, arg_list: list):
    """Update *GROUP*SAMPLE* NEW_GROUP_NAME"""
    group = arg_list[0].split()
    group = " ".join(group).replace(r"*", r"%")
    new_group = arg_list[1].split()
    new_group = " ".join(new_group)

    stmt = update(Group) \
            .where(Group.codename.ilike(group)) \
            .values(codename=new_group)
    try:
        result = await session.execute(stmt)
    except Exception:
        await logger.info("Error updating: there are duplicates!")
        await session.rollback()
        return
    await logger.info(f"Update {result.rowcount} entry(-ies)")

async def opt_us(session: AsyncSession, arg_list: list):
    """Update *STUDENT*SAMPLE* NEW_STUDENT_NAME OTHER_GROUP SUBJECT1 SUBJECT2 ..."""
    if len(arg_list) < 4:
        await logger.error("Usage: --us *STUDENT*SAMPLE* NEW_STUDENT_NAME "
                           "OTHER_GROUP SUBJECT1 ...")
        return
    student = arg_list[0].split()
    student = " ".join(student).replace(r"*", r"%")
    if student.find(",") < 0:
        await logger.error("Student must have ',' between lastname and name")
        return
    new_student = arg_list[1].split()
    new_student = " ".join(new_student)
    if new_student.find(",") < 0:
        await logger.error("New student name must have ',' between lastname and name")
        return
    group = arg_list[2]
    stmt = select(Group.id) \
            .select_from(Group) \
            .where(Group.codename.ilike(group))
    result = await session.execute(stmt)
    try:
        group_id, = result.first()
    except Exception:
        await logger.error(f"No such Group '{group}'")
        return

    stmt = update(Student) \
            .where(Student.fullname.ilike(student)) \
            .values(fullname=new_student, group_id=group_id) \
            .returning(Student.id)
    result = await session.execute(stmt)

    try:
        (student_id,), = result
    except Exception:
        await logger.error(f"Does not exist Student with name like '{student}' "
                           "to update.")
        return

    stmt = delete(StudentSubject).where(StudentSubject.student_id == student_id)
    result = await session.execute(stmt)

    for subject in arg_list[3:]:
        stmt = select(Subject.id) \
                .select_from(Subject) \
                .where(Subject.title.ilike(subject))
        result = await session.execute(stmt)

        try:
            (subject_id,), = result
        except Exception:
            await logger.error(f"No such Subject '{subject}'")
            await session.rollback()
            return

        session.add(StudentSubject(  student_id=student_id
                                   , subject_id=subject_id))

async def opt_uT(session: AsyncSession, arg_list: list):
    """Update *TEACHER*SAMPLE* NEW_TEACHER_NAME SUBJECT1 SUBJECT2 ..."""
    if len(arg_list) < 3:
        await logger.error("Usage: --uT *TEACHER*SAMPLE* NEW_TEACHER_NAME "
                           "SUBJECT1 ...")
        return
    teacher = arg_list[0].split()
    teacher = " ".join(teacher).replace(r"*", r"%")
    for degree in TEACHER_DEGREE:
        if teacher.lower().startswith(degree.lower()):
            break
    else:
        await logger.error("Teacher must start with one of degree prefix: " +
                           ", ".join(TEACHER_DEGREE))
        return
    new_teacher = arg_list[1].split()
    new_teacher = " ".join(new_teacher)
    for degree in TEACHER_DEGREE:
        if new_teacher.lower().startswith(degree.lower()):
            break
    else:
        await logger.error("New Teacher name must start with one of degree prefix: " +
                           ", ".join(TEACHER_DEGREE))
        return

    stmt = update(Teacher) \
            .where(Teacher.fullname.ilike(teacher)) \
            .values(fullname=new_teacher) \
            .returning(Teacher.id)
    result = await session.execute(stmt)

    try:
        (teacher_id,), = result
    except Exception:
        await logger.error(f"Does not exist Teacher with name like '{teacher}'"
                           " to update.")
        return

    stmt = delete(TeacherSubject).where(TeacherSubject.teacher_id == teacher_id)
    result = await session.execute(stmt)

    for subject in arg_list[2:]:
        stmt = select(Subject.id) \
                .select_from(Subject) \
                .where(Subject.title.ilike(subject))
        result = await session.execute(stmt)

        try:
            (subject_id,), = result
        except Exception:
            await logger.error(f"No such Subject '{subject}'")
            await session.rollback()
            return

        session.add(TeacherSubject(  teacher_id=teacher_id
                                   , subject_id=subject_id))

async def opt_dS(session: AsyncSession, arg_list: list):
    """Delete *SUBJECT*SAMPLE*"""
    subject = " ".join(arg_list).split()
    subject = " ".join(subject)
    await logger.info(f"Delete Subject '{subject}'")
    if subject == "":
        raise ValueError("Absent subject name")

    subject = subject.replace(r'*', r'%')
    stmt = delete(Subject).where(Subject.title.ilike(subject))
    result = await session.execute(stmt)
    await logger.info(f"Deleted {result.rowcount} entry(-ies)")

async def opt_dG(session: AsyncSession, arg_list: list):
    """Delete *GROUP*SAMPLE*"""
    group = " ".join(arg_list).split()
    group = " ".join(group)
    await logger.info(f"Delete Group '{group}'")
    if group == "":
        raise ValueError("Absent group name")

    group = group.replace(r'*', r'%')
    stmt = delete(Group).where(Group.codename.ilike(group))
    result = await session.execute(stmt)
    await logger.info(f"Deleted {result.rowcount} entry(-ies)")

async def opt_ds(session: AsyncSession, arg_list: list):
    """Delete *STUDENT*SAMPLE*"""
    student = " ".join(arg_list).split()
    student = " ".join(student)
    await logger.info(f"Delete Student '{student}'")
    if student == "":
        raise ValueError("Absent student name")

    student = student.replace(r'*', r'%')
    stmt = delete(Student).where(Student.fullname.ilike(student))
    result = await session.execute(stmt)
    await logger.info(f"Deleted {result.rowcount} entry(-ies)")

async def opt_dT(session: AsyncSession, arg_list: list):
    """Delete *TEACHER*SAMPLE*"""
    teacher = " ".join(arg_list).split()
    teacher = " ".join(teacher)
    await logger.info(f"Delete Teacher '{teacher}'")
    if teacher == "":
        raise ValueError("Absent teacher name")

    teacher = teacher.replace(r'*', r'%')
    stmt = delete(Teacher).where(Teacher.fullname.ilike(teacher))
    result = await session.execute(stmt)
    await logger.info(f"Deleted {result.rowcount} entry(-ies)")

async def opt_dg(session: AsyncSession, arg_list: list):
    """Delete *STUDENT*SAMPLE* | DATE | *SUBJECT*SAMPLE*"""
    arg = " ".join(arg_list).split()
    arg = " ".join(arg)
    try:
        date_of = datetime.strptime(arg, r"%Y-%m-%d").date()
        await logger.info(f"Delete Grade by date '{arg}'")
        stmt = delete(Grade).where(Grade.date_of == date_of)
        result = await session.execute(stmt)
        await logger.info(f"Deleted {result.rowcount} entry(-ies)")
        return
    except ValueError:
        pass
    except Exception as e:
        await logger.error(f"Delete argument error: {str(e)}")
        return

    if arg.find(",") >= 0:
        await logger.info(f"Delete Grade by Student '{arg}'")
        arg = arg.replace(r'*', r'%')
        stmt = delete(Grade) \
            .where(Grade.student_id == Student.id) \
            .where(Student.fullname.ilike(arg))
        result = await session.execute(stmt)
        await logger.info(f"Deleted {result.rowcount} entry(-ies)")
        return

    await logger.info(f"Delete Grade by Subject '{arg}'")
    arg = arg.replace(r'*', r'%')
    stmt = delete(Grade) \
        .where(Grade.subject_id == Subject.id) \
        .where(Subject.title.ilike(arg))
    result = await session.execute(stmt)
    await logger.info(f"Deleted {result.rowcount} entry(-ies)")

options = \
{   "cS": (opt_cS, 1, "Create SUBJECT")
,   "cG": (opt_cG, 1, "Create GROUP")
,   "cs": (opt_cs, '+', "Create STUDENT GROUP SUBJECT1 SUBJECT2 ...")
,   "cT": (opt_cT, '+', "Create TEACHER SUBJECT1 SUBJECT2 ...")
,   "cg": (opt_cg, 5, "Create YYYY-MM-DD STUDENT 2..5 TEACHER SUBJECT")

,   "rS": (opt_rS, '*', "Read *SUBJECT*SAMPLE*")
,   "rG": (opt_rG, '*', "Read *GROUP*SAMPLE*")
,   "rs": (opt_rs, '*', "Read *STUDENT*SAMPLE*")
,   "rT": (opt_rT, '*', "Read *TEACHER*SAMPLE*")
,   "rg": (opt_rg, 1, "Read *STUDENT_OR_TEACHER_OR_SUBJECT*SAMPLE* | DATE")

,   "uS": (opt_uS, 2, "Update *SUBJECT*SAMPLE* NEW_SUBJECT_NAME")
,   "uG": (opt_uG, 2, "Update *GROUP*SAMPLE* NEW_GROUP_NAME")
,   "us": (opt_us, '+', "Update *STUDENT*SAMPLE* NEW_STUDENT_NAME OTHER_GROUP "
                        "SUBJECT1 SUBJECT2 ...")
,   "uT": (opt_uT, '+', "Update *TEACHER*SAMPLE* NEW_TEACHER_NAME "
                        "SUBJECT1 SUBJECT2 ...")

,   "dS": (opt_dS, 1, "Delete *SUBJECT*SAMPLE*")
,   "dG": (opt_dG, 1, "Delete *GROUP*SAMPLE*")
,   "ds": (opt_ds, 1, "Delete *STUDENT*SAMPLE*")
,   "dT": (opt_dT, 1, "Delete *TEACHER*SAMPLE*")
,   "dg": (opt_dg, 1, "Delete *STUDENT*SAMPLE* | DATE | *SUBJECT*SAMPLE*")
}

async def async_handle_options(ordered) -> None:
    engine = create_async_engine(
        f"postgresql+asyncpg://{CONF_PSUSER}:{CONF_PSPASS}"
        f"@{CONF_PSHOST}:{CONF_PSPORT}/{CONF_PSNAME}",
        echo=CONF_DGECHO,
    )
    # async_sessionmaker: a factory for new AsyncSession objects.
    # expire_on_commit - don't expire objects after transaction commit
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with async_session() as session:
            async with session.begin():
                for opt, arg_list in ordered:
                    await logger.info(f"Handle '--{opt} {' '.join(arg_list)}'")
                    await options[opt][0](session, arg_list)
    except (ObjectNotExecutableError, ProgrammingError, DBAPIError) as e:
        await logger.warning(excm(str(e)))
    except ConnectionRefusedError as e:
        await logger.error(excm(str(e)))
        return

    # for AsyncEngine created in function scope, close and
    # clean-up pooled connections
    await engine.dispose()


class ActionOrdered(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if 'ordered' not in namespace:
            setattr(namespace, 'ordered', [])
        previous = namespace.ordered
        previous.append((self.dest, values))
        setattr(namespace, 'ordered', previous)

def handle_options():
    parser = argparse.ArgumentParser(description="CRUD For UNI Database. "
                                     "Student name must have ',' ('Петренко, Тарас'). "
                                     "Teacher name must start with: "
                                     + ", ".join(TEACHER_DEGREE) + ".")
    for opt, how in options.items():
        parser.add_argument(f"--{opt}", metavar='o', nargs=how[1], help=how[2],
                            action=ActionOrdered)

    args = parser.parse_args()

    if "ordered" not in args:
        try:
            match (input("Init database ? [Y-fill with fake data|C-only init|N] ")
                         or "N") \
                    .strip()[0].upper():
                case "Y":
                    asyncio.run(async_init(fill_with_fakes=True))
                case "C":
                    asyncio.run(async_init(fill_with_fakes=False))
                case "N":
                    print("N")
                case _:
                    print("Nothing")
        except (KeyboardInterrupt, EOFError):
            print()
        return
    
    asyncio.run(async_handle_options(args.ordered))

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
    handle_options()
