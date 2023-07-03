#!/usr/bin/env python3

from __future__ import annotations

import asyncio
from aiologger import Logger
from configparser import ConfigParser
# from datetime import date
import os
from pathlib import Path
# import random

from sqlalchemy import select
from sqlalchemy import func, and_
from sqlalchemy.exc import IntegrityError, NoResultFound
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


def excm(msg: str):
    return "{{{ " + "..... EXCEPTION ....." + os.linesep + msg + os.linesep + "}}}"

##################################################################################
async def select_11(async_session: async_sessionmaker[AsyncSession]) -> None:
    """
    -- 11. Середня оцінка, яку певний викладач ставить певному студентові.
    SELECT st.fullname, tr.fullname, ROUND(AVG(gd.grade),2), COUNT(gd.grade)
    FROM grades gd
    INNER JOIN teacher_subjects ts ON ts.subject_id = gd.subject_id
    INNER JOIN teachers tr ON tr.id = ts.teacher_id
    INNER JOIN students st ON st.id = gd.student_id
    WHERE gd.student_id = 27 AND ts.teacher_id = 4
    --          STUDENT --^            TEACHER --^
    GROUP BY st.id, tr.id
    ORDER BY st.fullname, tr.fullname
    """
    async with async_session() as session:
        try:

            STUDENT_ID = 13
            TEACHER_ID = 4

            stmt = select(Student.fullname
                          , Teacher.fullname
                          , func.round(func.avg(Grade.grade), 2).label("avgd")
                          , func.count(Grade.grade).label("numgd")) \
                    .select_from(Grade) \
                    .join(TeacherSubject
                          , onclause=TeacherSubject.subject_id==Grade.subject_id) \
                    .join(Teacher) \
                    .join(Student) \
                    .where(and_(  Grade.student_id == STUDENT_ID
                                , TeacherSubject.teacher_id == TEACHER_ID)) \
                    .group_by(Student.id, Teacher.id) \
                    .order_by(Student.fullname, Teacher.fullname)

            await logger.info(f"{os.linesep}*** SQL: ***{os.linesep}"
                              f"{str(stmt)}{os.linesep}")
            result = await session.execute(stmt)

            await logger.info(
                    f"11. Середня оцінка, яку певний викладач (id {TEACHER_ID}) "
                    f"ставить певному студентові (id {STUDENT_ID}):")
            for st, tr, avgd, numgd in result:
                await logger.info("%s : %s : %s from %s grades" % (st, tr, avgd, numgd))

            await session.commit()

        except IntegrityError as e:
            await logger.error(excm(str(e)))
        except NoResultFound as e:
            await logger.error(excm(str(e)))

async def async_main() -> None:
    engine = create_async_engine(
        f"postgresql+asyncpg://{CONF_PSUSER}:{CONF_PSPASS}"
        f"@{CONF_PSHOST}:{CONF_PSPORT}/{CONF_PSNAME}",
        echo=CONF_DGECHO,
    )
    # async_sessionmaker: a factory for new AsyncSession objects.
    # expire_on_commit - don't expire objects after transaction commit
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    await select_11(async_session)

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
    overview_config()
    #print(CONF_PSNAME, CONF_PSHOST, CONF_PSPORT, CONF_PSUSER, CONF_PSPASS, CONF_DGECHO)
    logger = Logger.with_default_handlers(name='NoPrintLogger')
    asyncio.run(async_main())
