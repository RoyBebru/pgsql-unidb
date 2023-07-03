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
async def select_08(async_session: async_sessionmaker[AsyncSession]) -> None:
    """
    -- 8. Знайти середню оцінку, який ставить певний викладач зі своїх предметів.
    SELECT tr.fullname, ROUND(AVG(gd.grade),2), COUNT(*)
    FROM grades gd, teachers tr
    INNER JOIN teacher_subjects ts ON ts.teacher_id = tr.id
    WHERE gd.subject_id = ts.subject_id AND ts.teacher_id = tr.id
    GROUP BY tr.id
    ORDER BY tr.fullname
    """
    async with async_session() as session:
        try:

            stmt = select(  Teacher.fullname
                          , func.round(func.avg(Grade.grade), 2).label("avgd")) \
                    .select_from(Grade, Teacher) \
                    .join(TeacherSubject) \
                    .where(and_(Grade.subject_id == TeacherSubject.subject_id
                                , TeacherSubject.teacher_id == Teacher.id)) \
                    .group_by(Teacher.id) \
                    .order_by(Teacher.fullname)

            await logger.info(f"{os.linesep}*** SQL: ***{os.linesep}"
                              f"{str(stmt)}{os.linesep}")
            result = await session.execute(stmt)

            await logger.info("8. Знайти середню оцінку, який ставить "
                              "кожний певний викладач зі своїх предметів:")
            for tr, avgd in result:
                await logger.info("%25s = %s" % (tr, avgd))

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

    await select_08(async_session)

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
