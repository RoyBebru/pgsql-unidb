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
from sqlalchemy import func, desc
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
async def select_02(async_session: async_sessionmaker[AsyncSession]) -> None:
    """
    -- 2. Знайти студента із найвищою середньою оцінкою з певного предмета.
    -- Якщо оцінка однакова - найвищою вважається середня оцінка з найбільшої кількості
    -- оцінок.
    SELECT sb.title || CHR(9) || (
        SELECT st.fullname || chr(9) || ROUND(AVG(gd.grade),2)
        FROM grades gd
        INNER JOIN students st ON st.id = gd.student_id 
        WHERE gd.subject_id = sb.id
        GROUP BY st.fullname
        ORDER BY AVG(gd.grade) DESC, COUNT(gd.grade) DESC
        LIMIT 1
    ) AS max_avr_by_subject
    FROM subjects sb
    """
    async with async_session() as session:
        try:
            # stmt = select(
            #     Student.fullname, func.round(func.avg(Grade.grade), 2).label('avgd')) \
            #     .select_from(Grade).join(Student).group_by(Student.id) \
            #     .order_by(desc('avgd')).limit(5) #.all()

            stmt1 = select(func.concat(Student.fullname
                                       , func.chr(9)
                                       , func.round(func.avg(Grade.grade), 2))) \
                    .select_from(Grade) \
                    .join(Student) \
                    .where(Grade.subject_id == Subject.id) \
                    .group_by(Student.fullname) \
                    .order_by(desc(func.avg(Grade.grade)), desc(func.count(Grade.grade))) \
                    .limit(1)

            stmt = select(func.concat(Subject.title, func.chr(9), stmt1)) \
                   .select_from(Subject)

            await logger.info(f"{os.linesep}*** SQL: ***{os.linesep}"
                              f"{str(stmt)}{os.linesep}")
            result = await session.execute(stmt)

            await logger.info("2. Знайти студента із найвищою середньою "
                              "оцінкою з кожного певного предмета." + os.linesep +
                              "Якщо оцінка однакова - найвищою вважається середня "
                              "оцінка з найбільшої кількості оцінок.")
            for r in result:
                await logger.info("%30s : %25s = %-s" % tuple(r[0].split(chr(9))))

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

    await select_02(async_session)

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
