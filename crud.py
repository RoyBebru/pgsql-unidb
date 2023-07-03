#!/usr/bin/env python3

from __future__ import annotations


import asyncio
from aiologger import Logger
from faker import Faker
import os
import platform
import random


# from sqlalchemy import select
# from sqlalchemy import update
from sqlalchemy import delete
from sqlalchemy.exc import IntegrityError, ObjectNotExecutableError
from sqlalchemy.exc import ProgrammingError, DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sqlalchemy import UniqueConstraint
from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship, backref


DBURL="postgresql+asyncpg://postgres:my-secret@127.0.0.1:5432/uni"
CONF_DGECHO = False


class Base(AsyncAttrs, DeclarativeBase):
    pass

#{{{ Database OOP Model

class Group(Base):
    __tablename__ = 'groups'
    id: Mapped[int] = mapped_column(primary_key=True)
    codename: Mapped[str] = mapped_column(String)
    __table_args__ = (UniqueConstraint(codename, name="group_codename"),)

class Student(Base):
    __tablename__ = 'students'
    id: Mapped[int] = mapped_column(primary_key=True)
    fullname: Mapped[str] = mapped_column(String)
    group_id: Mapped[int] = mapped_column('group_id', Integer,
                                          ForeignKey('groups.id',
                                                     ondelete="CASCADE"))
    # group = relationship("Group", cascade="all, delete",
    #                      backref=backref("student_groups", cascade="all, delete"))

#}}}

fake = Faker("uk_UA")

def excm(msg: str):
    return "{{{ " + "..... EXCEPTION ....." + os.linesep + msg + os.linesep + "}}}"

async def insert_fake_objects(async_session: async_sessionmaker[AsyncSession]) -> None:
    async with async_session() as session:
        try:
            async with session.begin():
                obj_list = []

                # Fake groups
                fake_groups = [ "GOIT-31", "TOGI-32", "TIGO-33" ]
                obj_list.extend([ Group(codename=gr) for gr in fake_groups ])

                # Fake Students
                num_of_students = random.randint(30, 50)
                fake_students = [ f"{fake.last_name()}, {fake.first_name()}"
                                  for _ in range(num_of_students) ]
                obj_list.extend([ Student(fullname=st,
                                          group_id=random.randint(1,len(fake_groups)))
                                  for st in fake_students ])
                # Add all to DB
                session.add_all(obj_list)

        except IntegrityError as e:
            await logger.error(excm(str(e)))


async def async_init() -> None:
    engine = create_async_engine(DBURL, echo=CONF_DGECHO)
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
    await engine.dispose()

async def do_del_group(session: AsyncSession, arg_list: list):
    group = " ".join(arg_list).split()
    group = " ".join(group)
    logger.info(f"Delete Group '{group}'")
    stmt = delete(Group).where(Group.codename == group)
    result = await session.execute(stmt)
    await logger.info(f"Deleted {result.rowcount} entry(-ies)")


async def async_do_smth() -> None:
    engine = create_async_engine(DBURL, echo=CONF_DGECHO,)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with async_session() as session:
            async with session.begin():

                await do_del_group(session, ["TIGO-33"])

    except (ObjectNotExecutableError, ProgrammingError, DBAPIError) as e:
        await logger.warning(excm(str(e)))
    except ConnectionRefusedError as e:
        await logger.error(excm(str(e)))
        return

    await engine.dispose()


if __name__ == "__main__":
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    logger = Logger.with_default_handlers(name='NoPrintLogger')
    asyncio.run(async_init())
    asyncio.run(async_do_smth())
