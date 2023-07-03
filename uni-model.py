from __future__ import annotations

from sqlalchemy import UniqueConstraint, CheckConstraint
from sqlalchemy import ForeignKey, Integer, SmallInteger, String, Date
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship, backref


class Base(AsyncAttrs, DeclarativeBase):
    pass

#{{{ Database OOP Model

class Subject(Base):
    """
    CREATE TABLE subjects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title VARCHAR(255) UNIQUE NOT NULL
    );
    """
    __tablename__ = 'subjects'
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String)
    __table_args__ = (UniqueConstraint(title, name="subject_title"),)


class Group(Base):
    """
    CREATE TABLE groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codename VARCHAR(255) UNIQUE NOT NULL
    );
    """
    __tablename__ = 'groups'
    id: Mapped[int] = mapped_column(primary_key=True)
    codename: Mapped[str] = mapped_column(String)
    __table_args__ = (UniqueConstraint(codename, name="group_codename"),)


class Teacher(Base):
    """
    CREATE TABLE teachers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fullname VARCHAR(255) UNIQUE NOT NULL
    );
    """
    __tablename__ = 'teachers'
    id: Mapped[int] = mapped_column(primary_key=True)
    fullname: Mapped[str] = mapped_column(String)
    __table_args__ = (UniqueConstraint(fullname, name="teacher_fullname"),)


class Student(Base):
    """
    CREATE TABLE students (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fullname VARCHAR(255) UNIQUE NOT NULL,
        group_id INTEGER,
        FOREIGN KEY (group_id) REFERENCES groups (id)
            ON DELETE SET NULL
            ON UPDATE CASCADE
    );
    """
    __tablename__ = 'students'
    id: Mapped[int] = mapped_column(primary_key=True)
    fullname: Mapped[str] = mapped_column(String)
    group_id: Mapped[int] = mapped_column('group_id', Integer,
                                          ForeignKey('groups.id', ondelete="CASCADE"))
    group = relationship("Group", cascade="all, delete",
                         backref=backref("student_groups", cascade="all, delete"))

class TeacherSubject(Base):
    """
    CREATE TABLE teacher_subjects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        teacher_id INTEGER NOT NULL,
        subject_id INTEGER NOT NULL,
        FOREIGN KEY (teacher_id) REFERENCES teachers (id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
        FOREIGN KEY (subject_id) REFERENCES subjects (id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
    );
    """
    __tablename__ = 'teacher_subjects'
    id: Mapped[int] = mapped_column(primary_key=True)
    teacher_id: Mapped[int] = mapped_column('teacher_id'
                                            , Integer
                                            , ForeignKey('teachers.id',
                                                         ondelete="CASCADE"))
    teacher = relationship("Teacher", cascade="all, delete",
                           backref=backref("teacher_subject_teachers",
                                           cascade="all, delete") )
    subject_id: Mapped[int] = mapped_column('subject_id'
                                            , Integer
                                            , ForeignKey('subjects.id',
                                                         ondelete="CASCADE"))
    subject = relationship("Subject", cascade="all, delete",
                         backref=backref("teacher_subject_subjects",
                                         cascade="all, delete"))


class StudentSubject(Base):
    """
    CREATE TABLE student_subjects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER NOT NULL,
        subject_id INTEGER NOT NULL,
        FOREIGN KEY (student_id) REFERENCES students (id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
        FOREIGN KEY (subject_id) REFERENCES subjects (id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
    );
    """
    __tablename__ = 'student_subjects'
    id: Mapped[int] = mapped_column(primary_key=True)
    student_id: Mapped[int] = mapped_column('student_id'
                                            , Integer
                                            , ForeignKey('students.id',
                                                         ondelete="CASCADE"))
    student = relationship("Student", cascade="all, delete",
                           backref=backref("student_subject_students",
                                           cascade="all, delete"))
    subject_id: Mapped[int] = mapped_column('subject_id'
                                            , Integer
                                            , ForeignKey('subjects.id',
                                                         ondelete="CASCADE"))
    subject = relationship("Subject", cascade="all, delete",
                           backref=backref("student_subject_subjects",
                                           cascade="all, delete"))


class Grade(Base):
    """
    CREATE TABLE grades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date_of DATE NOT NULL,
        grade TINYINT NOT NULL,
        student_id INTEGER NOT NULL,
        subject_id INTEGER NOT NULL,
        teacher_id INTEGER NOT NULL,
        FOREIGN KEY (student_id) REFERENCES students (id)
        FOREIGN KEY (subject_id) REFERENCES subjects (id)
        FOREIGN KEY (teacher_id) REFERENCES teachers (id)
    );
    """
    __tablename__ = 'grades'
    id: Mapped[int] = mapped_column(primary_key=True)
    date_of: Mapped[Date] = mapped_column(Date)
    grade: Mapped[int] = mapped_column(SmallInteger)
    student_id: Mapped[int] = mapped_column('student_id'
                                            , Integer
                                            , ForeignKey('students.id'))
    student = relationship('Student', cascade="all, delete",
                           backref=backref("grade_students",
                                           cascade="all, delete"))
    subject_id: Mapped[int] = mapped_column('subject_id'
                                            , Integer
                                            , ForeignKey('subjects.id'))
    subject = relationship('Subject', cascade="all, delete",
                           backref=backref("grade_subjects",
                                           cascade="all, delete"))
    teacher_id: Mapped[int] = mapped_column('teacher_id'
                                            , Integer
                                            , ForeignKey('teachers.id'))
    teacher = relationship('Teacher', cascade="all, delete",
                           backref=backref("grade_teachers",
                                           cascade="all, delete"))
    __table_args__ = (CheckConstraint("2 <= grade AND grade <= 5"),)
