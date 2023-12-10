from sqlalchemy import func,desc,and_

from src.models import Teacher,Student,Discipline,Grade,Group
from src.db import session



def select_1():
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    print(result)
def select_2(discipline_id):
    result = session.query(Discipline.name,Student.fullname,func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                .select_from(Grade)\
                .join(Student)\
                .join(Discipline)\
                .filter(Discipline.id == discipline_id)\
                .group_by(Student.id, Discipline.name)\
                .order_by(desc('avg_grade'))\
                .limit(1).all()
    print(result)
def select_3(discipline_id):
    result = session.query(Discipline.name, func.round(func.avg(Grade.grade), 2) \
                          .label('avg_grade')) \
        .select_from(Grade) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Discipline.name) \
        .order_by(desc('avg_grade')) \
        .all()

    print(result)
def select_4():
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')).all()
    print(result)
def select_5(teachers_id):
    result = session.query(Discipline.name, Teacher.fullname).select_from(Teacher).join(Discipline).filter(Teacher.id==teachers_id).all()
    print(result)
def select_6(group_id):
    result = session.query(Student.fullname, Group.name).select_from(Student).join(Group).filter(Group.id==group_id).all()
    print(result)
def select7(group_id,discipline_id):
    result = session.query(Student.fullname,Group.name,Discipline.name,Grade.grade).select_from(Grade) \
        .join(Discipline) \
        .join(Student) \
        .join(Group)\
        .filter(and_(Discipline.id==discipline_id,Group.id==group_id))\
        .all()
    print(result)
def select8(teacher_id):
    result = session.query(Teacher.fullname,func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade)\
        .join(Discipline)\
        .join(Teacher)\
        .filter(Teacher.id == teacher_id) \
        .group_by(Teacher.fullname) \
        .all()
    print(result)
def select9(student_id):
    result = session.query(Student.fullname,Discipline.name).select_from(Grade).join(Discipline).join(Student).filter(Student.id==student_id)\
        .group_by(Student.fullname,Discipline.name)\
        .order_by(Student.fullname)\
        .all()
    print(result)
def select10(student_id,teacher_id):
    result = session.query(Discipline.name,Student.fullname,Student.fullname).select_from(Grade).join(Discipline).join(Student).join(Teacher)\
        .filter(and_(Student.id==student_id,Teacher.id == teacher_id)) \
        .group_by(Student.fullname, Discipline.name, Teacher.fullname) \
        .order_by(Discipline.name) \
        .all()
    print(result)
if __name__ == '__main__':
    select_1()
    select_2(1)
    select_3(1)
    select_4()
    select_5(3)
    select_6(2)
    select7(1,3)
    select8(3)
    select9(2)
    select10(2,4)