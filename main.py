import logging

import columns
import models

logging.basicConfig(level=logging.DEBUG)


class Student(models.Model):
    __tablename__ = 'student'
    st_id = columns.IntegerColumn(primary_key=True)
    name = columns.StringColumn(nullable=False)


st = Student.insert(name='kek')

Student.commit()