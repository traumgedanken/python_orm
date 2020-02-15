import logging

import columns
import models

logging.basicConfig(level=logging.DEBUG)


class Student(models.Model):
    __tablename__ = 'student'
    st_id = columns.IntegerColumn(primary_key=True)
    name = columns.StringColumn(nullable=False, length=20)
    age = columns.IntegerColumn()


logging.debug(Student.insert(name='Pavlo'))
logging.debug(Student.insert(name='Petro', age=52))

logging.debug('Updating:')
st = Student.insert(st_id=40, name="Vasyl", age=25)
st.name = 'New Vasyl'

logging.debug('Getting many:')
for s in Student.get_many(Student.age > 20):
    logging.debug(s)

logging.debug('Getting one:')
logging.debug(Student.get_one(Student.name == 'Pavlo'))

logging.debug('Deleting all:')
Student.delete(Student.st_id > 0)
for s in Student.get_many():
    logging.debug(s)

# Student.commit()