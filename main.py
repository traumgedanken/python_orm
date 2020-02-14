import columns
import models


class Student(models.Model):
    __tablename__ = 'student'
    st_id = columns.IntegerColumn(primary_key=True)


st = Student.insert(st_id=3)
