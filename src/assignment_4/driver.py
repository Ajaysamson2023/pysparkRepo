from src.assignment_4.utils import *

spark = sparkSession()
dataframe = create_dataframe(spark)
dataframe.show()

first_dept_row = department_row(dataframe)
first_dept_row.show()

highest_salary = highest_salary(dataframe)
highest_salary.show()

employee_salary = employee_salary(dataframe)
employee_salary.show()


