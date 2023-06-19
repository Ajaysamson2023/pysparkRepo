from src.assignment_2.utils import *

spark = spark_session()

employee_df = employee_df(spark)
employee_df.show(truncate=False)

select_column = select_column(employee_df)
select_column.show(truncate=False)

add_column = add_columns(employee_df)
add_column.show(truncate=False)

salary_column = salary_column(employee_df)
salary_column.show(truncate=False)

datatype_change = datatype_change(employee_df)
datatype_change.show(truncate=False)

new_salary_column = new_column(employee_df)
new_salary_column.show(truncate=False)

rename_column = rename_col(employee_df)
rename_column.show(truncate=False)

maximum_salary = maximum_salary(employee_df)
maximum_salary.show(truncate=False)

drop_column = column_drop(add_column)
drop_column.show(truncate=False)

distinct_value = distinct_value(add_column)
distinct_value.show(truncate=False)
