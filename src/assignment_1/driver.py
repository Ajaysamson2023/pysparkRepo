from src.assignment_1.utils import *

spark = spark_session()
df = create_dataframe(spark)
df.show()

time_format = time_stamp_format(df)
time_format.show(truncate=False)

date_type_format = date_type(time_format)
date_type_format.show(truncate=False)

remove_space = remove_extra_space(df)
remove_space.show(truncate=False)

replace_values = replace_values(time_format)
replace_values.show(truncate=False)

transform_df = transform_dataframe(spark)
transform_df.show(truncate=False)

add_column = add_column(transform_df)
add_column.show(truncate=False)

combine_df = combine_df(df, transform_df)
combine_df.show(truncate=False)

field_records = field_records(combine_df)
field_records.show(truncate=False)
