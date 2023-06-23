from src.assignment_3.utils import *

spark = sparkSession()
product_dataframe = create_dataframe(spark)
product_dataframe.show()

Total_amount_pivot = total_amount_pivot(product_dataframe)
Total_amount_pivot.show()


unpivot_country = unpivot_dataframe(product_dataframe)
unpivot_country.show()
