import pandas as pd
from pandasql import sqldf, load_births

births = load_births()
print(sqldf("SELECT * FROM births WHERE births > 250000 LIMIT 5;", locals()))

q = """
    SELECT date(date) as DOB, SUM(births) as "Total Births"
    FROM births
    GROUP BY date
    LIMIT 10;
    """
print(sqldf(q, locals())) 
print(sqldf(q, globals()))
#  if your DataFrame is only defined inside a function and does not exist in global scope, 
#  you need to pass in locals() return dictionary, If your DataFrame exists in global scope, 
#  you need to pass in result of globals() .

# define a function with local or global in it, so no need to call it everytime.
def pysqldf(q):
    return sqldf(q, globals())
print(pysqldf(q))

dfcustomer = pd.read_csv("DimCustomer.csv")
dfcustomer.head(3)
dfcustomer.dtypes
print(pysqldf("""select FirstName from dfcustomer limit 5"""))

dfinternetsales = pd.read_csv("FactInternetSales.csv")
dfinternetsales.head(3)

query = """SELECT * 
           FROM dfcustomer as c
           LEFT OUTER JOIN dfinternetsales as s
           ON c.CustomerKey = s.CustomerKey
           LIMIT 5"""

pysqldf(query)


