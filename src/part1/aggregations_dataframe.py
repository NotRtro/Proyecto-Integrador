from pyspark.sql import functions as F

def category_amount(df):
    return df.groupBy('category').agg(F.sum('tender_amount_pen').alias('value'))

def buyer_count(df):
    return df.groupBy('buyer_id').agg(F.count('*').alias('value'))
