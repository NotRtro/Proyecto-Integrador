def category_amount(df):
    return df.select('category','tender_amount_pen').rdd.map(lambda r: (r[0],r[1] or 0.)).reduceByKey(lambda a,b:a+b)

def buyer_count(df):
    return df.select('buyer_id').rdd.map(lambda r: (r[0],1)).reduceByKey(lambda a,b:a+b)
