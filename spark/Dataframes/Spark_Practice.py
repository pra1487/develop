"""
1. spark session creation
2. df creation
3. groupby, agg, collect_list, count, sum, alias
4. withColumn, withColumnRenamed, when-otherwise
5. null drop (how = 'any') (how = 'any', thresh=2) (how='any', subset=[])
6. null fill (value=0)(value='')
7. filters on df, select columns
8. withColumn coalesce cast
9. split, size, concat_ws 
10. distinct, dropDups, sort
11. empty df, StructType, lit, explode
12. pivot, union, regexp_replace, translate
13. avg, max, min, first, last, avg, count, sum, sum_distinct, count_distinct, approx_count_distinct, collect_list, collect_set
14. substring
15. to_timestamp, to_date, year, month, dayofmonth
16. translate, split
17. rename multiple columns
18. jdbc read from spark
19. window func, web api data process
"""

st = [('virat',60),('dhoni',40),('surya',65),('sachin',70)]

def func(li):
    if len(li)==0:
        return []
    else:
        max_marks = max(marks for name, marks in li)
        result = [(name, marks) for name, marks in li if marks==max_marks]
        return result
print(func(st))
