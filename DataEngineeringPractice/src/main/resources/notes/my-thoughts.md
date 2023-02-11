**Major Factors to consider** 
- Storage Space
- Network I/O
- Compute Power

**Predicate Push-down** - Filters applied at Storage Layer    
**Bloom Filters** -  Probabilistic algorithm which says if element exists.  
**Compression** - To avoid High Storage and Network cost  
**Encryption** - Secure Sensitive data  
**Partitions**  - Store the data based on common columns ( Cardinality must be low ) (One Partition Per Country)  
**Bucketing**   - Store the data into fixed buckets(hash_function(bucketing_column) % num_buckets) (Split into X States in each country)  
**File Formats** -   Row vs Columnar | Avro vs Parquet vs ORC |   
**Join Optimizations**  
    - Small tables must come as early as possible in Join  
    - Ordering of ON condition hardly matters  
    - Don't apply functions on join participated columns  
    - Avoid User Defined Functions  
    - Prefer Equi Join over non-Equi Join  

**Join Types**  
```roomsql

    -- Map Join / Broadcast Join
    select /*+ MAPJOIN(b) */ a.id, a.name, b.name from table_1 a join table_2 b on a.id=b.id
    

```

**Skewness**  
    - Separate Null vs Non-Null and then Union  
    - Find Skey Column and split based on that column vs other and then Union  
    - Split Overall Query into small parts and then Union  
    - 


    
    
