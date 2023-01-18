# etl-mal-airflow
ETL Pipeline to extract data from myanimelist.net


### Architecture des séries
```python
{
  _id: ObjectId(str),
  Rank: int,
  Title: str,
  URL: str,
  ALternative titles: Object,
  Information: Object,
  Statistics: Object,
  Characters: Array
}
```
