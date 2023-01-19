# etl-mal-airflow
ETL Pipeline to extract data from ![myanimelist.net](https://myanimelist.net/topmanga.php)

![schema_pipeline](./etl_mal_schema.png)


### Architecture des séries
```python
{
  _id: ObjectId(str),
  Rank: int,
  Title: str,
  URL: str,
  Alternative titles: Object,
  Information: Object,
  Statistics: Object,
  Characters: Array
}
```
