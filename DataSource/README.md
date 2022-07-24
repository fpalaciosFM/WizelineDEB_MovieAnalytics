# Datasources

The datasource consists of the following three csv files:
- [user_purchase.csv](https://drive.google.com/file/d/1rqmnKgl_HXOfM7p4G_RC5v4clvbmJvks/view) (id = 1rqmnKgl_HXOfM7p4G_RC5v4clvbmJvks)
- [movie_review.csv](https://drive.google.com/file/d/1eh_0vzQGWnUZm5OH8j5M1D88he0uHILi/view) (id = 1eh_0vzQGWnUZm5OH8j5M1D88he0uHILi)
- [log_reviews.csv](https://drive.google.com/file/d/1UVKS9V2PAQKvyBwkaMl2wEYa3zgL-GlH/view) (id = 1UVKS9V2PAQKvyBwkaMl2wEYa3zgL-GlH)

The datasrouce [user_purchase.csv](https://drive.google.com/file/d/1rqmnKgl_HXOfM7p4G_RC5v4clvbmJvks/view) must be initially stored in a postgresql table, the sql script that creates this table is:

```sql
CREATE SCHEMA <schema_name>;
CREATE TABLE <schema_name>.user_purchase (
   invoice_number varchar(10),
   stock_code varchar(20),
   detail varchar(1000),
   quantity int,
   invoice_date timestamp,
   unit_price numeric(8,3),
   customer_id int,
   country varchar(20)
);
```

The ```COPY``` command imports data from a csv file into a table:

```sql
SET datestyle='MDY';
COPY <schema_name>.user_purchase
FROM 'C:\<Path to csv file>\user_purchase.csv'
DELIMITER ','
CSV HEADER;
```
