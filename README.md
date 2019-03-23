A minimal database core with the essential functionality of creation, retrieval, update, and deletion of records. The core is implemented in Python 3.

The indexes are B+ trees. The core manages pages with caching, uses Least-Recent-Used (LRU) as the default replacement policy, and supports pinning of a page, forbidding it to be replaced.

Everything related to the database is stored under `./schema`

`./schema/metadata.pickle` is the metadata of the database

All tables are stored under the **sub-directories** of `./schema/tables`. Every table and its index are stored in the same directory, with the name of the table as `XXX.table` and the name of the index as `XXX.index`.

All primary key indexes are called `PRIMARY.index`

For example, for table `spam` and index `spammer`, the files are organized as:

```
./schema/tables/spam/spam.table

./schema/tables/spam/spammer.index

./schema/tables/spam/PRIMARY.index
```
