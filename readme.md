# azxtable
> azure node table helper with redundancy

Features:
- [x] multi-tenancy and environments
- [x] provide redundancy by untilizing two different storage accounts
- [x] enhance bulk import/batch operations
- [x] auto create table if not exists
- [x] utilizing redundancy to provide simultaneous query of two tables

Use Cases:
* host it on AWS Lambda or some nodejs serverless hosting
* tenant specific storage with redundancy
* bulk logging - buffer log to insert 100 at a time by simply calling an API
    * set table name as logYYYYMM
    * tenant specific logs
* time-series data - query multiple tables: table = 'logThisMonth,logLastMonth'


## Install

```
$ npm install azxtable
```

## Usage
```
import AzxTable from 'azxtable';
let azxtable = new AzxTable({
    azxtable: 'connection string', 
    azxtable2: 'connection string2'
});
```
## helpers
batchCsv, batchJson, query, itemUpdate, itemDelete

## options
```yml
{
    table: 'table1,table2',
    tenantCode: options.tenantCode || process.env.tenantCode || 'a',
    pk: options.pk || '_default',
    rk: options.rk,
    envCode: (options.envCode || process.env.envCode || 'prd').toUpperCase(),
    body: options.body,
    $filter: options.$filter,
    $top: options.$top,
    $select: options.$select,
    delimiter: options.delimiter,
    headers: options.headers,
    nextpk: options.nextpk,
    nextrk: options.nextrk,
    idfield: options.idfield || 'Id'
}
```


## MIT
