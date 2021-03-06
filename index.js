const AzureTable = require('azure-table-node');
const csv = require('csvtojson');

function parseConnectionString(cstr) {
  const parts = cstr.split(';');
  const dict = {};
  parts.forEach(item => {
    const kv = item.split('=');
    dict[kv[0]] = kv[1];
  });
  if (dict.TableEndpoint) {
    if (!/\/+$/.test(dict.TableEndpoint)) {
      dict.TableEndpoint += '/';
    }
  }

  return dict;
}

function createTablePromise(myClient, tableName) {
  return new Promise(resolve => {
    myClient.createTable(tableName, resolve);
  });
}

function batchCommit(myClient) {
  return new Promise(resolve => {
    myClient.commit((err, data) => {
      resolve([err, data]);
    });
  });
}

function doQuery(myClient, tableName, opts) {
  return new Promise(resolve => {
    const handleQueryResult = (err, data, continuation) => {
      // err is null
      // data contains the array of objects (entities)
      // continuation is undefined or two element array to be passed to next query
      const rst = {
        errors: []
      };
      if (err) {
        if (err.code === 'TableNotFound') {
          createTablePromise(myClient, tableName).then(() => {
            myClient.queryEntities(tableName, opts, handleQueryResult);
          });
          return;
        }
        rst.errors = [err];
      } else {
        if (data) {
          rst.items = data;
          rst.count = data.length;
        }
        if (continuation && continuation.length > 1) {
          rst.nextpk = continuation[0];
          rst.nextrk = continuation[1];
        }
      }

      // console.log( 'query data:', rst );
      resolve(rst);
    };

    myClient.queryEntities(tableName, opts, handleQueryResult);
  });
}

class AzxTable {
  constructor(opts) {
    opts = opts || {};
    opts.azxtable = opts.azxtable || process.env.azxtable;
    opts.azxtable2 = opts.azxtable2 || process.env.azxtable2;
    opts.envCode = opts.envCode || process.env.envCode || 'prd';
    opts.tenantCode = opts.tenantCode || process.env.tenantCode || 'a';

    this.options = opts;

    // handle ac1.SharedAccessSignature property?
    const ac1 = parseConnectionString(opts.azxtable);
    this.azxclient = AzureTable.createClient({
      accountUrl: ac1.TableEndpoint || `https://${ac1.AccountName}.table.core.windows.net/`,
      accountName: ac1.AccountName,
      accountKey: ac1.AccountKey
    });

    if (opts.azxtable2) {
      const ac2 = parseConnectionString(opts.azxtable2);
      this.azxclient2 = AzureTable.createClient({
        accountUrl: ac2.TableEndpoint || `https://${ac2.AccountName}.table.core.windows.net/`,
        accountName: ac2.AccountName,
        accountKey: ac2.AccountKey
      });
    }
  }

  getParams(opts) {
    opts = opts || {};
    const params = {
      table: String(opts.table),
      tenantCode: opts.tenantCode || this.options.tenantCode,
      pk: opts.pk || '_default',
      envCode: (opts.envCode || this.options.envCode).toUpperCase(),
      body: opts.body,
      $filter: opts.$filter,
      $top: opts.$top,
      $select: opts.$select,
      delimiter: opts.delimiter,
      headers: opts.headers,
      nextpk: opts.nextpk,
      nextrk: opts.nextrk,
      rk: opts.rk,
      idfield: opts.idfield || 'Id'
    };

    if (params.table.indexOf(',') > 0) {
      const tables = params.table.split(',');
      params.table = tables[0].trim();
      if (tables[1]) {
        params.table2 = tables[1].trim();
        params.tableName2 = `${params.tenantCode}${params.envCode}${params.table2}`;
      }
    }

    params.tableName = `${params.tenantCode}${params.envCode}${params.table}`;
    return params;
  }

  createTableIfNotExists(tableName) {
    return new Promise(resolve => {
      // since this is batch, we just create table
      // console.log('creating table1:', tableName);
      const q1 = createTablePromise(this.azxclient, tableName);
      if (this.azxclient2) {
        // console.log('creating table2:', tableName);
        const q2 = createTablePromise(this.azxclient2, tableName);
        return Promise.all([q1, q2]).then(resolve);
      }
      q1.then(resolve);
    });
  }

  batchJsonRaw(body, params, resolve) {
    const azxclientBatch = this.azxclient.startBatch();
    const azxclientBatch2 = this.azxclient2 ? this.azxclient2.startBatch() : null;

    body.errors = body.errors || [];
    body.items = body.items || [];

    // validate table name
    if (!/^[a-z][a-zA-Z0-9]{2,62}$/.test(params.tableName)) {
      body.errors.push({
        message: `invalid tableName ${params.tableName} value`
      });
    }

    // validate partition key
    if (!/[a-zA-Z0-9-_.~,! ]+/.test(params.pk)) {
      body.errors.push({
        message: `invalid PartitionKey ${params.pk} value`
      });
    }

    // valid items
    if (body.items.length <= 0) {
      body.errors.push({
        message: `items array is required`
      });
    }

    // only batch of 100
    if (body.items.length > 100) {
      body.errors.push({
        message: 'expected items count to be less than 100 but got' + body.items.length
      });
    }

    // no error
    if (body.errors <= 0) {
      // validate and prep each row
      body.items.forEach((item, i) => {
        item.PartitionKey = params.pk;
        // console.log( 'one', item );
        if (!item.RowKey) {
          item.RowKey = item[params.idfield] || item.GTIN14 || item.UPC;
          // console.log('two', params.idfield, item);
        }

        if (!item.RowKey || !/[a-zA-Z0-9-_.~,! ]+/.test(item.RowKey)) {
          body.errors.push({
            message: `${i} has invalid RowKey/Id ${item.RowKey}`
          });
          item.RowKey = null;
        }
        // no error
        if (item.RowKey) {
          if (item.delete) {
            item.__etag = '*';
            azxclientBatch.deleteEntity(params.tableName, item);
            if (azxclientBatch2) {
              azxclientBatch2.deleteEntity(params.tableName, item);
            }
          } else {
            // console.log( 'insert', item );
            azxclientBatch.insertOrMergeEntity(params.tableName, item);
            if (azxclientBatch2) {
              azxclientBatch2.insertOrMergeEntity(params.tableName, item);
            }
          }
        }
      });

      // commit if no errors
      if (body.errors <= 0) {
        const handleCommit = (err, data) => {
          if (err) {
            console.log('batch result err:', err);
            body.errors.push({
              message: JSON.stringify(err)
            });
          }
          if (data) {
            // console.log( 'batch result data:', data );
            params.etags = data;
          }
          params.errors = body.errors;
          resolve(params);
        };

        if (this.azxclientBatch2) {
          const q1 = batchCommit(azxclientBatch);
          const q2 = batchCommit(azxclientBatch2);
          return Promise.all([q1, q2]).then(values => {
            handleCommit(values[0][0], values[0][1]);
          });
        }

        return azxclientBatch.commit(handleCommit);
      }
    }

    console.log('batch error:', body);
    // if it reach this point, it must have errored
    resolve(body);
  }

  batchJson(opts) {
    return new Promise(resolve => {
      const params = this.getParams(opts);
      // since this is batch, we just create table
      this.createTableIfNotExists(params.tableName).then(() => {
        this.batchJsonRaw(params.body, params, resolve);
      });
    });
  }

  batchCsv(opts) {
    return new Promise(resolve => {
      const parsedBody = {
        items: []
      };
      const params = this.getParams(opts);
      const csvopts = {
        noheader: false,
        delimiter: params.delimiter || ','
      };
      if (params.headers) {
        csvopts.headers = params.headers.split(',');
        csvopts.noheader = true;
      }

      this.createTableIfNotExists(params.tableName).then(() => {
        // parse csv
        csv(csvopts).fromString(params.body).on('json', jsonObj => {
          parsedBody.items.push(jsonObj);
        }).on('done', () => {
          // resolve here
          this.batchJsonRaw(parsedBody, params, resolve);
        });
      });
    });
  }

  query(opts) {
    return new Promise(resolve => {
      const params = this.getParams(opts);
      const localopts = {
        query: params.$filter,
        limitTo: params.$top
      };
      if (params.$select) {
        localopts.onlyFields = params.$select.split(',');
      }
      if (params.nextpk && params.nextrk) {
        localopts.continuation = [params.nextpk, params.nextrk];
      }
      const q1 = doQuery(this.azxclient, params.tableName, localopts);

      // also query from tableName2
      if (params.tableName2) {
        // use alternative account for performance, if available
        const q2 = doQuery(this.azxclient2 || this.azxclient, params.tableName2, opts);

        return Promise.all([q1, q2]).then(values => {
          // console.log(values);
          values[0].azxclient2 = values[1];
          resolve(values[0]);
        });
      }

      return Promise.all([q1]).then(values => {
        // console.log(values);
        resolve(values[0]);
      });
    });
  }

  itemUpdate(opts) {
    return new Promise(resolve => {
      const params = this.getParams(opts);
      // validate ids
      const item = params.body || {};
      item.PartitionKey = params.pk;
      item.RowKey = params.rk || item.RowKey || item.Id || item.GTIN14 || item.UPC;

      const handleResult = (err, cb) => {
        const rst = {
          errors: []
        };
        if (err) {
          if (err.code === 'TableNotFound') {
            this.createTableIfNotExists(params.tableName).then(() => {
              return this.azxclient.insertOrMergeEntity(params.tableName, item, err => handleResult(err, cb));
            });
            return;
          }
          rst.errors = [err];
        }
        cb(rst);
      };

      // console.log( item );
      if (this.azxclient2) {
        const q1 = new Promise(resolve => {
          this.azxclient.insertOrMergeEntity(params.tableName, item, err => handleResult(err, resolve));
        });
        const q2 = new Promise(resolve => {
          this.azxclient2.insertOrMergeEntity(params.tableName, item, err => handleResult(err, resolve));
        });
        return Promise.all([q1, q2]).then(values => {
          // console.log(values);
          values[0].azxclient2 = values[1];
          resolve(values[0]);
        });
      }
      this.azxclient.insertOrMergeEntity(params.tableName, item, err => handleResult(err, resolve));
    });
  }

  itemDelete(opts) {
    return new Promise(resolve => {
      const params = this.getParams(opts);
      // validate ids
      const item = {
        PartitionKey: params.pk,
        RowKey: params.rk,
        __etag: '*'
      };
      const handleResult = (err, cb) => {
        const rst = {
          errors: []
        };
        if (err) {
          if (err.code !== 'TableNotFound') {
            rst.errors.push(err);
          }
          rst.message = 'failed';
        }
        cb(rst);
      };

      if (this.azxclient2) {
        const q1 = new Promise(resolve => {
          this.azxclient.deleteEntity(params.tableName, item, err => handleResult(err, resolve));
        });
        const q2 = new Promise(resolve => {
          this.azxclient2.deleteEntity(params.tableName, item, err => handleResult(err, resolve));
        });
        return Promise.all([q1, q2]).then(values => {
          // console.log(values);
          values[0].azxclient2 = values[1];
          resolve(values[0]);
        });
      }

      this.azxclient.deleteEntity(params.tableName, item, err => handleResult(err, resolve));
    });
  }
}

module.exports = AzxTable;
