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

// handle ac1.SharedAccessSignature property?
const ac1 = parseConnectionString(process.env.aztable);
const aztable = AzureTable.createClient({
  accountUrl: ac1.TableEndpoint || `https://${ac1.AccountName}.table.core.windows.net/`,
  accountName: ac1.AccountName,
  accountKey: ac1.AccountKey
});

let azxtable;

if (process.env.aztablealt) {
  const ac2 = parseConnectionString(process.env.azxtable);
  azxtable = AzureTable.createClient({
    accountUrl: ac2.TableEndpoint || `https://${ac2.AccountName}.table.core.windows.net/`,
    accountName: ac2.AccountName,
    accountKey: ac2.AccountKey
  });
}

function getParams(opts) {
  opts = opts || {};
  const params = {
    table: String(opts.table),
    tenantCode: opts.tenantCode || process.env.tenantCode || 'a',
    partitionKey: opts.partitionKey || '_default',
    envCode: (process.env.envCode || 'prd').toUpperCase(),
    body: opts.body,
    $filter: opts.$filter,
    $top: opts.$top,
    $select: opts.$select,
    delimiter: opts.delimiter,
    headers: opts.headers,
    nextpk: opts.nextpk,
    nextrk: opts.nextrk,
    rowKey: opts.rowKey
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

function createTablePromise(myClient, tableName) {
  return new Promise(resolve => {
    myClient.createTable(tableName, true, resolve);
  });
}

function createTableIfNotExists(tableName) {
  return new Promise(resolve => {
    // since this is batch, we just create table
    console.log('creating table:', tableName);
    const q1 = createTablePromise(aztable, tableName);
    if (azxtable) {
      const q2 = createTablePromise(azxtable, tableName);
      return Promise.all([q1, q2]).then(resolve);
    }
    q1.then(resolve);
  });
}

function batchJsonRaw(body, params, resolve) {
  const aztableBatch = aztable.startBatch();
  const azxtableBatch = azxtable ? azxtable.startBatch() : null;

  body.errors = body.errors || [];
  body.items = body.items || [];
  // validate table name
  if (!/^[a-z][a-zA-Z0-9]{2,62}$/.test(params.tableName)) {
    body.errors.push({
      message: `invalid tableName ${params.tableName} value`
    });
  }
  // validate partition key
  if (!/[a-zA-Z0-9-_.~,! ]+/.test(params.partitionKey)) {
    body.errors.push({
      message: `invalid PartitionKey ${params.partitionKey} value`
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
  // console.log( 'body', body );
  // no error
  if (body.errors <= 0) {
    // validate and prep each row
    body.items.forEach((item, i) => {
      item.PartitionKey = params.partitionKey;
      // console.log( 'one', item );
      if (!item.RowKey) {
        body.idField = body.idField || 'Id';
        item.RowKey = item[body.idField] || item.GTIN14 || item.UPC;
        // console.log( 'two', item );
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
          aztableBatch.deleteEntity(params.tableName, item);
          if (azxtableBatch) {
            azxtableBatch.deleteEntity(params.tableName, item);
          }
        } else {
          // console.log( 'insert', item );
          aztableBatch.insertOrMergeEntity(params.tableName, item);
          if (azxtableBatch) {
            azxtableBatch.insertOrMergeEntity(params.tableName, item);
          }
        }
      }
    });
    // commit if no errors
    if (body.errors <= 0) {
      if (azxtableBatch) {
        aztableBatch.commit(err => {
          if (err) {
            console.log('batchx result err:', err);
            body.errors.push({
              message: JSON.stringify(err)
            });
          }

          // do nothing
        });
      }

      aztableBatch.commit((err, data) => {
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
      });
      return;
    }
  }
  // if it reach this point, it must have errored
  resolve(body);
}

function batchJson(opts) {
  return new Promise(resolve => {
    const params = getParams(opts);
    // since this is batch, we just create table
    createTableIfNotExists(params.tableName).then(() => {
      batchJsonRaw(params.body, params, resolve);
    });
  });
}

function batchCsv(opts) {
  return new Promise(resolve => {
    const parsedBody = {
      items: []
    };
    const params = getParams(opts);
    const csvopts = {
      noheader: false,
      delimiter: params.delimiter || ','
    };
    if (csvopts.headers) {
      csvopts.headers = params.headers.split(',');
      csvopts.noheader = true;
    }

    createTableIfNotExists(params.tableName).then(() => {
      // parse csv
      csv(csvopts).fromString(params.body).on('json', jsonObj => {
        parsedBody.items.push(jsonObj);
      }).on('done', () => {
        // resolve here
        batchJsonRaw(parsedBody, params, resolve);
      });
    });
  });
}

function doQuery(myClient, tableName, opts) {
  return new Promise(resolve => {
    myClient.queryEntities(tableName, opts, (err, data, continuation) => {
      // err is null
      // data contains the array of objects (entities)
      // continuation is undefined or two element array to be passed to next query
      const rst = {
        errors: []
      };
      if (err) {
        if (err.code === 'TableNotFound') {
          createTableIfNotExists(tableName).then(() => {
            rst.errors = [{
              code: 'retry',
              message: 'service temporarily unavailable, please try again'
            }];
            resolve(rst);
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
    });
  });
}

function query(opts) {
  return new Promise(resolve => {
    const params = getParams(opts);
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
    const q1 = doQuery(aztable, params.tableName, localopts);

    // also query from tableName2
    if (params.tableName2) {
      // use alternative account for performance, if available
      const q2 = doQuery(azxtable || aztable, params.tableName2, opts);

      return Promise.all([q1, q2]).then(values => {
        // console.log(values);
        values[0].azxtable = values[1];
        resolve(values[0]);
      });
    }

    return Promise.all([q1]).then(values => {
      // console.log(values);
      resolve(values[0]);
    });
  });
}

function itemUpdate(opts) {
  return new Promise(resolve => {
    const params = getParams(opts);
    // validate ids
    const item = params.body || {};
    item.PartitionKey = params.partitionKey;
    item.RowKey = params.rowKey || item.RowKey || item.Id || item.GTIN14 || item.UPC;

    // console.log( item );
    aztable.insertOrMergeEntity(params.tableName, item, err => {
      const rst = {
        errors: []
      };
      if (err) {
        if (err.code === 'TableNotFound') {
          createTableIfNotExists(params.tableName).then(() => {
            rst.errors = [{
              code: 'retry',
              message: 'service temporarily unavailable, please try again'
            }];
            resolve(rst);
          });
          return;
        }
        rst.errors = [err];
      }
      resolve(rst);
    });
  });
}

function itemDelete(opts) {
  return new Promise(resolve => {
    const params = getParams(opts);
    // validate ids
    const item = {
      PartitionKey: params.partitionKey,
      RowKey: params.rowKey,
      __etag: '*'
    };
    aztable.deleteEntity(params.tableName, item, err => {
      const rst = {
        errors: []
      };
      if (err) {
        if (err.code === 'TableNotFound') {
          createTableIfNotExists(params.tableName).then(() => {
            rst.errors = [{
              code: 'retry',
              message: 'service temporarily unavailable, please try again'
            }];
            resolve(rst);
          });
          return;
        }
        rst.errors = [err];
        rst.message = 'failed';
      }
      resolve(rst);
    });
  });
}

module.exports = {
  batchCsv: batchCsv,
  batchJson: batchJson,
  query: query,
  itemUpdate: itemUpdate,
  itemDelete: itemDelete
};
