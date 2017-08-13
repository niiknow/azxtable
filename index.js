const AzureTable = require('azure-table-node');
const csv = require('csvtojson');

const ac1 = process.env.aztable.split(':');
const defaultClient = AzureTable.createClient({
  accountUrl: `http://${ac1[0]}.table.core.windows.net/`,
  accountName: ac1[0],
  accountKey: ac1[1]
});

/* let altClient;

if (process.env.aztablealt) {
  const ac2 = process.env.aztablealt.split(':');

  altClient = AzureTable.createClient({
    accountUrl: `http://${ac2[0]}.table.core.windows.net/`,
    accountName: ac2[0],
    accountKey: ac2[1]
  });
} */

function getParams(request) {
  request.queryString = request.queryString || {};
  request.env = request.env || {};
  const params = {
    originalName: request.pathParams.tableName,
    tenantCode: request.queryString.tenantCode || request.env.tenantCode || 'a',
    partitionKey: request.queryString.partitionKey || '_default',
    envCode: (request.env.envCode || 'prd').toUpperCase()
  };

  if (!params.originalName && request.queryString.table) {
    const tables = request.queryString.table.split(',');
    params.originalName = tables[0];
    if (tables[1]) {
      params.originalName2 = tables[1];
      params.tableName2 = `${params.tenantCode}${params.envCode}${params.originalName2}`;
    }
  }
  params.tableName = `${params.tenantCode}${params.envCode}${params.originalName}`;
  return params;
}

function handleError(error, tableName, tableClient) {
  console.log(error);
  return new Promise((resolve, reject) => {
    if (error) {
      if (error.code === 'TableNotFound') {
        console.log('creating table:', tableName);
        tableClient.createTable(tableName, true, err => {
          if (err) {
            console.log(err);
            return reject(err);
          }
          // created table
          resolve();
        });
        return;
      }
    }
    reject();
  });
}

function batchJsonRaw(body, params, resolve) {
  const batchClient = defaultClient.startBatch();
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
          batchClient.deleteEntity(params.tableName, item);
        } else {
          // console.log( 'insert', item );
          batchClient.insertOrMergeEntity(params.tableName, item);
        }
      }
    });
    // commit if no errors
    if (body.errors <= 0) {
      //  console.log( 'enter' );
      batchClient.commit((err, data) => {
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

function batchJson(request) {
  return new Promise(resolve => {
    const params = getParams(request);
    // since this is batch, we just create table
    defaultClient.createTable(params.tableName, true, () => {
      batchJsonRaw(request.body, params, resolve);
    });
  });
}

function batchCsv(request) {
  return new Promise(resolve => {
    const body = {
      items: []
    };
    const params = getParams(request);
    const opts = {
      noheader: false,
      delimiter: request.queryString.delimiter || ','
    };
    if (request.queryString.headers) {
      opts.headers = request.queryString.headers.split(',');
      opts.noheader = true;
      // console.log( 'opts', opts );
    }
    // since this is batch, we just create table
    defaultClient.createTable(params.tableName, true, () => {
      // parse csv
      csv(opts).fromString(request.body).on('json', jsonObj => {
        body.items.push(jsonObj);
      }).on('done', () => {
        // resolve here
        batchJsonRaw(body, params, resolve);
      });
    });
  });
}

function doQuery(client, tableName, opts) {
  return new Promise(resolve => {
    client.queryEntities(tableName, opts, (err, data, continuation) => {
      // err is null
      // data contains the array of objects (entities)
      // continuation is undefined or two element array to be passed to next query
      const rst = {};
      if (err) {
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

function query(request) {
  return new Promise(resolve => {
    const params = getParams(request);
    const opts = {
      query: request.queryString.$filter,
      limitTo: request.queryString.$top
    };
    if (request.queryString.$select) {
      opts.onlyFields = request.queryString.$select.split(',');
    }
    if (request.queryString.nextpk && request.queryString.nextrk) {
      opts.continuation = [request.queryString.nextpk, request.queryString.nextrk];
    }
    const q1 = doQuery(defaultClient, params.tableName, opts);
    /*
    if (params.tableName2) {
      const q2 = doQuery(defaultClient, params.tableName2, opts);
    } */
    Promise.all([q1]).then(values => {
      // console.log(values);
      resolve(values[0]);
    });
  });
}

function itemUpdate(request) {
  return new Promise(resolve => {
    const params = getParams(request);
    // validate ids
    const item = request.body || {};
    item.PartitionKey = params.partitionKey;
    item.RowKey = request.pathParams.id || item.RowKey || item.Id || item.GTIN14 || item.UPC;
    // console.log( item );
    defaultClient.insertOrMergeEntity(params.tableName, item, err => {
      const rst = {
        errors: []
      };
      if (err) {
        handleError(err, params.tableName, defaultClient);
        rst.errors = [err];
      }
      resolve(rst);
    });
  });
}

function itemDelete(request) {
  return new Promise(resolve => {
    const params = getParams(request);
    // validate ids
    const item = {
      PartitionKey: params.partitionKey,
      RowKey: request.pathParams.id,
      __etag: '*'
    };
    defaultClient.deleteEntity(params.tableName, item, err => {
      const rst = {
        errors: []
      };
      if (err) {
        handleError(err, params.tableName, defaultClient);
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
