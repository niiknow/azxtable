import test from 'ava';
import underTest from '.';

test('insert or update and delete', t => {
  const testKey = 'test' + (new Date()).getTime();
  const req = {
    pathParams: {
      tableName: 'aztabletest',
      id: testKey
    },
    queryString: {},
    body: {
      value: 'hi'
    }
  };

  return underTest.itemUpdate(req).then(body1 => {
    return underTest.itemDelete(req).then(body2 => {
      t.is(body2.errors.length, 0);
      t.is(body1.errors.length, 0);
    });
  });
});

test('batchCsv', t => {
  const testKey = 'test' + (new Date()).getTime();
  const req = {
    pathParams: {
      tableName: 'aztabletest',
      id: testKey
    },
    queryString: {
      headers: 'Id,v2,v3'
    },
    body: 'a,b,c\nd,e,f\ng,h,i\nj,k,l\nm,n,o'
  };

  return underTest.batchCsv(req).then(body => {
    t.not(body, undefined);
  });
});

test('should query', t => {
  const req = {
    pathParams: {
      tableName: 'aztabletest'
    },
    queryString: {
      $filter: 'PartitionKey eq \'_default\'',
      $top: 2,
      $select: 'Id,v2'
    }
  };

  return underTest.query(req).then(body => {
    t.is(body.items.length, 2);
  });
});
