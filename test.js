import test from 'ava';
import m from '.';

test('insert or update and delete', t => {
  const testKey = 'test' + (new Date()).getTime();
  const opts = {
    table: 'aztabletest',
    rk: testKey,
    body: {
      value: 'hi'
    }
  };

  return m.itemUpdate(opts).then(body1 => {
    return m.itemDelete(opts).then(body2 => {
      t.is(body2.errors.length, 0);
      t.is(body1.errors.length, 0);
    });
  });
});

test('batchCsv', t => {
  const opts = {
    table: 'aztabletest',
    headers: 'Id,v2,v3',
    body: 'a,b,c\nd,e,f\ng,h,i\nj,k,l\nm,n,o'
  };

  return m.batchCsv(opts).then(body => {
    t.is(body.errors.length, 0);
  });
});

test('should query', t => {
  const req = {
    table: 'aztabletest',
    $filter: 'PartitionKey eq \'_default\'',
    $top: 2,
    $select: 'Id,v2'
  };

  return m.query(req).then(body => {
    t.is(body.items.length, 2);
  });
});
