"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var _ = require('lodash');
var Promise = require('bluebird');
var chai = require('chai');
var expect = require('chai').expect;
var config = require('./config');
var mysql = require('mysql');
var createNodeMySQL = require('./mysql-wrap');
var self = this;
describe('mysqlWrap', function () {
    before(function (done) {
        self.stripIds = function (data) {
            return _.isArray(data) ? _.map(data, self.stripIds) : _.omit(data, 'id');
        };
        var pool = mysql.createPool(config.mysql);
        self.sql = createNodeMySQL(pool);
        pool.getConnection(function (err, conn) {
            if (err) {
                console.log(err, err.stack);
            }
            else {
                self.masterConn = conn;
                done();
            }
        });
    });
    beforeEach(function (done) {
        self.masterConn.query('TRUNCATE TABLE `table`', function (err, res) {
            self.a = { id: 1, unique: 'a', field: 'foo' };
            self.b = { id: 2, unique: 'b', field: 'bar' };
            self.c = { id: 3, unique: 'c', field: 'foo' };
            self.masterConn.query('INSERT INTO `table` (`unique`, `field`) ' +
                'VALUES ' +
                _.map([self.a, self.b, self.c], function (row) {
                    return '("' + row.unique + '", "' + row.field + '")';
                }).join(', '), function (err, res) {
                self.masterConn.query('TRUNCATE TABLE `table2`', function (err, res) {
                    self.masterConn.query('INSERT INTO `table2` (`field`) ' + 'VALUES ("bar")', function (err) { return done(); });
                });
            });
        });
    });
    describe('connection', function () {
        it('should get a single connection', function (done) {
            self.sql
                .connection()
                .then(function (c) {
                return Promise.all(_.map(_.range(2), function () { return c.query('SELECT CONNECTION_ID()'); })).then(function (resp) {
                    expect(resp[0]).to.deep.equal(resp[1]);
                    c.release();
                    done();
                });
            })
                .done();
        });
    });
    describe('build', function () {
        before(function (done) {
            self.rowsToEdges = function (rows, fields) {
                return _.map(rows, function (r) { return ({
                    node: r,
                    cursor: self.toCursor(r, fields || ['id']),
                }); });
            };
            self.toCursor = function (r, fields) {
                return new Buffer(_.map(_.isArray(fields) ? fields : [fields], function (f) { return String(r[f]); }).join('#')).toString('base64');
            };
            self.cursorFig = function (od) { return ({
                cursor: _.extend({ orderBy: 'id' }, od),
            }); };
            done();
        });
        it('should have cursor option', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                first: 100,
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false,
                    },
                    edges: self.rowsToEdges([self.a, self.b, self.c]),
                });
                done();
            })
                .done();
        });
        it('should handle orderBy with direction', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                first: 100,
                orderBy: { field: 'id', direction: 'DESC' },
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false,
                    },
                    edges: self.rowsToEdges([self.c, self.b, self.a]),
                });
                done();
            })
                .done();
        });
        it('should handle orderBy with serialization', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                first: 100,
                orderBy: {
                    field: 'id',
                    serialize: function (v) { return String(v + 1); },
                },
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false,
                    },
                    edges: _.map([self.a, self.b, self.c], function (r) { return ({
                        node: r,
                        cursor: self.toCursor({ id: r.id + 1 }, 'id'),
                    }); }),
                });
                done();
            })
                .done();
        });
        it('should handle orderBy with deserialization', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                first: 100,
                after: self.toCursor(self.a, 'id'),
                orderBy: {
                    field: 'id',
                    deserialize: function (v) { return Number(v) + 1; },
                },
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 1,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false,
                    },
                    edges: self.rowsToEdges([self.c]),
                });
                done();
            })
                .done();
        });
        it('should limit with "first" field', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                first: 1,
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: true,
                        hasPreviousPage: false,
                    },
                    edges: self.rowsToEdges([self.a]),
                });
                done();
            })
                .done();
        });
        it('should limit with the "last" field', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                last: 1,
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: true,
                    },
                    edges: self.rowsToEdges([self.c]),
                });
                done();
            })
                .done();
        });
        it('should enable next page selection with the "after" field', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                first: 100,
                after: self.toCursor(self.a, 'id'),
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false,
                    },
                    edges: self.rowsToEdges([self.b, self.c]),
                });
                done();
            })
                .done();
        });
        it('should enable previous page selection with the "before" field', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                last: 100,
                before: self.toCursor(self.c, 'id'),
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false,
                    },
                    edges: self.rowsToEdges([self.a, self.b]),
                });
                done();
            })
                .done();
        });
        it('should limit with "first" and "after"', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                first: 1,
                after: self.toCursor(self.a, 'id'),
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: true,
                        hasPreviousPage: false,
                    },
                    edges: self.rowsToEdges([self.b]),
                });
                done();
            })
                .done();
        });
        it('should limit with "last" and "after"', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                last: 1,
                after: self.toCursor(self.a, 'id'),
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: true,
                    },
                    edges: self.rowsToEdges([self.c]),
                });
                done();
            })
                .done();
        });
        it('should limit with "first" and "before"', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                first: 1,
                before: self.toCursor(self.c, 'id'),
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: true,
                        hasPreviousPage: false,
                    },
                    edges: self.rowsToEdges([self.a]),
                });
                done();
            })
                .done();
        });
        it('should limit with "last" and "before"', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                last: 1,
                before: self.toCursor(self.c, 'id'),
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: true,
                    },
                    edges: self.rowsToEdges([self.b]),
                });
                done();
            })
                .done();
        });
        it('should handle compound orderBy', function (done) {
            var orderBy = ['field', 'id'];
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                orderBy: orderBy,
                first: 100,
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false,
                    },
                    edges: self.rowsToEdges([self.b, self.a, self.c], orderBy),
                });
                done();
            })
                .done();
        });
        it('should handle compound orderBy with direction', function (done) {
            var orderBy = [{ field: 'field' }, { field: 'id', direction: 'DESC' }];
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                orderBy: orderBy,
                first: 100,
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false,
                    },
                    edges: self.rowsToEdges([self.b, self.c, self.a], _.map(orderBy, function (o) { return o.field; })),
                });
                done();
            })
                .done();
        });
        it('should handle compound orderBy with complex fig', function (done) {
            var orderBy = ['field', 'id'];
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                orderBy: ['field', 'id'],
                first: 2,
                before: self.toCursor(self.c, orderBy),
                after: self.toCursor(self.b, orderBy),
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 1,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false,
                    },
                    edges: self.rowsToEdges([self.a], orderBy),
                });
                done();
            })
                .done();
        });
        it('should handle compound orderBy with complex fig with direction', function (done) {
            var orderBy = [{ field: 'field' }, { field: 'id', direction: 'DESC' }];
            self.sql
                .build()
                .select()
                .from('`table`')
                .run(self.cursorFig({
                orderBy: orderBy,
                first: 2,
                before: self.toCursor(self.a, _.map(orderBy, function (o) { return o.field; })),
            }))
                .then(function (resp) {
                expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false,
                    },
                    edges: self.rowsToEdges([self.b, self.c], _.map(orderBy, function (o) { return o.field; })),
                });
                done();
            })
                .done();
        });
        it('should have whereIfDefined method', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .whereIfDefined('id = ?', undefined)
                .run()
                .then(function (resp) {
                expect(resp).to.have.deep.members([self.a, self.b, self.c]);
                return self.sql
                    .build()
                    .select()
                    .from('`table`')
                    .whereIfDefined('id = ?', 0)
                    .run();
            })
                .then(function (resp) {
                expect(resp).to.deep.equal([]);
                done();
            })
                .done();
        });
        it('should return query generator', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .where('field = ?', self.b.field)
                .run()
                .then(function (resp) {
                chai.assert.deepEqual(resp, [self.b]);
                done();
            })
                .done();
        });
        it('should be able to pass query options through "run" command', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .where('id = ?', self.b.id)
                .run({ resultCount: true })
                .then(function (resp) {
                chai.assert.deepEqual(resp, {
                    resultCount: 1,
                    results: [self.b],
                });
                done();
            })
                .done();
        });
        it('should be invokable through a "one" command', function (done) {
            self.sql
                .build()
                .select()
                .from('`table`')
                .where('id = ?', self.b.id)
                .one()
                .then(function (resp) {
                chai.assert.deepEqual(resp, self.b);
                done();
            })
                .done();
        });
    });
    describe('queryStream', function () {
        it('should return a readable stream of rows', function (done) {
            var expected = [self.a, self.b, self.c];
            self.sql
                .queryStream('SELECT * FROM `table` ORDER BY `id`')
                .then(function (stream) {
                stream.on('data', function (row) {
                    chai.assert.deepEqual(row, expected.shift());
                });
                stream.on('end', function () { return done(); });
            })
                .done();
        });
    });
    describe('selectStream', function () {
        it('should return a readable stream of rows', function (done) {
            self.sql
                .selectStream('table', { id: self.a.id })
                .then(function (stream) {
                stream.on('data', function (row) {
                    chai.assert.deepEqual(row, self.a);
                });
                stream.on('end', function () { return done(); });
            })
                .done();
        });
    });
    describe('query', function () {
        it('should select without values array', function (done) {
            self.sql
                .query('SELECT * FROM `table`')
                .then(function (rows) {
                chai.assert.sameDeepMembers(rows, [self.a, self.b, self.c]);
                done();
            })
                .done();
        });
        it('should have variable parameters using "?"', function (done) {
            self.sql
                .query('SELECT * FROM `table` WHERE id = ?', [2])
                .then(function (rows) {
                chai.assert.deepEqual(rows, [self.b]);
                done();
            })
                .done();
        });
        it('should have table/field parameters using "??"', function (done) {
            self.sql
                .query('SELECT ?? FROM `table`', ['unique'])
                .then(function (rows) {
                chai.assert.sameDeepMembers(rows, [
                    { unique: 'a' },
                    { unique: 'b' },
                    { unique: 'c' },
                ]);
                done();
            })
                .done();
        });
        it('should be case insensitive', function (done) {
            self.sql
                .query('sElEcT * FRoM `table` Where id = ?', [3])
                .then(function (rows) {
                chai.assert.deepEqual(rows, [self.c]);
                done();
            })
                .done();
        });
        it('should insert', function (done) {
            self.sql
                .query('INSERT INTO `table` (`unique`, `field`) ' +
                'VALUES ("testUniqueValue", "testFieldValue")')
                .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 1, 'affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'insertId');
                self.masterConn.query('SELECT * FROM `table` WHERE id = 4', function (err, rows) {
                    chai.assert.deepEqual(rows, [
                        {
                            id: 4,
                            unique: 'testUniqueValue',
                            field: 'testFieldValue',
                        },
                    ]);
                    done();
                });
            })
                .done();
        });
        it('should update', function (done) {
            self.sql
                .query('UPDATE `table` SET `field` = "edit" ' + 'WHERE `field` = "foo"')
                .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 2, 'affectedRows');
                chai.assert.strictEqual(res.changedRows, 2, 'changedRows');
                self.masterConn.query('SELECT * FROM `table` WHERE `field` = "edit"', function (err, rows) {
                    chai.assert.sameDeepMembers(rows, [
                        { id: 1, unique: 'a', field: 'edit' },
                        { id: 3, unique: 'c', field: 'edit' },
                    ], 'fields updated in database');
                    done();
                });
            })
                .done();
        });
        it('should delete', function (done) {
            self.sql
                .query('DELETE FROM `table` WHERE `field` = "foo"')
                .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 2, 'affectedRows');
                self.masterConn.query('SELECT * FROM `table` WHERE `field` = "foo"', function (err, rows) {
                    chai.assert.deepEqual(rows, [], 'fields deleted');
                    done();
                });
            })
                .done();
        });
        it('should have option to nest join', function (done) {
            self.sql
                .query({
                sql: 'SELECT * FROM `table` ' +
                    'INNER JOIN `table2` ' +
                    'ON `table`.`field` = `table2`.`field`',
                nestTables: true,
            })
                .then(function (rows) {
                chai.assert.deepEqual(rows, [
                    {
                        table: {
                            id: 2,
                            unique: 'b',
                            field: 'bar',
                        },
                        table2: {
                            id: 1,
                            field: 'bar',
                        },
                    },
                ]);
                done();
            })
                .done();
        });
        it('should have option to paginate', function (done) {
            self.sql
                .query({
                sql: 'SELECT * FROM `table`',
                paginate: {
                    page: 1,
                    resultsPerPage: 2,
                },
            })
                .then(function (resp) {
                chai.assert.deepEqual(_.omit(resp, 'results'), {
                    resultCount: 3,
                    pageCount: 2,
                    currentPage: 1,
                });
                chai.assert.sameDeepMembers(resp.results, [self.a, self.b]);
                done();
            })
                .done();
        });
        it('should have option to include result count', function (done) {
            self.sql
                .query({
                sql: 'SELECT * FROM `table` LIMIT 2',
                resultCount: true,
            })
                .then(function (resp) {
                chai.assert.deepEqual(_.omit(resp, 'results'), {
                    resultCount: 3,
                });
                chai.assert.sameDeepMembers(resp.results, [self.a, self.b]);
                done();
            })
                .done();
        });
    });
    describe('one', function () {
        it('should select a single row', function (done) {
            self.sql
                .one('SELECT * FROM `table` WHERE id = 1')
                .then(function (row) {
                chai.assert.deepEqual(row, self.a);
                done();
            })
                .done();
        });
    });
    describe('select', function () {
        it('should select by table and basic where clause', function (done) {
            self.sql
                .select('table', { id: 3, field: 'foo' })
                .then(function (rows) {
                chai.assert.deepEqual(rows, [self.c]);
                done();
            })
                .done();
        });
        it('should have option to paginate', function (done) {
            self.sql
                .select({
                table: 'table',
                paginate: {
                    page: 1,
                    resultsPerPage: 2,
                },
            })
                .then(function (rows) {
                chai.assert.deepEqual(rows, [self.a, self.b]);
                done();
            })
                .done();
        });
        it('should have option to select field', function (done) {
            self.sql
                .select({ table: 'table', fields: ['id'] })
                .then(function (rows) {
                chai.assert.deepEqual(rows, [{ id: 1 }, { id: 2 }, { id: 3 }]);
                done();
            })
                .done();
        });
    });
    describe('selectOne', function () {
        it('should select single row by table and basic where clause', function (done) {
            self.sql
                .selectOne('table', { field: 'foo' })
                .then(function (row) {
                chai.assert.deepEqual(row, self.a);
                done();
            })
                .done();
        });
        it('should have option to select fields', function (done) {
            self.sql
                .selectOne({ table: 'table', fields: ['id'] })
                .then(function (row) {
                chai.assert.deepEqual(row, { id: 1 });
                done();
            })
                .done();
        });
    });
    describe('insert', function () {
        it('should insert a single row', function (done) {
            self.sql
                .insert('table', { unique: 'd', field: 'baz' })
                .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 1, 'affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'insertId');
                self.masterConn.query('SELECT * FROM `table` WHERE `id` = 4', function (err, rows) {
                    chai.assert.deepEqual(rows, [{ id: 4, unique: 'd', field: 'baz' }], 'inserts into database');
                    done();
                });
            })
                .done();
        });
        it('should insert multiple rows', function (done) {
            self.sql
                .insert('table', [
                { unique: 'd', field: 'new' },
                { unique: 'e', field: 'new' },
            ])
                .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 2, 'affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'insertId');
                self.masterConn.query('SELECT * FROM `table` WHERE `field` = "new"', function (err, rows) {
                    chai.assert.deepEqual(rows, [
                        { id: 4, unique: 'd', field: 'new' },
                        { id: 5, unique: 'e', field: 'new' },
                    ], 'inserts into database');
                    done();
                });
            })
                .done();
        });
    });
    describe('replace', function () {
        it('should insert row', function (done) {
            self.sql
                .replace('table', { unique: 'd', field: 'baz' })
                .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 1, 'affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'insertId');
                self.masterConn.query('SELECT * FROM `table` WHERE `id` = 4', function (err, res) {
                    chai.assert.deepEqual(res, [{ id: 4, unique: 'd', field: 'baz' }], 'inserts into database');
                    done();
                });
            })
                .done();
        });
        it('should replace row with same unique key', function (done) {
            self.sql
                .replace('table', { unique: 'c', field: 'replaced' })
                .then(function () {
                self.masterConn.query('SELECT * FROM `table` WHERE `unique` = "c"', function (err, res) {
                    chai.assert.deepEqual(res, [{ id: 4, unique: 'c', field: 'replaced' }], 'replaces existing row and increments id');
                    done();
                });
            })
                .done();
        });
    });
    describe('save', function () {
        it('should insert row if does not exist', function (done) {
            self.sql
                .save('table', { unique: 'd', field: 'baz' })
                .then(function (res) {
                chai.assert.strictEqual(res.affectedRows, 1, 'returns affectedRows');
                chai.assert.strictEqual(res.insertId, 4, 'returns insert id');
                self.masterConn.query('SELECT * FROM `table` WHERE `id` = 4', function (err, res) {
                    chai.assert.deepEqual(res, [
                        { id: 4, unique: 'd', field: 'baz' },
                    ]);
                    done();
                });
            })
                .done();
        });
        it('should update row if exists by unique constraint', function (done) {
            self.sql
                .save('table', { unique: 'c', field: 'update' })
                .then(function () {
                self.masterConn.query('SELECT * FROM `table` WHERE `unique` = "c"', function (err, res) {
                    chai.assert.deepEqual(res, [
                        { id: 3, unique: 'c', field: 'update' },
                    ]);
                    done();
                });
            })
                .done();
        });
        it('should handle bulk save', function (done) {
            var rows = [
                { unique: 'a', field: 'edit-a' },
                { unique: 'b', field: 'edit-b' },
                { unique: 'd', field: 'new-field' },
            ];
            self.sql
                .save('table', rows)
                .then(function () {
                self.masterConn.query('SELECT * FROM `table`', function (err, res) {
                    chai.assert.sameDeepMembers(self.stripIds(res), self.stripIds(rows.concat([self.c])));
                    done();
                });
            })
                .done();
        });
    });
    describe('update', function () {
        it('should update row', function (done) {
            self.sql
                .update('table', { field: 'edit', unique: 'd' }, { id: 1 })
                .then(function (res) {
                self.masterConn.query('SELECT * FROM `table`', function (err, res) {
                    chai.assert.deepEqual(res, [
                        { id: 1, unique: 'd', field: 'edit' },
                        { id: 2, unique: 'b', field: 'bar' },
                        { id: 3, unique: 'c', field: 'foo' },
                    ], 'updates database');
                    done();
                });
            })
                .done();
        });
    });
    describe('delete', function () {
        it('should delete rows by where equals config', function (done) {
            self.sql
                .delete('table', { field: 'foo' })
                .then(function (res) {
                self.masterConn.query('SELECT * FROM `table`', function (err, res) {
                    chai.assert.deepEqual(res, [self.b]);
                    done();
                });
            })
                .done();
        });
    });
});
//# sourceMappingURL=mysql-wrap.unit.js.map