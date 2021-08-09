"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var _ = require('lodash');
var Promise = require('bluebird');
var squel = require('squel');
var createMySQLWrap = function (poolCluster, options, connection) {
    options = options || {};
    var self = {};
    var stripLimit = function (sql) { return sql.replace(/ LIMIT .*/i, ''); };
    var paginateLimit = function (fig) {
        return fig
            ? 'LIMIT ' +
                fig.resultsPerPage +
                ' ' +
                'OFFSET ' +
                (fig.page - 1) * fig.resultsPerPage
            : '';
    };
    var addCalcFoundRows = function (sql) {
        var pieces = sql.split(' ');
        pieces.splice(1, 0, 'SQL_CALC_FOUND_ROWS');
        return pieces.join(' ');
    };
    var getStatementObject = function (statementOrObject) {
        var statement = _.isObject(statementOrObject)
            ? statementOrObject
            : {
                sql: statementOrObject,
                nestTables: false,
            };
        if (statement.paginate) {
            statement.sql = addCalcFoundRows(stripLimit(statement.sql) + ' ' + paginateLimit(statement.paginate));
        }
        else if (statement.resultCount) {
            statement.sql = addCalcFoundRows(statement.sql);
        }
        return statement;
    };
    var prepareWhereEquals = function (whereEquals) {
        var values = [];
        var sql = _.map(whereEquals, function (val, key) {
            values.push(key, val);
            return '?? = ?';
        }, '').join(' AND ');
        return {
            values: values,
            sql: sql ? ' WHERE ' + sql : sql,
        };
    };
    var getConnection = function (readOrWrite) {
        return new Promise(function (resolve, reject) {
            if (connection) {
                resolve(connection);
            }
            else {
                if (options.replication) {
                    poolCluster.getConnection(options.replication[readOrWrite], function (err, conn) { return (err ? reject(err) : resolve(conn)); });
                }
                else {
                    poolCluster.getConnection(function (err, conn) {
                        return err ? reject(err) : resolve(conn);
                    });
                }
            }
        });
    };
    var selectedFieldsSQL = function (fields) { return (fields ? fields.join(', ') : '*'); };
    var prepareInsertRows = function (rowOrRows) {
        var values = [];
        var fields = _.isArray(rowOrRows)
            ? _.keys(_.first(rowOrRows))
            : _.keys(rowOrRows);
        var fieldsSQL = '(' +
            _.map(fields, function (field) {
                values.push(field);
                return '??';
            }).join(', ') +
            ')';
        var processValuesSQL = function (row) {
            return '(' +
                _.map(fields, function (field) {
                    values.push(row[field]);
                    return '?';
                }) +
                ')';
        };
        var valuesSQL = _.isArray(rowOrRows)
            ? _.map(rowOrRows, processValuesSQL).join(', ')
            : processValuesSQL(rowOrRows);
        return {
            sql: fieldsSQL + ' VALUES ' + valuesSQL,
            values: values,
        };
    };
    var isSQLReadOrWrite = function (statementRaw) {
        return /^SELECT/i.test(statementRaw.trim()) ? 'read' : 'write';
    };
    var mapOrderBy = function (raw) {
        return _.map(_.isArray(raw) ? raw : [raw], function (o) {
            return _.isString(o)
                ? {
                    field: o,
                    isAscending: true,
                }
                : _.extend(_.omit(_.clone(o), 'direction'), {
                    isAscending: o.direction === 'DESC' ? false : true,
                });
        });
    };
    var CURSOR_DELIMETER = '#';
    self.encodeCursor = function (orderByRaw, row) {
        var orderBy = mapOrderBy(orderByRaw);
        return new Buffer(_.map(orderBy, function (o) {
            return o.serialize ? o.serialize(row[o.field]) : String(row[o.field]);
        }).join(CURSOR_DELIMETER)).toString('base64');
    };
    var runCursor = function (q, fig) {
        var orderBy = mapOrderBy(fig.orderBy);
        var isAscending = fig.last && !fig.first ? false : true;
        var decodeCursor = function (c) {
            return _.map(new Buffer(c, 'base64').toString('ascii').split(CURSOR_DELIMETER), function (v, i) { return (orderBy[i].deserialize ? orderBy[i].deserialize(v) : v); });
        };
        var buildWhereArgs = function (values, isGreaterThan) {
            var build = function (values, orderBy, isGreaterThan) {
                var sql = _.map(orderBy, function (o, i) {
                    return i === values.length - 1
                        ? o.field + " " + ((o.isAscending ? isGreaterThan : !isGreaterThan) ? '>' : '<') + " ?"
                        : o.field + " = ?";
                }).join(' AND ');
                var sqls = [sql];
                var mappedValues = [values];
                if (values.length > 1) {
                    var w_1 = build(_.initial(values), _.initial(orderBy), isGreaterThan);
                    sqls = sqls.concat(w_1.sqls);
                    mappedValues = mappedValues.concat(w_1.mappedValues);
                }
                return {
                    sqls: sqls,
                    mappedValues: mappedValues,
                };
            };
            var w = build(values, orderBy, isGreaterThan);
            return [w.sqls.reverse().join(' OR ')].concat(_.flatten(w.mappedValues.reverse()));
        };
        _.each(orderBy, function (o) {
            q.order(o.field, o.isAscending ? isAscending : !isAscending);
        });
        if (fig.after) {
            q.where.apply(q, buildWhereArgs(decodeCursor(fig.after), true));
        }
        if (fig.before) {
            q.where.apply(q, buildWhereArgs(decodeCursor(fig.before), false));
        }
        q.limit(isAscending ? fig.first : fig.last);
        var query = q.toParam();
        return self
            .query({
            sql: query.text,
            resultCount: true,
        }, query.values)
            .then(function (resp) {
            if (isAscending && fig.last && fig.last < resp.results.length) {
                resp.results = resp.results.slice(resp.results.length - fig.last, resp.results.length);
            }
            else if (!isAscending && fig.last && fig.last < resp.results.length) {
                resp.results = resp.results.slice(0, fig.last);
            }
            if (!isAscending) {
                resp.results = resp.results.reverse();
            }
            return resp;
        })
            .then(function (resp) { return ({
            resultCount: resp.resultCount,
            pageInfo: {
                hasPreviousPage: fig.last ? resp.resultCount > fig.last : false,
                hasNextPage: fig.first ? resp.resultCount > fig.first : false,
            },
            edges: _.map(resp.results, function (r) { return ({
                node: r,
                cursor: self.encodeCursor(orderBy, r),
            }); }),
        }); });
    };
    self.build = function () {
        var wrap = function (method) { return function () {
            var s = squel[method]();
            s.run = function (fig) {
                fig = fig || {};
                if (fig.cursor) {
                    return runCursor(s, fig.cursor);
                }
                else {
                    var p = s.toParam();
                    return self.query(_.extend({
                        sql: p.text,
                    }, fig), p.values);
                }
            };
            s.one = function (fig) {
                var p = s.toParam();
                return self.one(_.extend({
                    sql: p.text,
                }, fig || {}), p.values);
            };
            s.whereIfDefined = function (sql, value) {
                if (value !== undefined) {
                    s.where(sql, value);
                }
                return s;
            };
            return s;
        }; };
        var buildSelf = {
            select: wrap('select'),
            update: wrap('update'),
            delete: wrap('delete'),
            insert: wrap('insert'),
        };
        return buildSelf;
    };
    self.connection = function () {
        return getConnection('write').then(function (conn) {
            var sql = createMySQLWrap(null, options, conn);
            sql.release = function () { return conn && conn.release && conn.release(); };
            return sql;
        });
    };
    var finishedWithConnection = function (conn) {
        if (!connection) {
            conn && conn.release && conn.release();
        }
    };
    self.query = function (statementRaw, values) {
        var statementObject = getStatementObject(statementRaw);
        return getConnection(isSQLReadOrWrite(statementObject.sql)).then(function (conn) {
            return new Promise(function (resolve, reject) {
                conn.query(statementObject, values || [], function (err, rows) {
                    if (err) {
                        finishedWithConnection(conn);
                        reject(err);
                    }
                    else if (statementObject.paginate ||
                        statementObject.resultCount) {
                        conn.query('SELECT FOUND_ROWS() AS count', function (err, result) {
                            finishedWithConnection(conn);
                            if (err) {
                                reject(err);
                            }
                            else if (statementObject.paginate) {
                                resolve({
                                    resultCount: _.first(result).count,
                                    pageCount: Math.ceil(_.first(result).count /
                                        statementObject.paginate.resultsPerPage),
                                    currentPage: statementObject.paginate.page,
                                    results: rows,
                                });
                            }
                            else if (statementObject.resultCount) {
                                resolve({
                                    resultCount: _.first(result).count,
                                    results: rows,
                                });
                            }
                        });
                    }
                    else {
                        finishedWithConnection(conn);
                        resolve(rows);
                    }
                });
            });
        });
    };
    self.queryStream = function (statementRaw, values) {
        var statementObject = getStatementObject(statementRaw);
        return getConnection(isSQLReadOrWrite(statementObject.sql)).then(function (conn) {
            var stream = conn.query(statementObject, values || []).stream();
            stream.on('error', function (err) {
                console.error(err);
                finishedWithConnection(conn);
            });
            stream.on('end', function () { return finishedWithConnection(conn); });
            return stream;
        });
    };
    self.one = function (statementRaw, values) {
        var statementObject = getStatementObject(statementRaw);
        statementObject.sql = stripLimit(statementObject.sql) + ' LIMIT 1';
        return self
            .query(statementObject, values)
            .then(function (rows) { return _.first(rows) || null; });
    };
    var buildSelect = function (tableRaw, whereEquals) {
        var statementObject = _.isObject(tableRaw)
            ? tableRaw
            : {
                table: tableRaw,
            };
        var where = prepareWhereEquals(whereEquals);
        var values = [statementObject.table].concat(where.values);
        var sql = 'SELECT ' +
            selectedFieldsSQL(statementObject.fields) +
            ' ' +
            'FROM ?? ' +
            where.sql +
            (statementObject.paginate
                ? ' ' + paginateLimit(statementObject.paginate)
                : '');
        return {
            sql: sql,
            values: values,
        };
    };
    self.select = function (tableRaw, whereEquals) {
        var query = buildSelect(tableRaw, whereEquals);
        return self.query(query.sql, query.values);
    };
    self.selectStream = function (tableRaw, whereEquals) {
        var query = buildSelect(tableRaw, whereEquals);
        return self.queryStream(query.sql, query.values);
    };
    self.selectOne = function (tableRaw, whereEquals) {
        var statementObject = _.isObject(tableRaw)
            ? tableRaw
            : {
                table: tableRaw,
            };
        var where = prepareWhereEquals(whereEquals);
        var values = [statementObject.table].concat(where.values);
        return self.one('SELECT ' +
            selectedFieldsSQL(statementObject.fields) +
            ' FROM ?? ' +
            where.sql, values);
    };
    self.insert = function (table, rowOrRows) {
        var rows = prepareInsertRows(rowOrRows);
        return self.query('INSERT INTO ?? ' + rows.sql, [table].concat(rows.values));
    };
    self.replace = function (table, rowRaw, callback) {
        var row = prepareInsertRows(rowRaw);
        return self.query('REPLACE INTO ?? ' + row.sql, [table].concat(row.values));
    };
    self.save = function (table, rowOrRows) {
        var rows = _.isArray(rowOrRows) ? rowOrRows : [rowOrRows];
        var prepareSaveRows = function () {
            var insertRow = prepareInsertRows(rows);
            var setValues = [];
            var setSQL = _.map(_.first(rows), function (val, key) {
                setValues.push(key, key);
                return '?? = VALUES(??)';
            }).join(', ');
            return {
                sql: insertRow.sql + ' ON DUPLICATE KEY UPDATE ' + setSQL,
                values: insertRow.values.concat(setValues),
            };
        };
        var row = prepareSaveRows();
        return self.query('INSERT INTO ?? ' + row.sql, [table].concat(row.values));
    };
    self.update = function (table, setData, whereEquals) {
        var prepareSetRows = function (setData) {
            var values = [];
            var sql = ' SET ' +
                _.map(setData, function (val, key) {
                    values.push(key, val);
                    return '?? = ?';
                }).join(', ');
            return {
                values: values,
                sql: sql,
            };
        };
        var set = prepareSetRows(setData);
        var where = prepareWhereEquals(whereEquals);
        var values = [table].concat(set.values).concat(where.values);
        return self.query('UPDATE ??' + set.sql + where.sql, values);
    };
    self.delete = function (table, whereEquals) {
        var where = prepareWhereEquals(whereEquals);
        var values = [table].concat(where.values);
        return self.query('DELETE FROM ?? ' + where.sql, values);
    };
    self.escape = function (data) { return poolCluster.escape(data); };
    self.escapeId = function (data) { return poolCluster.escapeId(data); };
    return self;
};
module.exports = createMySQLWrap;
//# sourceMappingURL=mysql-wrap.js.map