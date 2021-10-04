"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const bluebird_1 = __importDefault(require("bluebird"));
const chai_1 = __importStar(require("chai"));
const config = require("./config");
const mysql = require("mysql");
const createNodeMySQL = require("./mysql-wrap");
describe("mysqlWrap", () => {
    const self = {};
    before((done) => {
        self.stripIds = (data) => (lodash_1.default.isArray(data) ? lodash_1.default.map(data, self.stripIds) : lodash_1.default.omit(data, "id"));
        let pool = mysql.createPool(config.mysql);
        self.sql = createNodeMySQL(pool);
        pool.getConnection((err, conn) => {
            if (err) {
                console.log(err, err.stack);
            }
            else {
                self.masterConn = conn;
                done();
            }
        });
    });
    beforeEach((done) => {
        self.masterConn.query("TRUNCATE TABLE `table`", (err, res) => {
            self.a = { id: 1, unique: "a", field: "foo" };
            self.b = { id: 2, unique: "b", field: "bar" };
            self.c = { id: 3, unique: "c", field: "foo" };
            self.masterConn.query("INSERT INTO `table` (`unique`, `field`) " +
                "VALUES " +
                lodash_1.default.map([self.a, self.b, self.c], (row) => {
                    return '("' + row.unique + '", "' + row.field + '")';
                }).join(", "), (err, res) => {
                self.masterConn.query("TRUNCATE TABLE `table2`", (err, res) => {
                    self.masterConn.query("INSERT INTO `table2` (`field`) " + 'VALUES ("bar")', (err) => done());
                });
            });
        });
    });
    describe("connection", () => {
        it("should get a single connection", (done) => {
            self.sql
                .connection()
                .then((c) => {
                return bluebird_1.default.all(lodash_1.default.map(lodash_1.default.range(2), () => c.query("SELECT CONNECTION_ID()"))).then((resp) => {
                    chai_1.expect(resp[0]).to.deep.equal(resp[1]);
                    c.release();
                    done();
                });
            })
                .done();
        });
    });
    describe("build", () => {
        before((done) => {
            self.rowsToEdges = (rows, fields) => lodash_1.default.map(rows, (r) => ({
                node: r,
                cursor: self.toCursor(r, fields || ["id"])
            }));
            self.toCursor = (r, fields) => new Buffer(lodash_1.default.map(lodash_1.default.isArray(fields) ? fields : [fields], (f) => String(r[f])).join("#")).toString("base64");
            self.cursorFig = (od) => ({
                cursor: lodash_1.default.extend({ orderBy: "id" }, od)
            });
            done();
        });
        it("should have cursor option", (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                first: 100
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false
                    },
                    edges: self.rowsToEdges([self.a, self.b, self.c])
                });
                done();
            })
                .done();
        });
        it("should handle orderBy with direction", (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                first: 100,
                orderBy: { field: "id", direction: "DESC" }
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false
                    },
                    edges: self.rowsToEdges([self.c, self.b, self.a])
                });
                done();
            })
                .done();
        });
        it("should handle orderBy with serialization", (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                first: 100,
                orderBy: {
                    field: "id",
                    serialize: (v) => String(v + 1)
                }
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false
                    },
                    edges: lodash_1.default.map([self.a, self.b, self.c], (r) => ({
                        node: r,
                        cursor: self.toCursor({ id: r.id + 1 }, "id")
                    }))
                });
                done();
            })
                .done();
        });
        it("should handle orderBy with deserialization", (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                first: 100,
                after: self.toCursor(self.a, "id"),
                orderBy: {
                    field: "id",
                    deserialize: (v) => Number(v) + 1
                }
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 1,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false
                    },
                    edges: self.rowsToEdges([self.c])
                });
                done();
            })
                .done();
        });
        it('should limit with "first" field', (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                first: 1
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: true,
                        hasPreviousPage: false
                    },
                    edges: self.rowsToEdges([self.a])
                });
                done();
            })
                .done();
        });
        it('should limit with the "last" field', (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                last: 1
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: true
                    },
                    edges: self.rowsToEdges([self.c])
                });
                done();
            })
                .done();
        });
        it('should enable next page selection with the "after" field', (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                first: 100,
                after: self.toCursor(self.a, "id")
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false
                    },
                    edges: self.rowsToEdges([self.b, self.c])
                });
                done();
            })
                .done();
        });
        it('should enable previous page selection with the "before" field', (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                last: 100,
                before: self.toCursor(self.c, "id")
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false
                    },
                    edges: self.rowsToEdges([self.a, self.b])
                });
                done();
            })
                .done();
        });
        it('should limit with "first" and "after"', (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                first: 1,
                after: self.toCursor(self.a, "id")
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: true,
                        hasPreviousPage: false
                    },
                    edges: self.rowsToEdges([self.b])
                });
                done();
            })
                .done();
        });
        it('should limit with "last" and "after"', (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                last: 1,
                after: self.toCursor(self.a, "id")
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: true
                    },
                    edges: self.rowsToEdges([self.c])
                });
                done();
            })
                .done();
        });
        it('should limit with "first" and "before"', (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                first: 1,
                before: self.toCursor(self.c, "id")
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: true,
                        hasPreviousPage: false
                    },
                    edges: self.rowsToEdges([self.a])
                });
                done();
            })
                .done();
        });
        it('should limit with "last" and "before"', (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                last: 1,
                before: self.toCursor(self.c, "id")
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: true
                    },
                    edges: self.rowsToEdges([self.b])
                });
                done();
            })
                .done();
        });
        it("should handle compound orderBy", (done) => {
            const orderBy = ["field", "id"];
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                orderBy: orderBy,
                first: 100
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false
                    },
                    edges: self.rowsToEdges([self.b, self.a, self.c], orderBy)
                });
                done();
            })
                .done();
        });
        it("should handle compound orderBy with direction", (done) => {
            const orderBy = [{ field: "field" }, { field: "id", direction: "DESC" }];
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                orderBy: orderBy,
                first: 100
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 3,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false
                    },
                    edges: self.rowsToEdges([self.b, self.c, self.a], lodash_1.default.map(orderBy, (o) => o.field))
                });
                done();
            })
                .done();
        });
        it("should handle compound orderBy with complex fig", (done) => {
            const orderBy = ["field", "id"];
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                orderBy: ["field", "id"],
                first: 2,
                before: self.toCursor(self.c, orderBy),
                after: self.toCursor(self.b, orderBy)
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 1,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false
                    },
                    edges: self.rowsToEdges([self.a], orderBy)
                });
                done();
            })
                .done();
        });
        it("should handle compound orderBy with complex fig with direction", (done) => {
            const orderBy = [{ field: "field" }, { field: "id", direction: "DESC" }];
            self.sql
                .build()
                .select()
                .from("`table`")
                .run(self.cursorFig({
                orderBy: orderBy,
                first: 2,
                before: self.toCursor(self.a, lodash_1.default.map(orderBy, (o) => o.field))
            }))
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal({
                    resultCount: 2,
                    pageInfo: {
                        hasNextPage: false,
                        hasPreviousPage: false
                    },
                    edges: self.rowsToEdges([self.b, self.c], lodash_1.default.map(orderBy, (o) => o.field))
                });
                done();
            })
                .done();
        });
        it("should have whereIfDefined method", (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .whereIfDefined("id = ?", undefined)
                .run()
                .then((resp) => {
                chai_1.expect(resp).to.have.deep.members([self.a, self.b, self.c]);
                return self.sql.build().select().from("`table`").whereIfDefined("id = ?", 0).run();
            })
                .then((resp) => {
                chai_1.expect(resp).to.deep.equal([]);
                done();
            })
                .done();
        });
        it("should return query generator", (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .where("field = ?", self.b.field)
                .run()
                .then((resp) => {
                chai_1.default.assert.deepEqual(resp, [self.b]);
                done();
            })
                .done();
        });
        it('should be able to pass query options through "run" command', (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .where("id = ?", self.b.id)
                .run({ resultCount: true })
                .then((resp) => {
                chai_1.default.assert.deepEqual(resp, {
                    resultCount: 1,
                    results: [self.b]
                });
                done();
            })
                .done();
        });
        it('should be invokable through a "one" command', (done) => {
            self.sql
                .build()
                .select()
                .from("`table`")
                .where("id = ?", self.b.id)
                .one()
                .then((resp) => {
                chai_1.default.assert.deepEqual(resp, self.b);
                done();
            })
                .done();
        });
    });
    describe("queryStream", () => {
        it("should return a readable stream of rows", (done) => {
            let expected = [self.a, self.b, self.c];
            self.sql
                .queryStream("SELECT * FROM `table` ORDER BY `id`")
                .then((stream) => {
                stream.on("data", (row) => {
                    chai_1.default.assert.deepEqual(row, expected.shift());
                });
                stream.on("end", () => done());
            })
                .done();
        });
    });
    describe("selectStream", () => {
        it("should return a readable stream of rows", (done) => {
            self.sql
                .selectStream("table", { id: self.a.id })
                .then((stream) => {
                stream.on("data", (row) => {
                    chai_1.default.assert.deepEqual(row, self.a);
                });
                stream.on("end", () => done());
            })
                .done();
        });
    });
    describe("query", () => {
        it("should select without values array", (done) => {
            self.sql
                .query("SELECT * FROM `table`")
                .then((rows) => {
                chai_1.default.assert.sameDeepMembers(rows, [self.a, self.b, self.c]);
                done();
            })
                .done();
        });
        it('should have variable parameters using "?"', (done) => {
            self.sql
                .query("SELECT * FROM `table` WHERE id = ?", [2])
                .then((rows) => {
                chai_1.default.assert.deepEqual(rows, [self.b]);
                done();
            })
                .done();
        });
        it('should have table/field parameters using "??"', (done) => {
            self.sql
                .query("SELECT ?? FROM `table`", ["unique"])
                .then((rows) => {
                chai_1.default.assert.sameDeepMembers(rows, [{ unique: "a" }, { unique: "b" }, { unique: "c" }]);
                done();
            })
                .done();
        });
        it("should be case insensitive", (done) => {
            self.sql
                .query("sElEcT * FRoM `table` Where id = ?", [3])
                .then((rows) => {
                chai_1.default.assert.deepEqual(rows, [self.c]);
                done();
            })
                .done();
        });
        it("should insert", (done) => {
            self.sql
                .query("INSERT INTO `table` (`unique`, `field`) " + 'VALUES ("testUniqueValue", "testFieldValue")')
                .then((res) => {
                chai_1.default.assert.strictEqual(res.affectedRows, 1, "affectedRows");
                chai_1.default.assert.strictEqual(res.insertId, 4, "insertId");
                self.masterConn.query("SELECT * FROM `table` WHERE id = 4", (err, rows) => {
                    chai_1.default.assert.deepEqual(rows, [
                        {
                            id: 4,
                            unique: "testUniqueValue",
                            field: "testFieldValue"
                        }
                    ]);
                    done();
                });
            })
                .done();
        });
        it("should update", (done) => {
            self.sql
                .query('UPDATE `table` SET `field` = "edit" ' + 'WHERE `field` = "foo"')
                .then((res) => {
                chai_1.default.assert.strictEqual(res.affectedRows, 2, "affectedRows");
                chai_1.default.assert.strictEqual(res.changedRows, 2, "changedRows");
                self.masterConn.query('SELECT * FROM `table` WHERE `field` = "edit"', (err, rows) => {
                    chai_1.default.assert.sameDeepMembers(rows, [
                        { id: 1, unique: "a", field: "edit" },
                        { id: 3, unique: "c", field: "edit" }
                    ], "fields updated in database");
                    done();
                });
            })
                .done();
        });
        it("should delete", (done) => {
            self.sql
                .query('DELETE FROM `table` WHERE `field` = "foo"')
                .then((res) => {
                chai_1.default.assert.strictEqual(res.affectedRows, 2, "affectedRows");
                self.masterConn.query('SELECT * FROM `table` WHERE `field` = "foo"', (err, rows) => {
                    chai_1.default.assert.deepEqual(rows, [], "fields deleted");
                    done();
                });
            })
                .done();
        });
        it("should have option to nest join", (done) => {
            self.sql
                .query({
                sql: "SELECT * FROM `table` " + "INNER JOIN `table2` " + "ON `table`.`field` = `table2`.`field`",
                nestTables: true
            })
                .then((rows) => {
                chai_1.default.assert.deepEqual(rows, [
                    {
                        table: {
                            id: 2,
                            unique: "b",
                            field: "bar"
                        },
                        table2: {
                            id: 1,
                            field: "bar"
                        }
                    }
                ]);
                done();
            })
                .done();
        });
        it("should have option to paginate", (done) => {
            self.sql
                .query({
                sql: "SELECT * FROM `table`",
                paginate: {
                    page: 1,
                    resultsPerPage: 2
                }
            })
                .then((resp) => {
                chai_1.default.assert.deepEqual(lodash_1.default.omit(resp, "results"), {
                    resultCount: 3,
                    pageCount: 2,
                    currentPage: 1
                });
                chai_1.default.assert.sameDeepMembers(resp.results, [self.a, self.b]);
                done();
            })
                .done();
        });
        it("should have option to include result count", (done) => {
            self.sql
                .query({
                sql: "SELECT * FROM `table` LIMIT 2",
                resultCount: true
            })
                .then((resp) => {
                chai_1.default.assert.deepEqual(lodash_1.default.omit(resp, "results"), {
                    resultCount: 3
                });
                chai_1.default.assert.sameDeepMembers(resp.results, [self.a, self.b]);
                done();
            })
                .done();
        });
    });
    describe("one", () => {
        it("should select a single row", (done) => {
            self.sql
                .one("SELECT * FROM `table` WHERE id = 1")
                .then((row) => {
                chai_1.default.assert.deepEqual(row, self.a);
                done();
            })
                .done();
        });
    });
    describe("select", () => {
        it("should select by table and basic where clause", (done) => {
            self.sql
                .select("table", { id: 3, field: "foo" })
                .then((rows) => {
                chai_1.default.assert.deepEqual(rows, [self.c]);
                done();
            })
                .done();
        });
        it("should have option to paginate", (done) => {
            self.sql
                .select({
                table: "table",
                paginate: {
                    page: 1,
                    resultsPerPage: 2
                }
            })
                .then((rows) => {
                chai_1.default.assert.deepEqual(rows, [self.a, self.b]);
                done();
            })
                .done();
        });
        it("should have option to select field", (done) => {
            self.sql
                .select({ table: "table", fields: ["id"] })
                .then((rows) => {
                chai_1.default.assert.deepEqual(rows, [{ id: 1 }, { id: 2 }, { id: 3 }]);
                done();
            })
                .done();
        });
    });
    describe("selectOne", () => {
        it("should select single row by table and basic where clause", (done) => {
            self.sql
                .selectOne("table", { field: "foo" })
                .then((row) => {
                chai_1.default.assert.deepEqual(row, self.a);
                done();
            })
                .done();
        });
        it("should have option to select fields", (done) => {
            self.sql
                .selectOne({ table: "table", fields: ["id"] })
                .then((row) => {
                chai_1.default.assert.deepEqual(row, { id: 1 });
                done();
            })
                .done();
        });
    });
    describe("insert", () => {
        it("should insert a single row", (done) => {
            self.sql
                .insert("table", { unique: "d", field: "baz" })
                .then((res) => {
                chai_1.default.assert.strictEqual(res.affectedRows, 1, "affectedRows");
                chai_1.default.assert.strictEqual(res.insertId, 4, "insertId");
                self.masterConn.query("SELECT * FROM `table` WHERE `id` = 4", (err, rows) => {
                    chai_1.default.assert.deepEqual(rows, [{ id: 4, unique: "d", field: "baz" }], "inserts into database");
                    done();
                });
            })
                .done();
        });
        it("should insert multiple rows", (done) => {
            self.sql
                .insert("table", [
                { unique: "d", field: "new" },
                { unique: "e", field: "new" }
            ])
                .then((res) => {
                chai_1.default.assert.strictEqual(res.affectedRows, 2, "affectedRows");
                chai_1.default.assert.strictEqual(res.insertId, 4, "insertId");
                self.masterConn.query('SELECT * FROM `table` WHERE `field` = "new"', (err, rows) => {
                    chai_1.default.assert.deepEqual(rows, [
                        { id: 4, unique: "d", field: "new" },
                        { id: 5, unique: "e", field: "new" }
                    ], "inserts into database");
                    done();
                });
            })
                .done();
        });
    });
    describe("replace", () => {
        it("should insert row", (done) => {
            self.sql
                .replace("table", { unique: "d", field: "baz" })
                .then((res) => {
                chai_1.default.assert.strictEqual(res.affectedRows, 1, "affectedRows");
                chai_1.default.assert.strictEqual(res.insertId, 4, "insertId");
                self.masterConn.query("SELECT * FROM `table` WHERE `id` = 4", (err, res) => {
                    chai_1.default.assert.deepEqual(res, [{ id: 4, unique: "d", field: "baz" }], "inserts into database");
                    done();
                });
            })
                .done();
        });
        it("should replace row with same unique key", (done) => {
            self.sql
                .replace("table", { unique: "c", field: "replaced" })
                .then(() => {
                self.masterConn.query('SELECT * FROM `table` WHERE `unique` = "c"', (err, res) => {
                    chai_1.default.assert.deepEqual(res, [{ id: 4, unique: "c", field: "replaced" }], "replaces existing row and increments id");
                    done();
                });
            })
                .done();
        });
    });
    describe("save", () => {
        it("should insert row if does not exist", (done) => {
            self.sql
                .save("table", { unique: "d", field: "baz" })
                .then((res) => {
                chai_1.default.assert.strictEqual(res.affectedRows, 1, "returns affectedRows");
                chai_1.default.assert.strictEqual(res.insertId, 4, "returns insert id");
                self.masterConn.query("SELECT * FROM `table` WHERE `id` = 4", (err, res) => {
                    chai_1.default.assert.deepEqual(res, [{ id: 4, unique: "d", field: "baz" }]);
                    done();
                });
            })
                .done();
        });
        it("should update row if exists by unique constraint", (done) => {
            self.sql
                .save("table", { unique: "c", field: "update" })
                .then(() => {
                self.masterConn.query('SELECT * FROM `table` WHERE `unique` = "c"', (err, res) => {
                    chai_1.default.assert.deepEqual(res, [{ id: 3, unique: "c", field: "update" }]);
                    done();
                });
            })
                .done();
        });
        it("should handle bulk save", (done) => {
            let rows = [
                { unique: "a", field: "edit-a" },
                { unique: "b", field: "edit-b" },
                { unique: "d", field: "new-field" }
            ];
            self.sql
                .save("table", rows)
                .then(() => {
                self.masterConn.query("SELECT * FROM `table`", (err, res) => {
                    chai_1.default.assert.sameDeepMembers(self.stripIds(res), self.stripIds(rows.concat([self.c])));
                    done();
                });
            })
                .done();
        });
    });
    describe("update", () => {
        it("should update row", (done) => {
            self.sql
                .update("table", { field: "edit", unique: "d" }, { id: 1 })
                .then((res) => {
                self.masterConn.query("SELECT * FROM `table`", (err, res) => {
                    chai_1.default.assert.deepEqual(res, [
                        { id: 1, unique: "d", field: "edit" },
                        { id: 2, unique: "b", field: "bar" },
                        { id: 3, unique: "c", field: "foo" }
                    ], "updates database");
                    done();
                });
            })
                .done();
        });
    });
    describe("delete", () => {
        it("should delete rows by where equals config", (done) => {
            self.sql
                .delete("table", { field: "foo" })
                .then((res) => {
                self.masterConn.query("SELECT * FROM `table`", (err, res) => {
                    chai_1.default.assert.deepEqual(res, [self.b]);
                    done();
                });
            })
                .done();
        });
    });
});
//# sourceMappingURL=mysql-wrap.unit.js.map