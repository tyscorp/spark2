var Promise = require('bluebird');
var _ = require('lodash');
var JTT = require('./jtt');
var Table = require('./tables');
var LRU = require('lru-cache');

var queryCache = LRU(5);

var mysql;

function execQuery (cache, query) {
    if (cache && queryCache.has(query)) return queryCache.get(query);

    var time = Date.now();

    return mysql.queryAsync(query).get(0).tap(function (results) {
        //console.log(/*query*/ (Date.now() - time) + ' ms');

        cache && queryCache.set(query, results);
    });
}

function CandidateNetwork (table_name, Q) {
    this.parent = this;
    this.table_name = table_name;
    this.connections = [];
    this.Q = Q;
    this.valid = false;
    this.getQuery = _.memoize(CandidateNetwork.getQuery.bind(this)); // cache
    this.getTables = _.memoize(CandidateNetwork.getTables.bind(this)); // cache
}

CandidateNetwork.getQuery = function () {
    var q = mysql.escape(this.Q.join(' '));

    return 'SELECT * FROM ' +
            this.getTables().join(', ') + ' ' +
            'WHERE ' + this.getJoins().join(' AND ') + ' ' +
            'AND ' + this.getMatches(q).join(' AND ') + ';';
            
};

CandidateNetwork.getTables = function () {
    return _(this.connections)
    .map(function (node) {
        return node.getTables();
    })
    .flatten()
    .compact()
    .push(this.table_name)
    .uniq()
    .sort()
    .value();
};

// This is a *LOT* faster than multiple AND MATCH AGAINST
CandidateNetwork.prototype.exec = function (cache) {
    var q = mysql.escape(this.Q.join(' '));
    var tables = this.getTables().join(', ');
    var joins = this.getJoins().join(' AND ');
    var table_name = this.table_name;
    var query = this.getQuery();

    var index_key = table_name.substr(0, table_name.length - 1) + '_id';

    var select = _(this.getTables())
    .map(function (table_name) {
        var table = Table.getTable(table_name);

        return _.map(table.fields, function (field) {
            return '`' + table.name + '`.`' + field + '` AS ' + table.name + '_' + field;
        });
    })
    .flatten()
    .value()
    .join(', ');

    var queries = _.map(this.getMatches(q), function (match) {
        return 'SELECT `' + table_name + '`.`id` AS id, ' + select + ' ' +
               'FROM ' +
               tables + ' ' +
               'WHERE ' + joins + ' ' +
               'AND ' + match + ';'
    });

    var cn = this;
    var time = Date.now();
    var self = this;

    return Promise.map(queries, _.partial(execQuery, cache))
    .then(function (results) {
        if (results.length === 1) {
            return _.map(results[0], function (row) {
                return new JTT(row);
            });
        }

        // AND
        var ids = _.map(results, function (rows) {
            return _(rows).pluck(index_key).value();
        });

        var valid = _.intersection.apply(_, ids);

        return _.map(valid, function (id) {
            return new JTT(_.merge.apply(_, _.map(results, function (rows) {
                return _.find(rows, function (row) {
                    return row[index_key] == id;
                });
            })));
        });
        // OR
        /*return _(results)
        .flatten()
        .map(function (result) {
            return new JTT(result);
        })
        .value();*/
    })
    .map(function (jtt) {
        if (!cache) {
            jtt.query = self.getQuery();
            jtt.queries = queries;
            jtt.time = Date.now() - time;
        }

        return jtt;
    })
    .tap(function (results) {
        //console.log(query, results.length);
    });
};

CandidateNetwork.prototype.getGraph = function () {
    var graph = {};
    var table_name = this.table_name;
    graph[table_name] = {};

    _.forEach(this.connections, function (node) {
        graph[table_name][node.table_name] = node.getGraph();
    });

    return graph;
};

CandidateNetwork.prototype.setScoreTable = function (scoreTable) {
    var tables = this.getTables();

    this.scoreTable = _(scoreTable)
    .filter(function (data) {
        return data.valid && ~tables.indexOf(data.data.table_name);
    })
    .pluck('data')
    .value();
};

// Candidate network scoring function (Equation 3)
CandidateNetwork.prototype.getNormalizationScore = function () {
    var tables = this.getTables();

    var s1 = 0.15;
    var s2 = 1 / (this.Q.length + 1);

    var CN_ALL = tables.length;
    var CN_NF = _.reduce(tables, function (n, table_name) {
        if (Table.isDataTable(table_name)) return n + 1;

        return n;
    }, 0);

    return (1 + s1 - s1 * CN_ALL) * (1 + s2 - s2 * CN_NF);
};

CandidateNetwork.prototype.getJoins = function () {
    if (this.connections.length === 0) return [1];

    var self = this;

    var x = [];

    var table = Table.getTable(self.table_name);

    if (table.join) {
        x = x.concat(_.map(table.join, function (join) {

            if (_.find(self.connections, { table_name: join.with_table })) {
                return { a: '`' + self.table_name + '`.`' + join.key[0] + '`', b: '`' + join.with_table + '`.`' + join.key[1] + '`' };
            }

            return [];
        }));
    }
    if (table.fk) {
        x = x.concat(_.map(table.fk, function (table_name) {

            if (_.find(self.connections, { table_name: table_name })) {
                var t = Table.getTable(table_name);
                var join = _.find(t.join, { with_table: self.table_name });
                return { a: '`' + self.table_name + '`.`' + join.key[1] + '`', b: '`' + table_name + '`.`' + join.key[0] + '`' };
            }

            return [];
        }));
    }

    return _(x.concat(_.map(self.connections, function (node) {
        return node.getJoins();
    })))
    .compact()
    .flatten()
    .compact()
    .map(function (tuple) {
        var t = [tuple.a, tuple.b].sort();
        return t[0] + '=' + t[1];
    })
    .uniq()
    .sort()
    .value();
};

CandidateNetwork.prototype.getMatches = function (q) {
    return _(this.getTables())
    .map(function (table_name) {
        return Table.getTable(table_name);
    })
    .filter(function (table) {
        return !!table.search;
    })
    .map(function (table) {
        var x = _.map(table.search, function (field) {
            return 'MATCH (`' + table.name + '`.`' + field + '`) AGAINST (' + q + ' IN BOOLEAN MODE)'
        });

        if (x.length === 1) return x[0];

        return '(' + x.join(' OR ') + ');';
    })
    .sort()
    .value();
};

CandidateNetwork.prototype.trim = function () {
    this.connections = _.filter(this.connections, function (node) {
        return node.trim();
    });

    return this;
};

CandidateNetwork.prototype.toJSON = function () {
    var clean = _.omit(this, 'parent', 'valid');

    clean.connections = _.map(clean.connections, function (conn) {
        return conn.toJSON();
    });

    return clean;
};

CandidateNetwork.prototype.setValid = function (value) {
    this.valid = value;
};

function Node (parent, table_name) {
    this.parent = parent;
    this.table_name = table_name;
    this.connections = [];
    this.valid = false;
}

Node.prototype.getGraph = function () {
    var graph = {};

    _.forEach(this.connections, function (node) {
        graph[node.table_name] = node.getGraph();
    });

    return graph;
};

Node.prototype.getTables = function () {
    return [this.table_name].concat(_.map(this.connections, function (node) {
        return node.getTables();
    }));
};

Node.prototype.getJoins = function () {
    if (this.connections.length === 0) return null;

    var self = this;

    var x = [];

    var table = Table.getTable(self.table_name);

    if (table.join) {
        x = x.concat(_.map(table.join, function (join) {
            if (_.find(self.connections, { table_name: join.with_table })) {
                return { a: '`' + self.table_name + '`.`' + join.key[0] + '`', b: '`' + join.with_table + '`.`' + join.key[1] + '`' };
            }

            return [];
        }));
    }
    if (table.fk) {
        x = x.concat(_.map(table.fk, function (table_name) {

            if (_.find(self.connections, { table_name: table_name })) {
                var t = Table.getTable(table_name);
                var join = _.find(t.join, { with_table: self.table_name });
                return { a: '`' + self.table_name + '`.`' + join.key[1] + '`', b: '`' + table_name + '`.`' + join.key[0] + '`' };
            }

            return [];
        }));
    }

    return x.concat(_.map(self.connections, function (node) {
        return node.getJoins();
    }));
};

Node.prototype.trim = function () {
    this.connections = _.filter(this.connections, function (node) {
        return node.trim();
    });

    return this.valid;
};

Node.prototype.toJSON = function () {
    var clean = _.omit(this, 'parent', 'valid');

    clean.connections = _.map(clean.connections, function (conn) {
        return conn.toJSON();
    });

    return clean;
};

Node.prototype.setValid = function (value) {
    this.valid = value;

    this.parent.setValid(value);
};

var init = function (db) {
    mysql = db;
};

var generate = function (Q) {
    var data_tables = Table.getDataTables();

    return Promise.map(data_tables, function (table) {
        var query = 'SELECT * ' +
                    'FROM `' + table.name + '` ' +
                    'WHERE MATCH (`' + table.search.join('`, `') + '`) ' +
                    'AGAINST (' + mysql.escape(Q.join(' ')) + ') LIMIT 1;';

        return mysql.queryAsync(query).spread(function (rows, fields) {
            return { table: table, valid: rows.length > 0 };
        });
    })
    .filter(function (result) {
        if (result.valid) return true;

        return false;
    })
    .then(function (data) {
        var valid_data_tables = _(data).pluck('table').pluck('name').value();

        return generate_all(valid_data_tables, 10, Q);
    })
    .tap(function (cns) {
        var d = _.flatten(_.map(data_tables, function (table) {
            return _.map(Q, function (keyword) {
                return _.map(table.search, function (field) {
                    return { table_name: table.name, keyword: keyword, field: field };
                });
            });
        }));
        
        return Promise.map(d, function (e) {
            var query = 'SELECT * ' +
                    'FROM `' + e.table_name + '` ' +
                    'WHERE MATCH (`' + e.field + '`) ' +
                    'AGAINST (' + mysql.escape(e.keyword) + ') LIMIT 1;';

            return mysql.queryAsync(query).spread(function (rows, fields) {
                return { data: e, valid: rows.length > 0 };
            });
        })
        .then(function (g) {
            _.forEach(cns, function (cn) {
                cn.setScoreTable(g);
            });
        });
    });
};

function generate_all (valid, max_depth, Q) {
    return _(max_depth).times(function (i) {
        return _.map(_.pluck(Table.getDataTables(), 'name'), function (table_name) {
            return generate_n(valid, new CandidateNetwork(table_name, Q), 0, i).trim();
        });
    })
    .flatten()
    .uniq(function (cn) {
        return JSON.stringify(cn.getGraph());
    })
    .value();
}

function generate_n (valid, network, i, limit) {
    if (~valid.indexOf(network.table_name)) {
        network.setValid(true);
    }

    if (i > limit) return network;

    var table = Table.getTable(network.table_name);

    if (table.fk) {
        network.connections = network.connections.concat(_(table.fk)
        .map(function (table_name) {
            return generate_n(valid, new Node(network, table_name), i + 1, limit);
        })
        .value());
    }

    if (table.join) {
        network.connections = network.connections.concat(_(table.join)
        .filter(function (join) {
            return ~valid.indexOf(join.with_table) &&
                join.with_table !== network.parent.table_name &&
                join.with_table !== network.parent.parent.table_name &&
                join.with_table !== network.parent.parent.parent.table_name;
        })
        .map(function (join) {
            return generate_n(valid, new Node(network, join.with_table), i + 1, limit);
        })
        .value());
    }

    return network;
}

module.exports = {
    init: init,
    generate: generate
};
