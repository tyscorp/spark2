var Promise = require('bluebird');
var _ = require('lodash');

var mysql;

var data_tables = {
    movies: {
        search: ['title'],
        fk: ['genre', 'direct']
    },
    genres: {
        search: ['genre'],
        fk: ['genre']
    },
    directors: {
        search: ['name'],
        fk: ['direct']
    }
};

var join_tables = {
    genre: {
        join: [{
            data_table: 'movies',
            key: ['movie_id', 'id']
        },
        {
            data_table: 'genres',
            key: ['genre_id', 'id']
        }]
    },
    direct: {
        join: [{
            data_table: 'movies',
            key: ['movie_id', 'id']
        },
        {
            data_table: 'directors',
            key: ['director_id', 'id']
        }]
    }
};

function getTable (table_name) {
    return data_tables[table_name] || join_tables[table_name];
}

function isDataTable (table_name) {
    return !!~Object.keys(data_tables).indexOf(table_name);
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
CandidateNetwork.prototype.exec = function () {
    var q = mysql.escape(this.Q.join(' '));
    var tables = this.getTables().join(', ');
    var joins = this.getJoins().join(' AND ');

    var queries = _.map(this.getMatches(q), function (match) {
        return 'SELECT * ' +
               'FROM ' +
               tables + ' ' +
               'WHERE ' + joins + ' ' +
               'AND ' + match + ';'
    });

    return Promise.map(queries, function (query) {
        return mysql.queryAsync(query).get(0).tap(function () { console.log(query); });    
    })
    .then(function (results) {
        if (results.length === 1) {
            return results[0];
        }

        var ids = _.map(results, function (rows) {
            return _(rows).pluck('id').value();
        });

        var valid = _.intersection.apply(_, ids);

        return _.map(valid, function (id) {
            return _.merge.apply(_, _.map(results, function (rows) {
                return _.find(rows, { id : id });
            }));
        });
    });      
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

CandidateNetwork.prototype.getScore = function () {
    // This is DISCOVER, not SPARK2
    return this.scoreTable.length;
};

CandidateNetwork.prototype.getJoins = function () {
    if (this.connections.length === 0) return [1];

    var self = this;

    var x = [];

    if (!isDataTable(self.table_name)) {
        var table = getTable(self.table_name);

        x = _.map(table.join, function (join) {
            if (_.find(self.connections, { table_name: join.data_table })) {
                return { 1: '`' + self.table_name + '`.`' + join.key[0] + '`', 2: '`' + join.data_table + '`.`' + join.key[1] + '`' };
            }

            return [];
        });
    }
    else {
        var table = getTable(self.table_name);

        x = _.map(table.fk, function (table_name) {

            if (_.find(self.connections, { table_name: table_name })) {
                var t = getTable(table_name);
                var join = _.find(t.join, { data_table: self.table_name });
                return { 1: '`' + self.table_name + '`.`' + join.key[1] + '`', 2: '`' + table_name + '`.`' + join.key[0] + '`' };
            }

            return [];
        });
    }

    return _(x.concat(_.map(self.connections, function (node) {
        return node.getJoins();
    })))
    .compact()
    .flatten()
    .compact()
    .map(function (tuple) {
        var t = [tuple[1], tuple[2]].sort();
        return t[0] + '=' + t[1];
    })
    .uniq()
    .sort()
    .value();
};

CandidateNetwork.prototype.getMatches = function (q) {
    return _(this.getTables())
    .filter(function (table_name) {
        return ~Object.keys(data_tables).indexOf(table_name);
    })
    .map(function (table_name) {
        var table = getTable(table_name);

        var x = _.map(table.search, function (field) {
            return 'MATCH (`' + table_name + '`.`' + field + '`) AGAINST (' + q + ' IN BOOLEAN MODE)'
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
};

CandidateNetwork.prototype.clean = function () {
    var clean = _.omit(this, 'parent', 'valid');

    clean.connections = _.map(clean.connections, function (conn) {
        return conn.clean();
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

Node.prototype.getTables = function () {
    return [this.table_name].concat(_.map(this.connections, function (node) {
        return node.getTables();
    }));
};

Node.prototype.getJoins = function () {
    if (this.connections.length === 0) return null;

    var self = this;

    var x = [];

    if (!isDataTable(self.table_name)) {
        var table = getTable(self.table_name);

        x = _.map(table.join, function (join) {
            if (_.find(self.connections, { table_name: join.data_table })) {
                return { 1: '`' + self.table_name + '`.`' + join.key[0] + '`', 2: '`' + join.data_table + '`.`' + join.key[1] + '`' };
            }

            return [];
        });
    }
    else {
        var table = getTable(self.table_name);

        x = _.map(table.fk, function (table_name) {

            if (_.find(self.connections, { table_name: table_name })) {
                var t = getTable(table_name);
                var join = _.find(t.join, { data_table: self.table_name });
                return { 1: '`' + self.table_name + '`.`' + join.key[1] + '`', 2: '`' + table_name + '`.`' + join.key[0] + '`' };
            }

            return [];
        });
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

Node.prototype.clean = function () {
    var clean = _.omit(this, 'parent', 'valid');

    clean.connections = _.map(clean.connections, function (conn) {
        return conn.clean();
    });

    return clean;
};

Node.prototype.setValid = function (value) {
    this.valid = value;

    this.parent.setValid(value);
};

Node.prototype.isDataTable = function () {
    return isDataTable(this.table_name);
};

var init = function (db) {
    mysql = db;
};

var generate = function (Q) {
    return Promise.map(Object.keys(data_tables), function (table_name) {
        var table = getTable(table_name);

        var query = 'SELECT * ' +
                    'FROM `' + table_name + '` ' +
                    'WHERE MATCH (`' + table.search.join('`, `') + '`) ' +
                    'AGAINST (' + mysql.escape(Q.join(' ')) + ') LIMIT 1;';

        return mysql.queryAsync(query).spread(function (rows, fields) {
            return { table_name: table_name, valid: rows.length > 0 };
        });
    })
    .filter(function (result) {
        if (result.valid) return true;

        return false;
    })
    .then(function (data) {
        var valid_data_tables = _.pluck(data, 'table_name');

        return generate_all(valid_data_tables, 10, Q);
    })
    .filter(function (cn) {
        return cn.valid;
    })
    .map(function (cn) {
        cn.trim();

        return cn;
    })
    .then(function (cns) {
        return _(cns)
        .map(function (cn) {
            return { query: cn.getQuery(), cn: cn };
        })
        .uniq(function (cn) {
            return cn.query;
        })
        .pluck('cn')
        .value();
    })
    .tap(function (cns) {
        var tables = Object.keys(data_tables);

        var d = _.flatten(_.map(tables, function (table_name) {
            return _.map(Q, function (keyword) {
                var table = getTable(table_name);

                return _.map(table.search, function (field) {
                    return { table_name: table_name, keyword: keyword, field: field };
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
    var tables = Object.keys(data_tables);

    return _.flatten(_.times(max_depth, function (i) {
        return _.map(valid, function (table_name) {
            return generate_n(valid, new CandidateNetwork(table_name, Q), 0, i);
        });
    }));
}

function generate_n (valid, network, i, limit) {
    if (~valid.indexOf(network.table_name)) {
        network.setValid(true);
    }

    if (i > limit) return network;

    var table = getTable(network.table_name);

    if (table.fk) {
        network.connections = _(table.fk)
        .map(function (table_name) {
            return generate_n(valid, new Node(network, table_name), i + 1, limit);
        })
        .value();
    }

    if (table.join) {
        network.connections = _(table.join)
        .filter(function (join) {
            return ~valid.indexOf(join.data_table) &&
                join.data_table !== network.parent.table_name &&
                join.data_table !== network.parent.parent.table_name &&
                join.data_table !== network.parent.parent.parent.table_name;
        })
        .map(function (join) {
            
            return generate_n(valid, new Node(network, join.data_table), i + 1, limit);
        })
        .value();
    }

    return network;
}

module.exports = {
    init: init,
    generate: generate
};
