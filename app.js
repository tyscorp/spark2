var express = require('express');
var Promise = require('bluebird');
var _ = require('lodash');
var MySQL = require('mysql');
var CN = require('./cn');
var VirtualDocument = require('./vdoc');

Promise.longStackTraces();

var app = express();
var mysql = Promise.promisifyAll(MySQL.createPool(require('./config')));

var ye = Promise.all(_.times(1000, function () {
    return mysql.queryAsync('USE `kit306`');
})).then(function () {
    CN.init(mysql);
});

app.get('/search', function (req, res) {
    if (!req.param('query')) return res.status(500).json({ error: 'no query' });

    var Q = req.param('query').split(' ');
    var k = parseInt(req.param('k')) || 10;

    ye.then(function () {
        CN.generate(Q).map(function (cn) {
            cn.score = cn.getNormalizationScore();
            cn.query = cn.getQuery();
            return cn;
        })
        .then(function (cns) {
            return _(cns).sortBy('score').reverse().value();
        })
        .map(function (cn) {
            return cn.exec().then(function (jtts) {
                var vdoc = new VirtualDocument(cn, jtts);

                vdoc.calculateScores(Q);

                return vdoc;
            });
        }, { concurrency: 1 })
        .then(function (results) {
            return _(results).pluck('data').flatten().sortBy('score').flatten().reverse().slice(0, k).value();
        })
        .then(function (data) {
            res.set({
                'Content-Type': 'application/json; charset=utf-8'
            })
            .status(200)
            .send(JSON.stringify(data, undefined, '    '));
        })
        .catch(function (error) {
            //console.log(error);
            res.status(500).send({ error: error });

            throw error;
        });
    });
});

app.get('/tree', function (req, res) {
    if (!req.param('query')) return res.status(500).json({ error: 'no query' });

    var Q = req.param('query').split(' ');
    var k = parseInt(req.param('k')) || 10;

    ye.then(function () {
        CN.generate(Q).map(function (cn) {
            cn.score = cn.getNormalizationScore();
            cn.query = cn.getQuery();
            return cn;
        })
        .then(function (cns) {
            return _(cns).sortBy('score').reverse().value();
        })
        .map(function (cn) {
            return cn.exec().then(function (jtts) {
                return new VirtualDocument(cn, jtts);
            });
        }, { concurrency: 1 })
        .then(function (data) {
            res.set({
                'Content-Type': 'application/json; charset=utf-8'
            })
            .status(200)
            .send(JSON.stringify(data, undefined, '    '));
        })
        .catch(function (error) {
            //console.log(error);
            res.status(500).send({ error: error });

            throw error;
        });
    });
});

app.get('/graph', function (req, res) {
    if (!req.param('query')) return res.status(500).json({ error: 'no query' });

    var Q = req.param('query').split(' ');
    var k = parseInt(req.param('k')) || 10;

    ye.then(function () {
        CN.generate(Q).map(function (cn) {
            cn.score = cn.getNormalizationScore();
            cn.query = cn.getQuery();
            cn.graph = cn.getGraph();
            return cn;
        })
        .then(function (cns) {
            return _(cns)
            .sortBy('score')
            .reverse()
            .map(function (cn) {
                return _.omit(cn, 'connections', 'Q', 'scoreTable');
            })
            .value();
        })
        .then(function (data) {
            res.set({
                'Content-Type': 'application/json; charset=utf-8'
            })
            .status(200)
            .send(JSON.stringify(data, undefined, '    '));
        });
    });
});

var server = app.listen(3000, function() {
    console.log('Listening on port %d', server.address().port);
});
