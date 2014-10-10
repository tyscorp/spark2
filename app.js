var express = require('express');
var Promise = require('bluebird');
var _ = require('lodash');
var MySQL = require('mysql');
var CN = require('./cn');

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
            cn.score = cn.getScore();
            cn.query = cn.getQuery();
            return cn;
        })
        .then(function (cns) {
            return _(cns).sortBy('score').reverse().value();
        })
        .map(function (cn) {
            var time = Date.now();

            return cn.exec().map(function (result) {
                result.type = cn.table_name;

                // temporary scoring function
                result.score = cn.score * _.reduce(Q, function (score, keyword, index) {
                    if (result.title && new RegExp(keyword, 'i').test(result.title)) {
                        score += 1 + (keyword.length / result.title.length);
                        return score;
                    }
                    if (result.name && new RegExp(keyword, 'i').test(result.name)) {
                        score += 1 + (keyword.length / result.name.length);
                        return score;
                    }

                    return score;
                }, 0);

                return result;
            });
        }, { concurrency: 1 })
        .then(function (results) {
            return _(results).flatten().sortBy('score').reverse().uniq('id').slice(0, k).value();
        })
        .then(function (data) {
            res.set({
                'Content-Type': 'application/json; charset=utf-8'
            })
            .status(200)
            .send(JSON.stringify(data, undefined, '    '));
        })
        .catch(function (error) {
            console.log(error);
            res.status(500).send({ error: error });
        });
    });
});

app.get('/cn', function (req, res) {
    if (!req.param('query')) return res.status(500).json({ error: 'no query' });

    var Q = req.param('query').split(' ');
    var k = parseInt(req.param('k')) || 10;

    ye.then(function () {
        CN.generate(Q).map(function (cn) {
            cn.score = cn.getScore();
            cn.query = cn.getQuery();
            return cn;
        })
        .then(function (cns) {
            return _(cns)
            .sortBy('score')
            .reverse()
            .map(function (cn) {
                return cn.clean(); // remove circular
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
