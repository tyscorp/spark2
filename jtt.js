var _ = require('lodash');
var Table = require('./tables');

function JTT (rows) {
    this.data = {};
    this.results = rows;
    this.score = 0;
    this.type = '';

    this.tf = [];
    this.tfweighted = [];
    this.dl = 0;
}

JTT.prototype.generate = function (cn) {
    this.tables = _(cn.getTables()).filter(Table.isDataTable).value();

    this.data = {};

    var self = this;

    _.forEach(self.results, function (val, key) {
        var k = key.split('_').slice(1).join('_');

        if (key.split('_')[0] === cn.table_name) {
            self.data[k] = val;
        }
        else if (~self.tables.indexOf(key.split('_')[0])) {
            var t = key.split('_')[0];
            self.data[t] = {};
            self.data[t][k] = val;
        }
    });

    this.dl = calcDL(this.data);

};

JTT.prototype.getSize = function () {
    return getSize(this.data);
};

JTT.prototype.calculateScoreA = function (Q, s, avdl, lnidf) {
    var tf = this.tf;
    var tfweighted = this.tfweighted;
    var dl = this.dl;

    this.score_a = _.reduce(Q, function (score, keyword, i) {
        if (!tfweighted[i]) return score;

        var a = 1 + Math.log(1 + Math.log(tfweighted[i]));
        var b = (1 - s) + s * (dl / avdl);

        return score + ((a / b) * (lnidf[i] + a*5));
        return score + (((a / b) * lnidf[i]));
    }, 0);

    return this.score_a;
};

JTT.prototype.calcTF = function (Q) {
    this.tf = _.map(Q, function (regex) {
        return matchKeyword(regex, this.data);
    }, this);

    this.tfweighted = _.map(Q, function (regex) {
        return matchKeyword(regex, this.data, true);
    }, this);
};

JTT.prototype.toJSON = function () {
    var obj = {};
    obj[this.type] = _.merge(this.data, { score: this.score, score_a: this.score_a, score_z: this.score_z });
    return obj;
};

function matchKeyword (regex, tree, w) {
    return _.reduce(tree, function (tf, val, key) {
        if (_.isString(val)) {
            if (regex.test(val)) {
                return tf + 1 + (w ? ((regex.toString().length - 3) / val.length) * 100: 0);
            }
        }
        else {
            return tf + matchKeyword(regex, val, w);
        }

        return tf;
    }, 0);
}

function calcDL (tree) {
    return _.reduce(tree, function (dl, val, key) {
        if (_.isString(val)) {
            dl += val.length;
        }
        else {
            dl += calcDL(val);
        }

        return dl;
    }, 0);
}

function getSize (tree) {
    return _.reduce(tree, function (score, val, key) {
        if (_.isObject(val)) {
            score += getSize(val);
        }

        return score;
    }, 1);
};

module.exports = JTT;
