var _ = require('lodash');
var Table = require('./tables');

function JTT (rows) {
    this.data = {};
    this.results = rows;
    this.score = 0;
    this.type = '';

    this.tf = [];
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
    var dl = this.dl;

    if (this.data.id === 452) {
        console.log(tf, dl);
    }

    this.score_a = _.reduce(Q, function (score, keyword, i) {
        if (!tf[i]) return score;
        return score + (((1 + Math.log(1 + Math.log(tf[i]))) / ((1 - s) + s * (dl / avdl))) * lnidf[i]);
    }, 0);

    return this.score_a;
};

JTT.prototype.calcTF = function (Q) {
    this.tf = _.map(Q, function (regex) {
        return matchKeyword(regex, this.data);
    }, this);
};

JTT.prototype.toJSON = function () {
    var obj = {};
    obj[this.type] = _.merge(this.data, { score_a: this.score_a });
    return obj;
};

function matchKeyword (regex, tree) {
    return _.reduce(tree, function (tf, val, key) {
        if (_.isString(val)) {
            if (regex.test(val)) {
                return tf + 1;
            }
        }
        else {
            return tf + matchKeyword(regex, val);
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
