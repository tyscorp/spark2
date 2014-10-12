var _ = require('lodash');

function VirtualDocument (cn, jtts) {
    _.forEach(jtts, function (jtt) {
        jtt.generate(cn);
    });

    this.data = jtts;
    this.cn = cn;
}

VirtualDocument.prototype.calculateScores = function (Q) {
    var self = this;

    var regexs = _.map(Q, function (keyword) {
        return new RegExp(keyword, 'i');
    });

    // pre-calculate term frequency for every tuple tree
    _.forEach(self.data, function (jtt) {
        jtt.calcTF(regexs);
    });

    // document frequency of terms
    var df = _.map(Q, function (keyword, index) {
        return _.reduce(self.data, function (n, jtt) {
            return n + jtt.tf[index];
        }, 0);
    });

    var N = this.data.length;

    // cached ln of df
    var lnidf = _.map(df, function (dfw) {
        return Math.log((N + 1) / dfw);
    });

    // calculate average document length
    var avdl = _.reduce(this.data, function (n, jtt) {
        return n + jtt.dl;
    }, 0) / this.data.length;

    _.forEach(self.data, function (jtt) {
        jtt.type = self.cn.table_name;
        if (jtt.type.substr(jtt.type.length - 1) === 's') jtt.type = jtt.type.substr(0, jtt.type.length - 1);

        var score_a = jtt.calculateScoreA(Q, 0.2, avdl, lnidf);

        //jtt.score = jtt.score_a// * (1 + Math.log(1 + Math.log(1 - self.cn.getNormalizationScore())));
        jtt.score_z = jtt.score_a * (1.5 / self.cn.getNormalizationScore());
        jtt.score = jtt.score_z;
    });
    
};

module.exports = VirtualDocument;
