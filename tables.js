var _ = require('lodash');

var Table = {};

Table.data = {
    movies: {
        name: 'movies',
        search: ['title'],
        fk: ['genre', 'direct'],
        fields: ['title']
    },
    genres: {
        name: 'genres',
        search: ['genre'],
        fk: ['genre'],
        fields: ['genre']
    },
    directors: {
        name: 'directors',
        search: ['name'],
        fk: ['direct'],
        fields: ['name']
    },
    actors: {
        name: 'actors',
        search: ['name'],
        fk: ['act'],
        fields: ['name']
    },
    genre: {
        name: 'genre',
        join: [{
            with_table: 'movies',
            key: ['movie_id', 'id']
        },
        {
            with_table: 'genres',
            key: ['genre_id', 'id']
        }],
        fields: ['movie_id', 'genre_id']
    },
    direct: {
        name: 'direct',
        join: [{
            with_table: 'movies',
            key: ['movie_id', 'id']
        },
        {
            with_table: 'directors',
            key: ['director_id', 'id']
        }],
        fields: ['director_id', 'movie_id']
    },
    act: {
        name: 'act',
        search: ['character_name'],
        join: [{
            with_table: 'movies',
            key: ['movie_id', 'id']
        },
        {
            with_table: 'actors',
            key: ['actor_id', 'id']
        }],
        fields: ['actor_id', 'movie_id', 'character_name']
    }
};

_.forEach(Table.data, function (table) {
    table.fields.unshift('id');
});



Table.getTable = function (table_name) {
    return Table.data[table_name];
}

Table.getDataTables = _.memoize(function () {
    return _.filter(Table.data, function (table) {
        return !!table.search;
    });
});

Table.isDataTable = function (table_name) {
    return !!Table.getTable(table_name).search;
};

module.exports = Table;
