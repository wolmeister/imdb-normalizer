const mysql = require('mysql2/promise');
const _ = require('lodash');

async function doWork() {
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'imdb2',
  });

  await connection.execute(
    'CREATE TABLE IF NOT EXISTS title_directors ( tconst INT NOT NULL, nconst INT NOT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8;'
  );
  await connection.execute(
    'CREATE TABLE IF NOT EXISTS title_writers ( tconst INT NOT NULL, nconst INT NOT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8;'
  );

  let [results] = await connection.query('select * from title_crew');
  let directors = [];
  let writers = [];

  results.forEach(result => {
    if (result.directors) {
      result.directors.split(',').forEach(d => {
        directors.push('(' + result.tconst + ',' + parseInt(d) + ')');
      });
    }
    if (result.writers) {
      result.writers.split(',').forEach(w => {
        writers.push('(' + result.tconst + ',' + parseInt(w) + ')');
      });
    }
  });

  _.chunk(directors, 20000).forEach(values => {
    connection.execute('INSERT INTO title_directors VALUES' + values.join(','));
  });

  _.chunk(writers, 20000).forEach(values => {
    connection.execute('INSERT INTO title_writers VALUES' + values.join(','));
  });

  directors = [];
  writers = [];

  await connection.execute(
    'CREATE TABLE IF NOT EXISTS title_genres ( tconst INT NOT NULL, genre VARCHAR(200) NOT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8;'
  );

  [results] = await connection.query('select tconst, genres from title_basics');
  let genres = [];

  results.forEach(result => {
    if (result.genres) {
      result.genres.split(',').forEach(g => {
        genres.push('(' + result.tconst + ',"' + g + '")');
      });
    }
  });

  _.chunk(genres, 20000).forEach(values => {
    connection.execute('INSERT INTO title_genres VALUES' + values.join(','));
  });
}

doWork();
