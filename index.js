const mysql = require('mysql2/promise');
const _ = require('lodash');

async function doWork() {
  const connection = await mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'imdb2',
    connectionLimit: 50,
  });

  await connection.execute(
    'CREATE TABLE IF NOT EXISTS title_directors ( tconst INT NOT NULL, nconst INT NOT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8;'
  );
  await connection.execute(
    'CREATE TABLE IF NOT EXISTS title_writers ( tconst INT NOT NULL, nconst INT NOT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8;'
  );

  await connection.execute('DELETE FROM title_directors');
  await connection.execute('DELETE FROM title_writers');

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

  console.log('directors.length', directors.length);
  _.chunk(directors, 20000).forEach(values => {
    connection.execute('INSERT INTO title_directors VALUES' + values.join(','));
  });

  console.log('writers.length', writers.length);
  _.chunk(writers, 20000).forEach(values => {
    connection.execute('INSERT INTO title_writers VALUES' + values.join(','));
  });

  directors = [];
  writers = [];

  // genres
  await connection.execute(
    'CREATE TABLE IF NOT EXISTS title_genres ( tconst INT NOT NULL, genre VARCHAR(200) NOT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8;'
  );
  await connection.execute('DELETE FROM title_genres');

  [results] = await connection.query('select tconst, genres from title_basics');
  let genres = [];

  results.forEach(result => {
    if (result.genres) {
      result.genres.split(',').forEach(g => {
        genres.push('(' + result.tconst + ',"' + g + '")');
      });
    }
  });

  console.log('genres.length', genres.length);
  _.chunk(genres, 20000).forEach(values => {
    connection.execute('INSERT INTO title_genres VALUES' + values.join(','));
  });

  genres = [];

  // known for title
  await connection.execute(
    'CREATE TABLE IF NOT EXISTS known_for_titles ( nconst INT NOT NULL, tconst INT NOT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8;'
  );
  await connection.execute('DELETE FROM known_for_titles');

  [results] = await connection.query(
    'select nconst, knownForTitles from name_basics'
  );
  let titles = [];

  results.forEach(result => {
    if (result.knownForTitles) {
      result.knownForTitles.split(',').forEach(t => {
        titles.push('(' + result.nconst + ',' + parseInt(t) + ')');
      });
    }
  });

  console.log('titles.length', titles.length);
  _.chunk(titles, 20000).forEach(values => {
    connection.execute(
      'INSERT INTO known_for_titles VALUES' + values.join(',')
    );
  });
}

doWork();
