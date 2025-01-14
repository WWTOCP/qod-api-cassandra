const express = require('express')
const pg = require('pg')

var app = express()
app.set('port',process.env.PORT || 8081)

const { Client } = pg
const dbClient = new Client({
    host: 'localhost',       // CockroachDB host
    port: 26257,             // Default CockroachDB port
    user: 'root',            // Username
    password: '',            // Password (if any, leave empty for default)
    database: 'qod',         // Specify the database you want to use
    ssl: false               // Set to true if using SSL, or configure with cert details
})
dbClient.connect()

function logMsg( msg ) {
    console.log(msg)
}

function logErr( err) {
    console.error(err)
}

app.use(express.urlencoded({ extended: true }))
app.use(express.json())
app.enable('trust proxy')

function getRandomInt(max) {
	return Math.floor(Math.random() * Math.floor(max))
}

/*

const pool  = pg.createPool({
	host     : process.env.DB_HOST,
	user     : process.env.DB_USER,
	password : process.env.DB_PASS,
    database : 'qod',
    insecureAuth : true
});
*/

var getConnection = function(res,callback) {
    logMsg('getting connection from pool')
    pool.getConnection(function(err, connection) {
        if( err ) {
            logErr('error getting connection: ' + err)
            res.status(500).json({"error": err })
            return
        }
        callback(connection)
    })
};

var getCRConnection = function(res, callback) {
    const dbClient = new Client({
        host: 'localhost',       // CockroachDB host
        port: 26257,             // Default CockroachDB port
        user: 'root',            // Username
        password: '',            // Password (if any, leave empty for default)
        database: 'qod',         // Specify the database you want to use
        ssl: false               // Set to true if using SSL, or configure with cert details
    })
    dbClient.connect()
    callback(dbClient)
}

// function between(min, max) {  
//     return Math.floor(
//       Math.random() * (max - min) + min
//     )
// }

function dailyQuoteId(){
    // assumes the order of the database is random, and day of year is same as quote id.
    var now = new Date()
    var start = new Date(now.getFullYear(), 0, 0)
    var diff = (now - start) + ((start.getTimezoneOffset() - now.getTimezoneOffset()) * 60 * 1000)
    var oneDay = 1000 * 60 * 60 * 24
    var day = Math.floor(diff / oneDay)
    return day
}

app.get('/author/:id',
    async function(req, res) {
        console.log('author endpoint executing...')
        try {
            //const dbres = dbClient.query('SELECT $1::text as message', ['Hello world!'])
            //console.log(dbres.rows[0].message) // Hello world!
            var authorId = req.params.id;
            const dbres = await dbClient.query('select author from authors where author_id = $1', [authorId])
            console.log(`# of db rows: ${dbres.rows.length}`)
            console.log(`Author Id: ${authorId} name is: ${dbres.rows[0].author}`)
            res.json(dbres.rows[0])
        } catch (ex) {
            console.log(ex.message)
        }
    }
)

app.get('/daily', 	
	async function(req, res) {
        logMsg('request: /daily')
        //var quoteId = dailyQuoteId()
        console.log(Number.MAX_SAFE_INTEGER); // 9007199254740991 or 9,007,199,254,740,991

        let quoteId = 1037839461057462273n
        console.log(`Attempting to retrieve quoteId: ${quoteId}`)
        var sql = `SELECT
                        quotes.quote_id, quotes.quote, authors.author, genres.genre
                    FROM
                        quotes, authors, genres
                    WHERE 
                        quotes.quote_id = $1
                        AND quotes.author_id = authors.author_id
                        AND quotes.genre_id = genres.genre_id;`
        console.log(`SQL = ${sql}`)
        try {
            const dbres = await dbClient.query(sql, [quoteId])
            console.log(`# of db rows: ${dbres.rows.length}`)
            console.log(`Quote Id: ${quoteId} author name is: ${dbres.rows[0]?.author}`)
            if( dbres.rows.length > 0 ) {
                const quoteRow = dbres.rows[0]
                res.json( { "source": "CockroachDB", "quote": quoteRow.quote, "id": quoteRow.quote_id, "author": quoteRow.author, "genre": quoteRow.genre } )
            } else {
                res.status(500).json({"error": `quote id ${quoteId} doesn't exist.` })
            }
        } catch (ex) {
            console.log(ex)
            res.status(500).json({"error": ex })
        }
    }
)

app.get('/quotes/:id', function(req,res) {
	var quote_id = req.params.id
    logMsg('request: /quotes/' + quote_id)
	getCRConnection(res, function(connection) {
        var sql = `SELECT
                        quotes.quote_id, quotes.quote, authors.author, genres.genre
                    FROM
                        quotes, authors, genres
                    WHERE
                        quote_id = $1 
                        AND quotes.author_id = authors.author_id 
                        AND quotes.genre_id = genres.genre_id;`;
        logMsg('query sql: ' + sql)
		connection.query(sql, [quote_id], function (error, resp) {
            logMsg('sql query completed')
            if( error ) {
                logErr(error)
				res.status(500).json({"error": err })
			} else {
				if( resp.rows.length > 0 ) {
                    logMsg('sql query completed, rows: ' + resp.rows.length)
                    const quoteRow = resp.rows[0]
					res.json( { "quote": quoteRow.quote, "id": quoteRow.quote_id, "author": quoteRow.author, "genre": quoteRow.genre } )
				} else {
                    logErr('quote id ['+quote_id+'] not found')
					res.status(404).json({"error": "quote id '" + quote_id + "' doesn't exist." })
                }
                //logMsg('connection releasing');
				//connection.release();
			}
		});
	});
});

app.get('/random',  
	function(req, res) {
        logMsg('request: /random');
		getConnection(res,function(connection){
            var sql = "SELECT COUNT(*) AS quote_count FROM quotes";
            logMsg('query sql: ' + sql)
            connection.query(sql, function (error, results, fields) {
                if( error ) {
                    logErr(error);
                    res.status(500).json({"error": error });
                } else {
                    var count = results[0].quote_count;
                    var quote_id = getRandomInt(count);
                    var sql = "SELECT quotes.quote_id, quotes.quote, authors.author, genres.genre FROM quotes, authors, genres WHERE quote_id=? and quotes.author_id=authors.author_id and quotes.genre_id=genres.genre_id ;";
                    logMsg('query sql: ' + sql + ', count: ' + count + ' quote_id: ' + quote_id );
                    connection.query(sql, [quote_id], function (error, rows, fields) {
                        if( error ) {
                            logErr(error);
                            res.status(500).json({"error": error });
                        } else {
                            logMsg('Randome quote from ' + rows[0].author );
                            res.json( { "source": "CockroachDB", "quote": rows[0].quote, "id": rows[0].quote_id, "author": rows[0].author, "genre": rows[0].genre } );	
                        }
                        logMsg('connection releasing');
                        connection.release();
                    });
                }
            });
		});
	}
);

app.get('/genres',  
	function(req, res) {
        logMsg('request: /genres');
		getConnection(res,function(connection){
            var sql = "SELECT genres.genre_id, genres.genre FROM qod.genres";
            logMsg('query sql: ' + sql)
            connection.query(sql, function (error, rows, fields) {
                if( error ) {
                    logErr(error);
                    res.status(500).json({"error": error });
                } else {
                    logMsg('genre rows returns: '+rows.length);
                    res.json( rows );	
                }
                logMsg('connection releasing');
                connection.release();
            });
		});
	}
);

app.get('/genres/:id', function(req,res) {
    var genre_id=req.params.id;
    logMsg('request: /genres/'+genre_id);
	getConnection(res,function(connection){
        var sql = "SELECT genres.genre_id, genres.genre FROM genres WHERE genres.genre_id=?;";
        logMsg('query sql: ' + sql);
		connection.query(sql, [genre_id], function (error, rows, fields) {
			if( error ) {
                logErr(error);
				res.status(500).json({"error": err });
			} else {
                logMsg('sql query completed, rows: '+rows.length);
				if( rows.length > 0 ) {
					res.json( { "genre_id": rows[0].genre_id, "genre": rows[0].genre } );	
				} else {
                    var erObj = {"error": "genre id '"+ genre_id + "' doesn't exist." };
                    logErr(erObj);
					res.status(404).json(erObj);
				}
            }
            logMsg('connection releasing');
            connection.release();
		});
	});
});

app.get('/',  
	function(req, res) {
        logMsg('root requested, redirecting to version');
		res.redirect('/version');
	}
);

app.get('/version',  
	function(req, res) {
        logMsg('/version');
		res.send(appVersion);
	}
);

const package = require('./package.json');
const appName = package.name;
const appVersion = package.version;

console.log(`Starting ${appName} v${appVersion}.`);

app.listen(app.get('port'), '0.0.0.0', function() {
	  console.log("Now serving quotes on port " + app.get('port'));
});

function shutdown() {
    console.log('Shutting down server...');
    dbClient.end();
    server.close(() => {
      console.log('Server closed.');
      process.exit(0); // Exit the process
    });
}

// Listen for termination signals (e.g., Ctrl+C, Docker stop)
//process.on('SIGINT', shutdown);  // Ctrl+C in the terminal
//process.on('SIGTERM', shutdown); // Termination signal from the system



	
