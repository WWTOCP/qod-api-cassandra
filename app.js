const express = require('express')
const cassandra = require('cassandra-driver');

const client = new cassandra.Client({
  contactPoints: [process.env.DB_HOST || '127.0.0.1'],
  localDataCenter: process.env.DB_DATACENTER || 'datacenter1',
  keyspace: 'qod'
});


var app = express()
app.set('port',process.env.PORT || 8080)

//const { Client } = pg

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

var getConnection = function(res, callback) {
    const dbClient = new cassandra.Client({
        contactPoints: [process.env.DB_HOST || '127.0.0.1'],
        localDataCenter: 'datacenter1', // default data center name
        keyspace: 'qod'
        /*host: process.env.DB_HOST || '127.0.0.1',       // CockroachDB host
        port: 26257,             // Default CockroachDB port
        user: process.env.DB_USER || 'user',            // Username (cockroach --insecure mode will have a 'root' user)
        password: process.env.DB_PASS || 'pass',        // Password (cockroach --insecure mode will have an empty password for the 'root' user)
        database: 'qod',         // Specify the database you want to use
        ssl: {
            enable: true, // disable SSL/TLS if cockroach is running in "--inssecure" mode.
            rejectUnauthorized: false // set to false if using a self-signed cert.
        }*/
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

app.get('/daily', 	
	async function(req, res) {
        logMsg('request: /daily')
        var quoteId = dailyQuoteId()
        getConnection(res, function(connection) {
            var sql = `SELECT
                            id, text, author, genre
                        FROM
                            quotes
                        WHERE 
                            id = ?;`
            connection.execute(sql, [quoteId], { prepare: true })
            .then( result => {
                logMsg('sql query completed')
                if( result.rows.length > 0 ) {
                    logMsg('sql query completed, rows: ' + result.rows.length)
                    const quoteRow = result.rows[0]
                    res.json( { "source": "CockroachDB", "quote": quoteRow.text, "id": quoteRow.id, "author": quoteRow.author, "genre": quoteRow.genre } )
                } else {
                    logErr('quote id [' + quoteId + '] not found')
                    res.status(404).json({"error": "quote id '" + quoteId + "' doesn't exist." })
                }
            })
            .catch(error => {
                logErr(error)
                res.status(500).json({"error": error })
            })
        })
    }
)

app.get('/random',  
	function(req, res) {
        logMsg('request: /random');
		getConnection(res,function(connection){
            var sql = `SELECT
                            COUNT(*) AS quote_count
                        FROM
                            quotes;`;
            logMsg('query sql: ' + sql)
            connection.execute(sql)
            .then(result => {
                var count = result.rows[0].quote_count;
                var quote_id = getRandomInt(count);
                console.log(`random quote id: ${quote_id}`)
                var sql = `SELECT
                                id, text, author, genre
                            FROM
                                quotes
                            WHERE
                                id = ?;`;
                logMsg('query sql: ' + sql + ', quotes row count: ' + count + ' quote_id: ' + quote_id );
                connection.execute(sql, [quote_id], { prepare: true })
                .then(result => {
                    const quoteRow = result.rows[0]
                    logMsg('Random quote from ' + quoteRow.author );
                    res.json( { "source": "CockroachDB", "quote": quoteRow.text, "id": quoteRow.id, "author": quoteRow.author, "genre": quoteRow.genre } );	
                })
                .catch(error => {
                    // Expected 4 or 0 byte int (8)
                    logErr(error);
                    res.status(500).json({"error": error });
                })
            })
            .catch(error => {
                logErr(error);
                res.status(500).json({"error": error });
            })
        });
	}
);

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
      console.log("Envrionment Varibles: ")
      for (const key in process.env) {
        console.log(`${key}: ${process.env[key]}`);
      }
});



	
