const express = require('express')
const cassandra = require('cassandra-driver');
const http = require('http');
const https = require('https');

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
        localDataCenter: 'datacenter1', // yes, the default data center is 'datacenter1'
        keyspace: 'qod'
    })
    dbClient.connect()
    callback(dbClient)
}

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
                    res.json( { "source": "cassandra", "quote": quoteRow.text, "id": quoteRow.id, "author": quoteRow.author, "genre": quoteRow.genre } )
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
                    res.json( { "source": "cassandra", "quote": quoteRow.text, "id": quoteRow.id, "author": quoteRow.author, "genre": quoteRow.genre } );	
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

app.get('/httpclient', 
    function(req, res) {
        // Choose the correct module based on the protocol
        const url = req.query.url
        if (!url) {
            return res.status(400).json({
                "endpoint": "/httpclient",
                "error": "Missing 'url' query parameter"
            });
        }
        const protocol = url.startsWith('https') ? https : http;
        const options = new URL(url); // Parses the URL into hostname, path, and protocol
        const httpReq = protocol.request(options, (httpRes) => {
            let data = '';
            httpRes.on('data', (chunk) => {
                data += chunk;
            });
            httpRes.on('end', () => {
                res.json ( {"endpoint" : "/httpclient", "url" : url, "httpStatusCode" : httpRes.statusCode, "output" : data.substring(0, data.length > 500 ? 500 : data.length)})
                console.log(data);
            });
        });
        httpReq.end()
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



	
