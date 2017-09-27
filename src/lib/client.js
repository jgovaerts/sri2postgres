/**
 * Created by pablo on 23/07/15.
 */

// var memwatch = require('memwatch-next');
const request = require('requestretry');
// var pg = require('pg');
//var Q = require('q');
//var Transaction = require('pg-transaction');
//var QueryStream = require('pg-query-stream');
//var retry = require('retry');



const Promise = require("bluebird");

const monitor = require("pg-monitor");

const options = {
    // your pg-promise initialization options;
};

monitor.attach(options); // attach to all events at once;

monitor.setLog((msg, info) => {
    console.log(msg)
    console.log(info)
    // save the screen message into your own log;
});

var pgp = require('pg-promise')(options);

// Constructor
function Client (config) {

    config = config || {};

    if (!config.hasOwnProperty('baseApiUrl')) {
        throw new Error('Api is not defined.');
    }

    this.baseApiUrl = config.baseApiUrl;
    this.functionApiUrl = config.functionApiUrl;
    this.apiCredentials = config.hasOwnProperty('credentials') ? config.credentials : {};
    this.apiHeaders = config.hasOwnProperty('headers') ? config.headers : {};
    this.apiRetries = config.hasOwnProperty('apiRetries') ? config.apiRetries : 2;

    this.dbTable = config.dbTable;

    this.resourceType = config.hasOwnProperty('resourceType') ? config.resourceType : 'document';
    this.requiredByRoot = config.hasOwnProperty('requiredByRoot') ? config.requiredByRoot : undefined;

    this.encodeURL = config.hasOwnProperty('encodeURL') ? config.encodeURL : true;

    this.apiTimeOut = config.hasOwnProperty('apiTimeOut') ? config.apiTimeOut : 0;

    this.lastSync = null;


    pgp.pg.defaults.ssl = (process.env.DATABASE_URL!=undefined); //use SSL for heroku

    // this.postgresClient = null;
    // this.createPostgresClient = function(){
    //     this.postgresClient = pgp(process.env.DATABASE_URL || config.db);
    // };
    this.postgresClient = config.db;

    this.updateDateSync = function() {
        this.lastSync = new Date();
    };

}

var totalSync = 0;
var totalNotSync = 0;

String.prototype.replaceAll = function(search, replace) {
    if (replace === undefined) {
        return this.toString();
    }
    return this.split(search).join(replace);
};

// var insertResources = async function(composeObject, client) {

//     console.log('insertResources')

//     var jsonData = composeObject.jsonData;
//     var count = jsonData.body.results.length;
//     var inserted = 0;

//     var insertQuery;

//     console.log('starting db transaction')
//     // console.log(this.Client)
//     // client = this.Client
// //    result = await this.Client.postgresClient.tx(async function (t) {
//     result = await client.postgresClient.tx(async function (t) {
//         // `t` and `this` here are the same;
//         // creating a sequence of transaction queries:

//         ql = []
//         jsonData.body.results.forEach( (j) => {
//             console.log(process.memoryUsage()); 
//             var key = j.$$expanded.key;
//             var stringifiedJson = JSON.stringify(j.$$expanded);
//             stringifiedJson = stringifiedJson.replaceAll("'", "''");
//             insertQuery  = "INSERT INTO "+client.dbTable+" VALUES ('"+key+"','"+stringifiedJson+"','"+client.resourceType+"')";
//             ql.push(t.none(insertQuery))
//         })
//         return t.batch(ql);
//     })
//     console.log('transaction done.')
//     return result.length
// };

// var updateData = function(jsonData){
//     var deferred = Q.defer();
//     var key = jsonData.body.key;
//     var updateQuery  = "UPDATE "+this.Client.dbTable+" SET details = '"+JSON.stringify(jsonData.body)+"' WHERE key = '"+key+"'";

//     this.Client.postgresClient.query(updateQuery, function (error, result) {
//         if (error) {
//             deferred.reject(new Error(error));
//         } else {
//             deferred.resolve(result);
//         }
//     });

//     return deferred.promise;
// };

// //private method
// var insertData = function(jsonData) {

//     var deferred = Q.defer();
//     var key = jsonData.body.key;
//     var insertQuery  = "INSERT INTO "+this.Client.dbTable+" VALUES ('"+key+"','"+JSON.stringify(jsonData.body)+"')";

//     this.Client.postgresClient.query(insertQuery, function (error, result) {

//         //error.code == 23505 UNIQUE VIOLATION
//         if (error && error.code == 23505) {

//             updateData(jsonData).then(function(response){
//                 deferred.resolve(response);
//             }).fail(function(error){
//                 deferred.reject(new Error(error));
//             });

//         } else {
//             deferred.resolve(result);
//         }
//     });

//     return deferred.promise;
// };

// // class methods
// Client.prototype.connect = async function() {

//     if ( this.postgresClient == null){
//         console.log("going to create client pg")
//         this.createPostgresClient();
//         console.log("created")
//     }

//     result = await this.postgresClient.connect();
// };

// //Creating-NodeJS-modules-with-both-promise-and-callback-API-support-using-Q
// Client.prototype.saveResource = function(table,callback) {

//     var deferred = Q.defer();

//     if ( !this.dbTable && !table){
//         deferred.reject("table must be passed.");
//     }else{

//         if (table) {
//             this.dbTable = table;
//         }

//         this.getApiContent().then(insertData).then(function(response){
//             this.Client.updateDateSync();
//             deferred.resolve(response);
//         }).fail(function(error){
//             deferred.reject(error);
//         });
//     }

//     deferred.promise.nodeify(callback);
//     return deferred.promise;
// };

Client.prototype.getURL = function(){

    var url = this.baseApiUrl+this.functionApiUrl;

    if ( this.encodeURL ){
        url =  encodeURI(url);
    }

    return url;
};


Client.prototype.getApiContent = function(next) {

    var self = this;

    // this.apiCredentials.open_timeout = this.apiTimeOut;

    return request.get({url: self.getURL(), auth: self.apiCredentials, headers: self.apiHeaders, json: true})
};


Client.prototype.saveResources = async function(filter){

    var count = 0
    var done = false

    const removeDollarFields = (o) => {
        for (var property in o) {
            if (o.hasOwnProperty(property)) {
                if (property.startsWith("$$") && property!='$$meta') {
                    delete o[property]
                } else  {
                    if (o.property !== null && typeof o.property === 'object') {
                        removeDollarFields(o)
                    }
                }
            }
        }
        return o
    }

    const handlePage = async function() {
        "use strict";

        var res = await this.getApiContent()

        if (res.statusCode != 200) {
            console.log(`\nFAILURE: ${r} => ${res.statusCode}\n${gutil.inspect(res.body)}\n\n`)
            //TODO:  error exit !
        } else if (res.body.results.length > 0) {
            
            var sql = `INSERT INTO ${this.dbTable} VALUES\n`
                sql += res.body.results.map( e => {
                                const key = e.$$expanded.key;
                                const stringifiedJson = JSON.stringify(removeDollarFields(e.$$expanded)).replaceAll("'", "''");
                                return `('${key}', '${stringifiedJson}','${this.resourceType}')`
                            }).join(',\n')
                sql += ';'

            try {
                const query_reply = await this.postgresClient.result(sql)
                if (query_reply.rowCount != res.body.results.length) {
                    console.log(`\n\nWARNING: INSERT count mismatch !`)    
                    console.log(`for query: ${sql}`)
                    console.log(`${query_reply.rowCount} <-> ${res.body.results.length}\n\n`)
                }
            } catch (err) {
                console.log(`\n\nSQL INSERT failed: ${err}`)
                console.log(`for query: ${sql}\n\n`)

                console.log(this.dbTable)

                process.exit(1)
            }        
        } 
        count += res.body.results.length

        var nextPage = res.body.$$meta.next;
        if (nextPage == undefined) {
            console.log(`NO NEXT PAGE => RETURNING (${this.dbTable})`)
            done = true
        } else {
            console.log(`NEXT PAGE: ${nextPage} (${count}) - (${this.dbTable})`)
            this.functionApiUrl = nextPage;
        }
    }

    while (!done) {
        await handlePage.call(this)    
    }

    return count
};

Client.prototype.deleteFromTable = async function(){
    var clientInstance = this;

    var sql = `DELETE FROM ${this.dbTable};`
    try {
        // console.log('this.postgresClient:')
        // console.log(this.postgresClient)
        const query_reply = await this.postgresClient.result(sql)
        //console.log(`DELETED ${query_reply.rowCount} rows.`)
        return query_reply.rowCount
    } catch (err) {
        console.log(`\n\nSQL DELETE failed: ${err}`)
        console.log(`for query: ${sql}\n\n`)
        process.exit(1)
    }
};


// Client.prototype.deleteFromTable = function(propertyConfig){

//     var deferred = Q.defer();

//     var clientInstance = this;

//     var deletionQuery = "DELETE FROM "+propertyConfig.targetTable;
//     console.log("SRI2POSTGRES: deleteFromTable :: Started");
//     this.postgresClient.query(deletionQuery, function (err) {
//         console.log("SRI2POSTGRES: deleteFromTable :: end");
//         if (err) {
//             console.log("SRI2POSTGRES: deleteFromTable :: ERROR " + err);
//             deferred.reject(new Error(err));
//         }else{
//             console.log("SRI2POSTGRES: deleteFromTable :: SUCCESS");
//             clientInstance.propertyConfig = propertyConfig;
//             deferred.resolve(clientInstance);
//         }
//     });

//     return deferred.promise;
// };

var saveError = function (key,link,code,message,database){
    var deferred = Q.defer();

    var errorInsertQuery  = "INSERT INTO content_as_text_errors VALUES ('"+key+"','"+link+"','"+code+"','"+message+"')";
    database.query(errorInsertQuery,function(queryError){
        if (queryError){
            console.error(message + " " +code);
            console.error(key);
            console.error(link);
            console.error("--*--");
        }
        deferred.resolve();
    });

    return deferred.promise;
};

Client.prototype.readFromTable = function(sri2PostgresClient){

    var deferred = Q.defer();

    var database = new pg.Client({
        user: sri2PostgresClient.dbUser,
        password: sri2PostgresClient.dbPassword,
        database: sri2PostgresClient.database,
        port: sri2PostgresClient.dbPort,
        host: sri2PostgresClient.dbHost,
        ssl: sri2PostgresClient.dbSsl
    });

    //console.log("SRI2POSTGRES: readFromTable :: Connecting to database");

    // database.connect(function(error){

        //console.log("SRI2POSTGRES: readFromTable :: Successfully Connected to database");

        if (error){
            console.log("SRI2POSTGRES: ERROR in readFromTable: " + error);
            return deferred.reject(error);
        }

        var offset = sri2PostgresClient.propertyConfig.hasOwnProperty('offset') ? sri2PostgresClient.propertyConfig.offset : 0;
        var limit = sri2PostgresClient.propertyConfig.hasOwnProperty('limit') ? sri2PostgresClient.propertyConfig.limit : 1000000;

        // SELECT key, obj->>'href' as link FROM jsonb, jsonb_array_elements(value->'attachments') obj WHERE type = 'curriculumzill' AND obj->>'type' = 'CONTENT_AS_TEXT' ORDER BY key LIMIT 5000 OFFSET 0

        var sqlQuery = "SELECT key, "+sri2PostgresClient.propertyConfig.propertyName+" AS link";
        sqlQuery += " FROM "+sri2PostgresClient.dbTable+" "+sri2PostgresClient.propertyConfig.fromExtraConditions;
        sqlQuery += " WHERE type = '"+sri2PostgresClient.resourceType+"' "+sri2PostgresClient.propertyConfig.whereExtraConditions;
        sqlQuery += " ORDER BY key LIMIT $1 OFFSET "+offset;
        var query = new QueryStream(sqlQuery, [limit]);
        var stream = sri2PostgresClient.postgresClient.query(query);
        var count = 0;
        var resourcesSync = 0;
        var queue = 0;

        function handleStreamFlow(){
            if (stream.readable){
                queue--;
                stream.resume();
            }else{
                deferred.resolve({resourcesSync: resourcesSync, resourcesNotSync: count-resourcesSync});
            }
        }

        stream.on('data',function(chunk){

            stream.pause();
            count++;
            queue++;

            var originalLink = chunk.link;
            var res = originalLink.split("/");
            var sourceName = res[res.length-1];
            sourceName = encodeURIComponent(sourceName);
            var componentUrl = "/" + res[1] + "/" +res[2] + "/" + sourceName;

            sri2PostgresClient.functionApiUrl = componentUrl;

            sri2PostgresClient.getApiContent().then(function(response){

                //console.log("SRI2POSTGRES: readFromTable :: Obtained content_as_text for: " + chunk.link);

                if (response.statusCode == 200 ){

                    var isBuffer = (response.body instanceof Buffer);

                    if (response.body.length > 0 && !isBuffer){

                        //console.log("SRI2POSTGRES: readFromTable ["+count+"] :: preparing INSERT for " +chunk.key);

                        var data = response.body.replaceAll("'", "''");
                        // After replacing ' -> '' there are still cases where \'' brake the query, so
                        // we need to transform \'' -> '' to correctly insert it.
                        data = data.replaceAll("\\''", "''");

                        var insertQuery  = "INSERT INTO "+sri2PostgresClient.propertyConfig.targetTable+" VALUES ('"+chunk.key+"',E'"+data+"')";

                        database.query(insertQuery,function(queryError){

                            if (queryError){
                                saveError(chunk.key,chunk.link,0,queryError.message,database);
                            }else{
                                resourcesSync++;
                                console.log("SRI2POSTGRES: readFromTable :: [ "+resourcesSync+"/"+count+" ]  INSERT SUCCESSFULLY for " +chunk.key);
                            }

                            handleStreamFlow();

                        });
                    }else{

                        var message = isBuffer ? 'response.body instanceof Buffer' : 'response.body is empty';
                        saveError(chunk.key,chunk.link,response.statusCode,message,database)
                            .then(handleStreamFlow);
                    }
                }else{
                    //statusCode != 200 => Error
                    saveError(chunk.key,chunk.link,response.statusCode,response.statusMessage,database)
                        .then(handleStreamFlow);
                }

            }).fail(function(getApiContentError){
                saveError(chunk.key,chunk.link,getApiContentError.code,getApiContentError.message,database)
                    .then(handleStreamFlow);
            });
        });

        stream.on('end',function(){
            if (queue == 0){
                deferred.resolve({resourcesSync: resourcesSync, resourcesNotSync: count-resourcesSync});
            }
        });
    // });

    return deferred.promise;
};

Client.prototype.saveResourcesInProperty = function(propertyConfig,callback){

    var deferred = Q.defer();

    console.log("SRI2POSTGRES: saveResourcesInProperty :: Started");
    // Delete all content from new database
    this.deleteFromTable(propertyConfig)
        .then(this.readFromTable)
        .then(function(response){
            deferred.resolve({resourcesSync: response.resourcesSync, resourcesNotSync: response.resourcesNotSync});
        }).fail(function(error){
            deferred.reject(error);
        });

    deferred.promise.nodeify(callback);
    return deferred.promise;
};

Client.prototype.saveResourcesInPropertyWithoutTableDeletion = function(propertyConfig,callback){

    var deferred = Q.defer();

    console.log("SRI2POSTGRES: saveResourcesInPropertyWithoutTableDeletion :: Started");

    this.propertyConfig = propertyConfig;

        this.readFromTable(this)
            .then(function(response){
                deferred.resolve({resourcesSync: response.resourcesSync, resourcesNotSync: response.resourcesNotSync});
            }).fail(function(error){
                deferred.reject(error);
            });

    deferred.promise.nodeify(callback);
    return deferred.promise;
};

// Client.prototype.end = function() {
//     pgp.end(); // terminate the database connection pool
// }

// export the class
module.exports = Client;