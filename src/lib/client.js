/**
 * Created by pablo on 23/07/15.
 */

var needle = require('needle');
var pg = require('pg');
var Q = require('q');
var Transaction = require('pg-transaction');
var QueryStream = require('pg-query-stream');
var retry = require('retry');

var pgp = require('pg-promise')();

const Promise = require("bluebird");

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

    this.dbUser = config.db.dbUser;
    this.dbPassword = config.db.dbPassword;
    this.database = config.db.database;
    this.dbPort = config.db.dbPort;
    this.dbHost = config.db.dbHost;
    this.dbSsl = config.db.hasOwnProperty('dbSsl') ? config.db.dbSsl : false;
    this.dbTable = config.db.dbTable;

    this.resourceType = config.hasOwnProperty('resourceType') ? config.resourceType : 'document';
    this.requiredByRoot = config.hasOwnProperty('requiredByRoot') ? config.requiredByRoot : undefined;

    this.encodeURL = config.hasOwnProperty('encodeURL') ? config.encodeURL : true;

    this.apiTimeOut = config.hasOwnProperty('apiTimeOut') ? config.apiTimeOut : 0;

    this.lastSync = null;
    this.postgresClient = null;

    pgp.pg.defaults.ssl = (process.env.DATABASE_URL!=undefined); //use SSL for heroku

    this.createPostgresClient = function(){

        this.postgresClient = pgp(process.env.DATABASE_URL || {
            user: this.dbUser,
            password: this.dbPassword,
            database: this.database,
            port: this.dbPort,
            host: this.dbHost,
            ssl: this.dbSsl
        });

    };

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

var insertResources = async function(composeObject) {

    console.log('insertResources')

    var jsonData = composeObject.jsonData;
    var count = jsonData.body.results.length;
    var inserted = 0;

    var insertQuery;

    console.log('starting db transaction')
    client = this.Client
    result = await this.Client.postgresClient.tx(async function (t) {
        // `t` and `this` here are the same;
        // creating a sequence of transaction queries:

        ql = []
        jsonData.body.results.forEach( (j) => {
            var key = j.$$expanded.key;
            var stringifiedJson = JSON.stringify(j.$$expanded);
            stringifiedJson = stringifiedJson.replaceAll("'", "''");
            insertQuery  = "INSERT INTO "+client.dbTable+" VALUES ('"+key+"','"+stringifiedJson+"','"+client.resourceType+"')";
            ql.push(t.none(insertQuery))
        })
        return t.batch(ql);
    })
    console.log('transaction done.')
    return result.length
};

var updateData = function(jsonData){
    var deferred = Q.defer();
    var key = jsonData.body.key;
    var updateQuery  = "UPDATE "+this.Client.dbTable+" SET details = '"+JSON.stringify(jsonData.body)+"' WHERE key = '"+key+"'";

    this.Client.postgresClient.query(updateQuery, function (error, result) {
        if (error) {
            deferred.reject(new Error(error));
        } else {
            deferred.resolve(result);
        }
    });

    return deferred.promise;
};

//private method
var insertData = function(jsonData) {

    var deferred = Q.defer();
    var key = jsonData.body.key;
    var insertQuery  = "INSERT INTO "+this.Client.dbTable+" VALUES ('"+key+"','"+JSON.stringify(jsonData.body)+"')";

    this.Client.postgresClient.query(insertQuery, function (error, result) {

        //error.code == 23505 UNIQUE VIOLATION
        if (error && error.code == 23505) {

            updateData(jsonData).then(function(response){
                deferred.resolve(response);
            }).fail(function(error){
                deferred.reject(new Error(error));
            });

        } else {
            deferred.resolve(result);
        }
    });

    return deferred.promise;
};

// class methods
Client.prototype.connect = async function() {

    if ( this.postgresClient == null){
        console.log("going to create client pg")
        this.createPostgresClient();
        console.log("created")
    }

    result = await this.postgresClient.connect();
};

//Creating-NodeJS-modules-with-both-promise-and-callback-API-support-using-Q
Client.prototype.saveResource = function(table,callback) {

    var deferred = Q.defer();

    if ( !this.dbTable && !table){
        deferred.reject("table must be passed.");
    }else{

        if (table) {
            this.dbTable = table;
        }

        this.getApiContent().then(insertData).then(function(response){
            this.Client.updateDateSync();
            deferred.resolve(response);
        }).fail(function(error){
            deferred.reject(error);
        });
    }

    deferred.promise.nodeify(callback);
    return deferred.promise;
};

Client.prototype.getURL = function(){

    var url = this.baseApiUrl+this.functionApiUrl;

    if ( this.encodeURL ){
        url =  encodeURI(url);
    }

    return url;
};


Client.prototype.getApiContent = function(next) {

    var deferred = Q.defer();
    var operation = retry.operation({retries: this.apiRetries});
    var self = this;

    this.apiCredentials.open_timeout = this.apiTimeOut;

    var needleOptions = {
      headers: self.apiHeaders, 
    }
    Object.assign(needleOptions, self.apiCredentials)


    operation.attempt(function(attempt){

        if(attempt > 1){
            console.log("getApiContent retry attempt: "+attempt+ " for: "+self.baseApiUrl+self.functionApiUrl);
            console.log("with options: " + needleOptions)
        }

        needle.get(self.getURL(), needleOptions, function (error,response) {

            if (operation.retry(error)) {
                return;
            }

            if (error) {
                return deferred.reject(operation.mainError());
            }

            //Doing this bind to keep Client instance reference.
            this.Client = self;
            deferred.resolve(response);
        });
    });

    deferred.promise.nodeify(next);
    return deferred.promise;
};


Client.prototype.saveResources = async function(filter){

    var deferred = Q.defer();
    totalSync = 0;
    totalNotSync = 0;
    var clientCopy = this;

    async function recurse(filter,client) {

        jsonData = await client.getApiContent()

        var composeObject = {filter: filter,jsonData: jsonData};
        inserted = await insertResources(composeObject);
        totalSync += inserted

        nextPage = jsonData.body.$$meta.next;

        if (nextPage == undefined) {
            console.log("NO NEXT PAGE => RETURNING")
            return {resourcesSync: totalSync,resourcesNotSync: totalNotSync };
        } else {
            console.log("NEXT PAGE: " + nextPage)
            client.functionApiUrl = nextPage;
            return await recurse(filter,client);            
        }
    }

    return await recurse(filter,clientCopy);
};


Client.prototype.deleteFromTable = function(propertyConfig){

    var deferred = Q.defer();

    var clientInstance = this;

    var deletionQuery = "DELETE FROM "+propertyConfig.targetTable;
    console.log("SRI2POSTGRES: deleteFromTable :: Started");
    this.postgresClient.query(deletionQuery, function (err) {
        console.log("SRI2POSTGRES: deleteFromTable :: end");
        if (err) {
            console.log("SRI2POSTGRES: deleteFromTable :: ERROR " + err);
            deferred.reject(new Error(err));
        }else{
            console.log("SRI2POSTGRES: deleteFromTable :: SUCCESS");
            clientInstance.propertyConfig = propertyConfig;
            deferred.resolve(clientInstance);
        }
    });

    return deferred.promise;
};

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

    database.connect(function(error){

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
    });

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

// export the class
module.exports = Client;