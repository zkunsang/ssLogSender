const net = require('net');
const JsonSocket = require('json-socket');

const MongoConnectionHelper = require('./MongoConnectionHelper');
const configs = require('./configs');
const events = require('events');
const DateUtil = require('./utils/DateUtil');
const moment = require('moment');
const { ObjectID, MongoError } = require('mongodb');


let clientSocket = null;
const eventEmitter = new events.EventEmitter();

let dbSourceCursor = {};
let sourceDb = null;
let dbDataList = {};

const COLLECTION_LIST = {
    INVEN: 'inven',
    LOGIN: 'login',
    NETWORK: 'network',
    STORY_LOG: 'story_log',
    PICTURE: 'picture',
    PRODUCT: 'product'
}

const SOURCE_DB = configs.dbMongoLog.sourceDb;

async function start() {
    const connection = await MongoConnectionHelper.setConnection(configs.dbMongoLog);
    sourceDb = connection.db(SOURCE_DB);

    const today = moment();
    const YYYYMMDD = DateUtil.utsToDs(today.subtract(configs.App.subDay, 'days').unix(), DateUtil.YYYYMMDD);
    
    const sourceList = Object.values(COLLECTION_LIST);

    for (const source of sourceList) {
        dbSourceCursor[source] = { YYYYMMDD };
        eventEmitter.on(source, parseLog(source, sendData));
    }

    const server = net.createServer();

    server.on('connection', (socket) => {
        if(clientSocket) 
            throw Error('clientSocket already');

        console.log(`[${moment().format(DateUtil.DEFAULT_FORMAT)}]${socket.address().address} connected`);
        clientSocket = new JsonSocket(socket);
        for(const source of sourceList) {
            setTimeout(() => eventEmitter.emit(source), 1000);
        }

        clientSocket.on('message', async(data) => {
		console.log(data)
            const source = data.source;
        
            const { dataList, connection } = dbDataList[source];
	
            for(const data of dataList) {
                await connection.deleteOne({_id: ObjectID(data._id)});
            }
        
            dbDataList[source] = null;
            setTimeout(() => eventEmitter.emit(source), 1000);
        });

        clientSocket.on('error', (err) => {
            console.log('clientSocket - error occured');
            clientSocket = null;
        });
    });

    server.on('error', (err) => {
        console.log('server - error occured');
    });
    
    server.listen(configs.App.port, () => {
        console.log(`${configs.App.port} - listen`)
    });
}

function parseLog(source, sendData) {
    return async () => {
        try {
            const YYYYMMDD = dbSourceCursor[source].YYYYMMDD;
            const collectionName = `${source}_${YYYYMMDD}`;
            const connection = sourceDb.collection(collectionName);
            const dataList = await connection.find({}, { limit: configs.App.limit }).toArray();

            // 데이터가 없으면
            if(dataList.length == 0) {
                const todayYYYYMMDD = moment().format(DateUtil.YYYYMMDD);
                // 당일날이면 커서를 옮길 필요가 없다.
                if(todayYYYYMMDD != YYYYMMDD) {
                    const parseDate = DateUtil.dsToDate(YYYYMMDD, DateUtil.YYYYMMDD);
                    const nextYYYYMMDD = DateUtil.utsToDs(parseDate.add(1, "days").unix(), DateUtil.YYYYMMDD);
                    
                    dbSourceCursor[source].YYYYMMDD = nextYYYYMMDD;
                    await sourceDb.dropCollection(collectionName);
                    console.log(`${collectionName} -- dropped` )
                }
                
                setTimeout(() => eventEmitter.emit(source), 1000);
                return;
            }

            if(!dbDataList[source]) {
                dbDataList[source] = {};
            }

            dbDataList[source] = { dataList, connection, source };
            sendData(dbDataList[source], collectionName);
        }
        catch(err) {
            if (err instanceof MongoError) {
                setTimeout(() => eventEmitter.emit(source), 1000);
            }
        }
    }
}

async function sendData(data, collectionName) {
    const { source, dataList } = data;
    
    clientSocket.sendMessage({ source, dataList, collectionName });
}

start();
