const MongoConnectionHelper = require('./MongoConnectionHelper');
const configs = require('./configs');
const events = require('events');
const DateUtil = require('./utils/DateUtil');
const fluent = require('./helper/FluetdHelper');
const eventEmitter = new events.EventEmitter();

let dbSourceCursor = {};
let sourceDb = null;

const COLLECTION_LIST = {
    INVEN: 'inven',
    LOGIN: 'login',
    NETWORK: 'network',
    STORY_LOG: 'story_log',
    PRODUCT: 'product'
}

const SOURCE_DB = 'log';

async function start() {
    await fluent.ready();
    const connection = await MongoConnectionHelper.setConnection(configs.dbMongoLog);
    sourceDb = connection.db(SOURCE_DB);

    eventEmitter.emit('processing');
}

eventEmitter.on('processing', async () => {
    const today = moment();
    const YYYYMMDD = DateUtil.utsToDs(today.subtract(configs.App.subDay, 'days').unix(), DateUtil.YYYYMMDD);
    
    const sourceList = Object.values(COLLECTION_LIST);

    for (const source of sourceList) {
        dbSourceCursor[source] = { YYYYMMDD };
    }

    await parseLog(COLLECTION_LIST.INVEN, fluent.sendInvenLog);
    await parseLog(COLLECTION_LIST.LOGIN, fluent.sendLoginLog);
    await parseLog(COLLECTION_LIST.NETWORK, fluent.sendInvenLog);
    await parseLog(COLLECTION_LIST.STORY_LOG, fluent.sendStoryLog);
    await parseLog(COLLECTION_LIST.PRODUCT, fluent.sendProductLog);

    eventEmitter.emit('processing');
});

function parseLog(source, sendLog) {
    const YYYYMMDD = dbSourceCursor[source].YYYYMMDD;
    const collectionName = `${source}_${YYYYMMDD}`;
    const connection = sourceDb.collection(collectionName);
    const dataList = await connection.find({}, { limit: configs.App.limit }).toArray();

    // 데이터가 없으면
    if(dataList.length == 0) {
        const todayYYYYMMDD = moment().format(DateUtil.YYYYMMDD);
        // 당일날이 되면 커서를 옮길 필요가 없다.
        if(todayYYYYMMDD != YYYYMMDD) {
            const parseDate = DateUtil.dsToUts(YYYYMMDD, DateUtil.YYYYMMDD);
            const nextYYYYMMDD = DateUtil.utsToDs(parseDate.add(1, "days"), DateUtil.YYYYMMDD);
            dbSourceCursor[source].YYYYMMDD = nextYYYYMMDD;
        }

        await sourceDb.dropCollection(collectionName);
        return;
    }

    try {
        for(const data of dataList) {
            await sendLog(data);
            // await connection.deleteOne({_id: data._id});
        }
    }
    catch(err) {
        console.error(err);
        // slack error Message;
    }
}



start();