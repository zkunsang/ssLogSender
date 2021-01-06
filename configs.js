const nodeEnv = process.env.NODE_ENV;

const dbMongoLog = require(`./configs/${nodeEnv}/dbMongoLog.json`);
const fluent = require(`./configs/${nodeEnv}/fluent.json`);

module.exports = {
    dbMongoLog,
    fluent
}