const nodeEnv = process.env.NODE_ENV;

const dbMongoLog = require(`./configs/${nodeEnv}/dbMongoLog.json`);
const fluent = require(`./configs/${nodeEnv}/fluent.json`);
const App = require(`./configs/${nodeEnv}/app.json`);

module.exports = {
    dbMongoLog,
    fluent,
    App,
}