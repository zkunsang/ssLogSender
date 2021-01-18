const logger = require('fluent-logger');
const configs = require('../configs.js');

class FluentHelper {
    constructor() {
        this.logger = null;
    }

    async ready() {
        this.fluentConfig = configs.fluent;

        if (!this.fluentConfig.useFluent) return;

        this.logger = logger.createFluentSender(
            this.fluentConfig.tag,
            {
                host: this.fluentConfig.host,
                port: this.fluentConfig.port,
                timeout: this.fluentConfig.timeout, // 1.0,
                reconnectInterval: this.fluentConfig.reconnectInterval // 600000 // 10 minutes
            });
    }

    sendLog(category, log) {
        if(!this.fluentConfig.useFluent) {
            console.log(`${category} - ${JSON.stringify(log)}`);
            return;
        }

        if (Array.isArray(log)) {
            for(const l of log) {
                this.logger.emit(category, l);
            }
        }
        else if (typeof log === 'object') {
            this.logger.emit(category, log);
            
        }
        else {
            console.error('not supported log')
        }
    }

    sendProductLog(productLog) {
        this.sendLog('product', productLog);
    }

    sendInvenLog(invenLog) {
        this.sendLog('inventory', invenLog);
    }

    sendLoginLog(invenLog) {
        this.sendLog('login', invenLog);
    }

    sendNetworkLog(networkLog) {
        this.sendLog('network', networkLog);
    }

    sendStoryLog(story) {
        this.sendLog('story', story);
    }
}

module.exports = new FluentHelper();