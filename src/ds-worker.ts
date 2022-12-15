import {parentPort, workerData} from "worker_threads";
import {ABI, Serializer} from "@greymass/eosio";
import * as console from "console";

const contracts: Map<string, ABI> = new Map();
let shipAbi: ABI;

const failedDSMap: Map<string, any[]> = new Map();

function workerLog(message?: any, ...optionalParams: any[]): void {
    console.log(`[WORKER ${workerData.wIndex}]`, message, ...optionalParams);
}

function addToFailedDS(contract: string, message: any) {
    if (!message.errorCounter) {
        message.errorCounter = 1;
    } else {
        message.errorCounter++;
    }
    if (failedDSMap.has(contract)) {
        failedDSMap.get(contract).push(message);
    } else {
        failedDSMap.set(contract, [message]);
        if (message.errorCounter === 1) {
            parentPort.postMessage({event: 'request_head_abi', contract});
        }
    }
    // workerLog(`Failed deserializations for ${contract}: ${failedDSMap.get(contract)?.length}`)
}

function checkFailed(contract: string) {
    const messages = failedDSMap.get(contract);
    if (messages && messages.length > 0) {
        // workerLog('Re-trying failed messages');
        while (messages.length > 0) {
            processMessage(messages.shift());
        }
    }
}

function processDelta(message: any) {
    const delta = message.content.extDelta;
    if (contracts.has(delta.code)) {
        const abi = contracts.get(delta.code);
        if (abi) {
            const type = abi.tables.find(value => value.name === delta.table)?.type;
            if (type) {
                try {
                    const dsValue = Serializer.decode({data: delta.value, type, abi});
                    parentPort.postMessage({
                        event: 'decoded_delta',
                        wIndex: workerData.wIndex,
                        data: {
                            index: message.content.index,
                            blockNum: message.content.blockNum,
                            value: Serializer.objectify(dsValue)
                        }
                    });
                    return;
                } catch (e) {
                    workerLog(e.message, delta.code, delta.table);
                }
            } else {
                // workerLog(`Missing ABI type for table ${delta.table} of ${delta.code}`);
            }
        }
    }
    addToFailedDS(delta.code, message);
}

function processAction(message: any) {
    const act = message.data.act;
    const abi = contracts.get(act.account);
    if (abi) {
        try {
            const decodedActData = Serializer.decode({data: act.data, type: act.name, abi});
            if (decodedActData) {
                act.data = decodedActData;
                parentPort.postMessage({
                    event: 'decoded_action',
                    wIndex: workerData.wIndex,
                    data: Serializer.objectify(message.data)
                });
                return;
            }
        } catch (e) {
            workerLog(e.message, message.data);
        }
    }
    addToFailedDS(act.account, message);
}

function processMessage(msg: any) {
    switch (msg.event) {
        case 'set_abi': {
            contracts.set(msg.data.account, ABI.from(msg.data.abi));
            checkFailed(msg.data.account);
            break;
        }
        case 'set_ship_abi': {
            shipAbi = msg.data.abi;
            break;
        }
        case 'delta': {
            processDelta(msg);
            break;
        }
        case 'action': {
            processAction(msg);
            break;
        }
    }
}

parentPort.on("message", value => {
    processMessage(value);
});
