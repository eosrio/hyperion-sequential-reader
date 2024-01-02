import {parentPort, workerData} from "worker_threads";
import {ABI, Serializer} from "@greymass/eosio";
import * as console from "console";
import {logLevelToInt} from "./utils.js";

const contracts: Map<string, ABI> = new Map();

function workerLog(level: string, message?: any, ...optionalParams: any[]): void {
    if (logLevelToInt(workerData.logLevel) >= logLevelToInt(level)) {
        const timestampHeader = `[${(new Date()).toISOString().slice(0, -1)}]`;
        const workerHeader = `[WORKER ${workerData.wIndex}]`;
        const levelHeader = `[${level.toUpperCase()}]`;
        console.log([timestampHeader, workerHeader, levelHeader].join(''), message, ...optionalParams);
    }
}

export interface DSMessage {
    index: number;  // action or delta index relative to block
    blockId: string;
    blockNum: number;
    data: any;
}

export interface DeltaDSMessage extends DSMessage {
    data: {
        code: string;
        table: string;
        value: string;
    }
}

export interface ActionDSMessage extends DSMessage {
    data: {
        account: string;
        name: string;
        data: string;
    }
}

export interface DeltaResponseDSMessage extends DSMessage {
    data: {
        code: string;
        table: string;
        value: any;
    }
}

export interface ActionResponseDSMessage extends DSMessage {
    data: {
        account: string;
        name: string;
        data: any;
    }
}

export interface ParentMessage {
    abi?: {
        account: string;
        obj: ABI;
    };
    delta?: DeltaDSMessage;
    action?: ActionDSMessage;
}

export interface WorkerMessage {
    index: number;  // worker index
    abi?: { account: string };
    delta?: DeltaResponseDSMessage;
    action?: ActionResponseDSMessage;
}

function processDelta(message: DeltaDSMessage): DeltaResponseDSMessage {
    const delta = message.data;
    if (contracts.has(delta.code)) {
        const abi = contracts.get(delta.code);
        if (abi) {
            const type = abi.tables.find(value => value.name === delta.table)?.type;
            if (type) {
                try {
                    const dsValue = Serializer.decode({data: delta.value, type, abi});
                    return {
                        ...message,
                        data: {
                            ...message.data,
                            value: Serializer.objectify(dsValue)
                        }
                    };

                } catch (e) {
                    workerLog('error', e.message, delta.code, delta.table);
                }
            } else {
                workerLog('error', `Missing ABI type for table ${delta.table} of ${delta.code}`);
            }
        }
    }
    // addToFailedDS(delta.code, message);
    throw new Error(`Failed to process delta ${delta.code}: ${JSON.stringify(message)}`);
}

function processAction(message: ActionDSMessage): ActionResponseDSMessage {
    const action = message.data;
    const abi = contracts.get(action.account);
    if (abi) {
        try {
            const decodedActData = Serializer.decode({
                data: action.data,
                type: action.name,
                ignoreInvalidUTF8: true,
                abi
            });
            if (decodedActData) {
                return {
                    ...message,
                    data: {
                        ...message.data,
                        data: decodedActData
                    }
                };
            }
        } catch (e) {
            workerLog('error', e.message, message);
        }
    }
    throw new Error(`Failed to process action ${JSON.stringify(action)}`);
}

function processMessage(msg: ParentMessage) {
    let response: WorkerMessage = {
        index: workerData.wIndex
    };
    if (msg.abi) {
        const abi = ABI.from(msg.abi.obj);
        const account = msg.abi.account;
        contracts.set(account, abi);

        response.abi = {account: account};
        workerLog('debug', `abi set for ${msg.abi.account}`);
    }
    if (msg.delta) {
        response.delta = processDelta(msg.delta);
        workerLog(
            'debug',
            `ds delta ${msg.delta.data.code}::${msg.delta.data.table} index: ${msg.delta.index} block_num: ${msg.delta.blockNum}`
        );
    }

    if (msg.action) {
        response.action = processAction(msg.action);
        workerLog(
            'debug',
            `ds action ${msg.action.data.account}::${msg.action.data.name} index: ${msg.action.index} block_num: ${msg.action.blockNum}`
        );
    }

    parentPort.postMessage(response);
}

parentPort.on("message", value => {
    processMessage(value);
});
