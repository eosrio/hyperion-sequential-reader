import * as console from "console";
import * as process from "process";


import {SharedObjectStore, MemoryBounds} from "shm-store";
import {ABI, Serializer} from "@greymass/eosio";
import workerpool from "workerpool";

import {logLevelToInt} from "./utils.js";
import {Action, DecodedAction, TableDelta} from "./types/antelope.js";

const logLevel = process.env.WORKER_LOG_LEVEL;

function workerLog(level: string, message?: any, ...optionalParams: any[]): void {
    if (logLevelToInt(logLevel) >= logLevelToInt(level)) {
        const timestampHeader = `[${(new Date()).toISOString().slice(0, -1)}]`;
        const workerHeader = `[WORKERPOOL]`;
        const levelHeader = `[${level.toUpperCase()}]`;
        console.log([timestampHeader, workerHeader, levelHeader].join(''), message, ...optionalParams);
    }
}

export interface DSMessage {
    shmRef: SharedArrayBuffer;
    memMap: {[key: string]: MemoryBounds}
    blockId: string;
    blockNum: number;
    data: any;
}

export interface ShipDSMessage extends DSMessage {
    type: string;
    data: string | Uint8Array;
}

export interface DeltaDSMessage extends DSMessage {
    data: TableDelta;
}

export interface ActionDSMessage extends DSMessage {
    data: Action;
}

export interface DeltaDSResponse extends DSMessage {
    data: {
        code: string;
        table: string;
        value: any;
    }
}

export interface ActionDSResponse extends DSMessage {
    data: DecodedAction;
}

function decodeShip(message: ShipDSMessage): DSMessage {
    const contractStore = SharedObjectStore.fromMemoryMap<ABI.Def>(message.shmRef, message.memMap);
    const data = message.data;
    const contract = contractStore.get('shipAbi');
    const abi = ABI.from(contract as ABI.Def);
    return {
        ...message,
        data: Serializer.decode({
            type: message.type,
            data: message.data,
            abi
        })
    };
}

function processDelta(message: DeltaDSMessage): DeltaDSResponse {
    const contractStore = SharedObjectStore.fromMemoryMap<ABI.Def>(message.shmRef, message.memMap);
    const delta = message.data;
    const contract = contractStore.get(delta.code);
    const abi = ABI.from(contract as ABI.Def);

    const type = contract.tables.find(value => value.name === delta.table)?.type;

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
    // addToFailedDS(delta.code, message);
    throw new Error(`Failed to process delta ${delta.code}: ${JSON.stringify(message)}`);
}
function processAction(message: ActionDSMessage): ActionDSResponse {
    const contractStore = SharedObjectStore.fromMemoryMap<ABI.Def>(message.shmRef, message.memMap);
    const action = message.data;
    const contract = contractStore.get(action.account);
    const abi = ABI.from(contract as ABI.Def);
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
                    data: Serializer.objectify(decodedActData)
                }
            };
        }
    } catch (e) {
        workerLog('error', e.message, message);
    }
    throw new Error(`Failed to process action ${JSON.stringify(action)}`);
}

workerpool.worker({
    decodeShip, processDelta, processAction
});