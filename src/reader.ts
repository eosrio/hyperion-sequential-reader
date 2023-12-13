import {RawData} from "ws";
import { StateHistorySocket } from "./state-history.js";
import {ABI, ABIDecoder, APIClient, Serializer} from "@greymass/eosio";
import {EventEmitter} from "events";
import {Worker} from "worker_threads";
import * as path from "path";
import {fileURLToPath} from "node:url";
import {cargo, queue, QueueObject} from "async";
import fetch from "node-fetch";
import * as process from "process";

import {OrderedSet} from "./orderedset.js";
import * as console from "console";


function logLevelToInt(level: string) {
    const levels = [
      'error', 'warning', 'info', 'debug'
    ];
    if (!levels.includes(level))
        throw new Error(`Unimplemented level ${level}`);
    return levels.indexOf(level);
}

const HIST_TIME = 15 * 60 * 2;  // 15 minutes in blocks

export interface HyperionSequentialReaderOptions {
    shipApi: string;
    chainApi: string;
    poolSize: number;
    irreversibleOnly?: boolean;
    blockConcurrency?: number;
    blockHistorySize?: number;
    startBlock?: number;
    endBlock?: number;
    outputQueueLimit?: number;
    logLevel?: string;
}

export class HyperionSequentialReader {
    ship: StateHistorySocket;
    max_payload_mb = 256;
    reconnectCount = 0;
    private connecting = false;
    private shipAbi?: ABI;
    private shipAbiReady = false;
    private shipInitStatus?: any;
    events = new EventEmitter();

    dsPool: Worker[] = [];
    private allowedContracts: Map<string, ABI> = new Map();
    startBlock: number;
    endBlock: number;

    // queues
    maxMessagesInFlight = 50;
    inputQueueLimit = 200;
    outputQueueLimit = 1000;
    blockConcurrency: number;
    inputQueue: QueueObject<any>;
    decodingQueue: QueueObject<any>;

    private decodedBlockCounter = 0;
    private pendingAck = 0;
    private paused = false;

    // block collector map
    blockCollector: Map<number, {
        ready: boolean,
        blockInfo: any,
        blockHeader: any,
        counters: {
            actions: number,
            deltas: number
        },
        createdAt?: bigint,
        proc_time_us?: number,
        targets: {
            actions: number,
            deltas: number
        }
        actions: any[],
        deltas: any[]
    }> = new Map();

    blockHistory: OrderedSet<number>;
    blockHistorySize: number;
    logLevel: string;

    api: APIClient;
    shipApi: string;
    abiRequests: Record<string, boolean> = {};
    private actionRefMap: Map<number, any> = new Map();
    private deltaRefMap: Map<string, any> = new Map();
    private lastEmittedBlock = 0;
    private nextBlockRequested = 0;
    private irreversibleOnly;

    onConnected: () => void = null;
    onDisconnect: () => void = null;
    onError: (err) => void = null;

    private _reporterTask;
    private _resumerTask;

    constructor(private options: HyperionSequentialReaderOptions) {

        this.logLevel = options.logLevel || 'warning';
        this.shipApi = options.shipApi;
        this.ship = new StateHistorySocket(this.shipApi, this.max_payload_mb);

        this.blockHistorySize = options.blockHistorySize || HIST_TIME;
        this.blockHistory = new OrderedSet<number>(this.blockHistorySize);

        this.api = new APIClient({
            url: options.chainApi,
            fetch
        });

        this.createWorkers({
            poolSize: options.poolSize || 1
        });

        this.irreversibleOnly = options.irreversibleOnly || false

        this.startBlock = options.startBlock || -1;
        this.endBlock = options.endBlock || 0xffffffff;

        if (!options.endBlock && !options.startBlock) {
            this.blockConcurrency = 1;
        } else {
            this.blockConcurrency = options.blockConcurrency || 5;
        }

        if (options.outputQueueLimit) {
            this.outputQueueLimit = options.outputQueueLimit;
        }

        // Initial Reading Queue
        this.inputQueue = cargo(async (tasks) => {
            try {
                await this.processInputQueue(tasks);
            } catch (e) {
                this.log('error', e);
                process.exit();
            }
        }, this.maxMessagesInFlight);

        // Parallel Decoding Queue
        this.decodingQueue = queue(async (task) => {
            await this.decodeShipData(task);
            this.decodedBlockCounter++;
            // readerLog(`[${blockNum}] Decoding Queue: ${this.decodingQueue.length()} | Paused: ${this.paused}`);
            if ((this.decodingQueue.length() < this.inputQueueLimit && this.blockCollector.size < this.outputQueueLimit) && this.paused) {
                this.resumeReading();
            }
        }, this.blockConcurrency);

        // Report average processing speed each 10s
        this._reporterTask = setInterval(() => {
            if (this.decodedBlockCounter > 0) {
                let readyblocks = 0;
                let readyPct = 0;
                this.blockCollector.forEach(value => {
                    if (value.ready) {
                        readyblocks++;
                    }
                });
                if (this.blockCollector.size > 0) {
                    readyPct = (readyblocks * 100 / this.blockCollector.size);
                }
                // this.log('info', `${this.decodedBlockCounter / 2} block/s | Blocks: ${this.blockCollector.size} (${readyPct.toFixed(1)}%) | Actions: ${this.actionRefMap.size} | Deltas: ${this.deltaRefMap.size}`);
                this.decodedBlockCounter = 0;
            }
        }, 2000);

        // Check if output queue is whitin limits
        this._resumerTask = setInterval(() => {
            if (this.blockCollector.size < this.outputQueueLimit && this.paused) {
                this.resumeReading();
            }
        }, 1000);
    }

    log(level: string, message?: any, ...optionalParams: any[]): void {
        if (logLevelToInt(this.logLevel) >= logLevelToInt(level))
            console.log(`[${(new Date()).toISOString().slice(0, -1)}][READER][${level.toUpperCase()}]`, message, ...optionalParams);
    }

    private async processInputQueue(tasks: any[]) {
        // this.log('info', `Tasks: ${tasks.length} | Decoding Queue: ${this.decodingQueue.length()}`);
        for (const task of tasks) {
            this.decodingQueue.push(task, null);
        }
        if ((this.decodingQueue.length() > this.inputQueueLimit || this.blockCollector.size >= this.outputQueueLimit) && !this.paused) {
            this.paused = true;
            this.pendingAck = tasks.length;
            this.inputQueue.pause();
            this.log('info', 'Reader paused!');
        } else {
            this.ackBlockRange(tasks.length);
        }
    }

    start() {
        if (this.connecting)
            throw new Error('Reader already connecting');

        this.log('info', `Connecting to ${this.shipApi}...`);
        this.connecting = true;

        this.ship.connect(
            (data: RawData) => {
                this.handleShipMessage(data as Buffer).catch((e) => this.log('error', e));
            },
            () => {
                this.connecting = false;
                this.shipAbiReady = false;
                if (this.onDisconnect)
                    this.onDisconnect();
            },
            (err) => {
                this.connecting = false;
                this.shipAbiReady = false;
                if (this.onError)
                    this.onError(err);
            },
            () => {
                this.connecting = false;
                if (this.onConnected)
                    this.onConnected();
            }
        );
    }

   stop() {
        this.log('info', 'Stopping...');
        clearInterval(this._reporterTask);
        clearInterval(this._resumerTask);
        this.ship.close();
        this.shipAbiReady = false;
        this.blockHistory.clear();
        this.blockCollector.clear();
        this.dsPool.forEach((worker) => worker.terminate());
    }

    restart(ms: number = 3000) {
        this.log('info', 'Restarting...');
        this.ship.close();
        this.shipAbiReady = false;
        this.blockHistory.clear()
        this.blockCollector.clear()
        setTimeout(() => {
            this.reconnectCount++;
            this.startBlock = this.lastEmittedBlock + 1;
            this.start();
        }, ms);
    }

    private send(param: (string | any)[]) {
        this.ship.send(Serializer.encode({
            type: 'request',
            object: param,
            abi: this.shipAbi
        }).array);
    }

    private ackBlockRange(size: number) {
        this.send(['get_blocks_ack_request_v0', {num_messages: size}]);
    }

    private async handleShipMessage(msg: Buffer) {
        if (!this.shipAbiReady) {
            this.loadShipAbi(msg);
            return;
        }
        const result = Serializer.decode({type: 'result', abi: this.shipAbi, data: msg});
        switch (result[0]) {
            case 'get_blocks_result_v0': {
                try {
                    this.inputQueue.push(result[1], null);
                } catch (e) {
                    this.log('error', '[decodeShipData]', e.message);
                    this.log('error', e);
                }
                break;
            }
            case 'get_status_result_v0': {
                const data = Serializer.objectify(result[1]) as any;
                this.log('info', `Head block: ${data.head.block_num}`);
                if (this.startBlock < 0) {
                    this.startBlock = (this.irreversibleOnly ? data.last_irreversible.block_num : data.head.block_num) + this.startBlock;
                } else {
                    // TODO: should we error here if the requested start block is after LIB?
                }
                this.requestBlocks({
                    from: this.startBlock,
                    to: this.endBlock
                });
                this.shipInitStatus = data;
                break;
            }
        }
    }

    private loadShipAbi(data: RawData) {
        const abi = JSON.parse(data.toString());
        this.shipAbi = ABI.from(abi);
        this.dsPool.forEach(value => {
            value.postMessage({
                event: 'set_ship_abi',
                data: {abi}
            });
        });
        this.shipAbiReady = true;
        this.send(['get_status_request_v0', {}]);
        this.ackBlockRange(1);
    }

    private requestBlocks(param: { from: number; to: number }) {
        this.log('info', `Requesting blocks from ${param.from} to ${param.to}`);
        this.send(['get_blocks_request_v0', {
            start_block_num: param.from,
            end_block_num: param.to,
            max_messages_in_flight: this.maxMessagesInFlight,
            have_positions: [],
            irreversible_only: this.irreversibleOnly,
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true,
        }]);
    }

    private async decodeShipData(resultElement: any) {
        const blockInfo = Serializer.objectify({
            head: resultElement.head,
            last_irreversible: resultElement.last_irreversible,
            this_block: resultElement.this_block,
            prev_block: resultElement.prev_block
        });

        const prevBlockNum = blockInfo.prev_block.block_num;
        const prevBlockId = blockInfo.prev_block.block_id;

        const blockNum = blockInfo.this_block.block_num;
        const blockId = blockInfo.this_block.block_id;

        this.log('debug', '[decodeShipData]:');
        this.log('debug', `prev: #${prevBlockNum} - ${prevBlockId}`);
        this.log('debug', `this: #${blockNum} - ${blockId}`);

        // fork handling;
        if (this.blockHistory.has(blockNum)) {
            let i = blockNum;
            this.log('debug', 'FORK detected!');
            this.log('debug', 'blockHistory last 40 values before handling:');
            this.log('debug', this.blockHistory.queue.slice(-40));

            this.log('debug', `purging block collector from ${i}`);

            while(this.blockCollector.delete(i))
                i++;

            this.log('debug', `done, purged up to ${i}`);
            this.log('debug', `purging block history from ${blockNum}`);

            this.blockHistory.deleteFrom(blockNum);

            this.log('debug', 'blockHistory last 40 values after handling:');
            this.log('debug', this.blockHistory.queue.slice(-40));

            this.log('debug', `blockHistory has #${blockNum}? ${this.blockHistory.has(blockNum)}`);

            this.log('debug', 'done.');
            this.lastEmittedBlock = 0;
            this.nextBlockRequested = 0;
        }

        this.blockHistory.add(blockNum);

        if (resultElement.block && blockNum) {

            const block = Serializer.decode({
                type: 'signed_block',
                data: resultElement.block.array as Uint8Array,
                abi: this.shipAbi
            }) as any;

            const blockHeader = Serializer.objectify({
                timestamp: block.timestamp,
                producer: block.producer,
                confirmed: block.confirmed,
                previous: block.previous,
                transaction_mroot: block.transaction_mroot,
                action_mroot: block.action_mroot,
                schedule_version: block.schedule_version,
                new_producers: block.new_producers,
                header_extensions: block.header_extensions,
                producer_signature: block.producer_signature,
                block_extensions: block.block_extensions,
            });

            const extendedDeltas = [];
            if (resultElement.deltas) {
                const deltaArrays = Serializer.decode({
                    type: 'table_delta[]',
                    data: resultElement.deltas.array as Uint8Array,
                    abi: this.shipAbi
                }) as any[];


                // process deltas
                for (let deltaArray of deltaArrays) {

                    // make sure the ABI for the watched contracts is updated before other processing is done
                    if (deltaArray[1].name === 'account') {
                        const abiRows = deltaArray[1].rows.map(r => {
                            if (r.present && r.data.array) {
                                const decodedRow = Serializer.decode({
                                    type: 'account',
                                    data: r.data.array,
                                    abi: this.shipAbi
                                });
                                if (decodedRow[1].abi) {
                                    return Serializer.objectify(decodedRow[1]);
                                }
                            }
                            return null;
                        }).filter(r => r !== null);
                        abiRows.forEach((abiRow, index) => {
                            if (this.allowedContracts.has(abiRow.name)) {
                                this.log('info', abiRow.name, `block_num: ${blockNum}`, abiRow.creation_date, `abi hex len: ${abiRow.abi.length}`);
                                if (abiRow.abi.length == 0)
                                    return;
                                console.time(`abiDecoding-${abiRow.name}-${blockNum}`);
                                const abiBin = new Uint8Array(Buffer.from(abiRow.abi, 'hex'));
                                const abi = ABI.fromABI(new ABIDecoder(abiBin));
                                this.addContract(abiRow.name, abi);
                                console.timeEnd(`abiDecoding-${abiRow.name}-${blockNum}`);
                            }
                        });
                    }


                    if (deltaArray[1].name === 'contract_row') {
                        let j = 0;
                        deltaArray[1].rows.forEach((row: any, index: number) => {
                            const deltaRow = Serializer.decode({
                                data: row.data.array,
                                type: 'contract_row',
                                abi: this.shipAbi
                            })[1];
                            const deltaObj = Serializer.objectify(deltaRow);
                            if (this.allowedContracts.has(deltaObj.code)) {
                                const extDelta = {
                                    present: row.present,
                                    ...deltaObj
                                };
                                const key = `${blockNum}:${index}`;
                                this.deltaRefMap.set(key, extDelta);
                                extendedDeltas.push(this.deltaRefMap.get(key));
                                this.dsPool[j].postMessage({
                                    event: 'delta',
                                    content: {
                                        index,
                                        blockNum,
                                        blockId,
                                        extDelta
                                    }
                                });
                                // round-robin to pools
                                j++;
                                if (j > this.dsPool.length - 1) {
                                    j = 0;
                                }
                            }
                        });
                    }
                }
            }

            const extendedActions = [];
            if (resultElement.traces) {
                const traces = Serializer.decode({
                    type: 'transaction_trace[]',
                    data: resultElement.traces.array as Uint8Array,
                    abi: this.shipAbi,
                    ignoreInvalidUTF8: true
                }) as any[];

                // process traces
                for (let trace of traces) {
                    let j = 0;
                    const rt = Serializer.objectify(trace[1]);
                    if (!rt.partial || rt.partial.length < 2)
                        continue;

                    const partialTransaction = rt.partial[1];

                    for (const at of rt.action_traces) {
                        const actionTrace = at[1];
                        if (actionTrace.receipt === null) {
                            this.log('warning', `action trace with receipt null! maybe hard_fail'ed deferred tx? block: ${blockNum}`);
                            continue;
                        }
                        if (this.allowedContracts.has(actionTrace.act.account)) {
                            const abiActionNames = [];
                            this.allowedContracts.get(actionTrace.act.account).actions.forEach((obj) => {
                                abiActionNames.push(obj.name.toString());
                            });
                            if (!abiActionNames.includes(actionTrace.act.name)) {
                                this.log(
                                    'warning',
                                    `action ${actionTrace.act.name} not found in ${actionTrace.act.account}'s abi, ignoring tx ${rt.id}...`);
                                continue;
                            }
                            const gs = actionTrace.receipt[1].global_sequence;
                            const extAction = {
                                actionOrdinal: actionTrace.action_ordinal,
                                creatorActionOrdinal: actionTrace.creator_action_ordinal,
                                trxId: rt.id,
                                cpu: rt.cpu_usage_us,
                                net: rt.net_usage_words,
                                ram: actionTrace.account_ram_deltas,
                                receipt: actionTrace.receipt[1],
                                receiver: actionTrace.receiver,
                                console: actionTrace.console,
                                signatures: partialTransaction.signatures,
                                act: actionTrace.act
                            };
                            this.actionRefMap.set(gs, extAction);
                            extendedActions.push(this.actionRefMap.get(gs));
                            this.dsPool[j].postMessage({
                                event: 'action',
                                data: {
                                    gs,
                                    blockNum,
                                    blockId,
                                    act: actionTrace.act
                                }
                            });
                            // round-robin to pools
                            j++;
                            if (j > this.dsPool.length - 1) {
                                j = 0;
                            }
                        }
                    }
                }
            }

            const nBlock = {
                ready: false,
                blockInfo,
                blockHeader,
                counters: {
                    actions: 0,
                    deltas: 0
                },
                targets: {
                    actions: extendedActions.length,
                    deltas: extendedDeltas.length,
                },
                deltas: extendedDeltas,
                actions: extendedActions,
                createdAt: process.hrtime.bigint()
            };

            this.blockCollector.set(blockNum, nBlock);

            if (extendedDeltas.length == 0 && extendedActions.length == 0)
                this.checkBlock(nBlock);
        }
    }

    createWorkers(param: { poolSize: number }) {
        for (let i = 0; i < param.poolSize; i++) {
            const __dirname = fileURLToPath(new URL('.', import.meta.url));
            const w = new Worker(path.join(__dirname, 'ds-worker.js'), {
                workerData: {
                    wIndex: i
                }
            });
            w.on("message", value => {
                this.handleWorkerMessage(value);
            });
            this.dsPool.push(w);
        }
        this.log('info', `Pool created with ${this.dsPool.length} workers`);
    }

    private handleWorkerMessage(value: any) {
        switch (value.event) {
            case 'request_head_abi': {
                if (!this.abiRequests[value.contract]) {
                    this.abiRequests[value.contract] = true;
                    this.api.v1.chain.get_abi(value.contract).then(abiData => {
                        this.log('info', `Current ABI loaded for ${abiData.account_name}`);
                        this.addContract(abiData.account_name, ABI.from(abiData.abi));
                        this.abiRequests[value.contract] = false;
                    });
                }
                break;
            }
            case 'decoded_delta': {
                this.collectDelta(value.data);
                // if (this.deltaCollector) {
                //     this.deltaCollector(value);
                // }
                break;
            }
            case 'decoded_action': {
                this.collectAction(value.data);
                // if (this.traceCollector) {
                //     this.traceCollector(value);
                // }
                break;
            }
        }
    }

    addContract(account: string, abi: ABI) {
        this.allowedContracts.set(account, abi);
        this.dsPool.forEach(value => {
            value.postMessage({
                event: 'set_abi',
                data: {account, abi: Serializer.objectify(abi)}
            });
        });
    }

    ack() {
        const nextBlock = this.blockCollector.get(this.lastEmittedBlock + 1);
        if (nextBlock && nextBlock.ready) {
            delete nextBlock.ready;
            this.lastEmittedBlock = nextBlock.blockInfo.this_block.block_num;
            this.blockCollector.delete(nextBlock.blockInfo.this_block.block_num);
            this.nextBlockRequested = 0;
            this.events.emit('block', nextBlock);
        } else {
            this.nextBlockRequested = this.lastEmittedBlock + 1;
        }
    }

    private collectAction(data) {
        const refAction = this.actionRefMap.get(data.gs);
        refAction.act.data = data.act.data;
        const block = this.blockCollector.get(data.blockNum);
        const blockId = block.blockInfo.this_block.block_id;
        if (blockId != data.blockId) {
            this.log(
                'warning',
                `discarding data due to fork on block #${data.blockNum}, data id: ${data.blockId}, collector id: ${blockId}`);
            return
        }

        block.counters.actions++;
        this.actionRefMap.delete(data.gs);
        this.checkBlock(block);
    }

    private collectDelta(data) {
        const key = `${data.blockNum}:${data.index}`;
        const refDelta = this.deltaRefMap.get(key);
        refDelta.value = data.value;
        const block = this.blockCollector.get(data.blockNum);
        const blockId = block.blockInfo.this_block.block_id;
        if (blockId != data.blockId) {
            this.log(
                'warning',
                `discarding data due to fork on block #${data.blockNum}, data id: ${data.blockId}, collector id: ${blockId}`);
            return
        }
        block.counters.deltas++;
        this.deltaRefMap.delete(key);
        this.checkBlock(block);
    }

    private checkBlock(block) {
        if (block.counters.actions === block.targets.actions && block.counters.deltas === block.targets.deltas) {
            const elapsed = process.hrtime.bigint() - block.createdAt;
            block.proc_time_us = Number(elapsed / BigInt(1000));
            delete block.createdAt;
            delete block.counters;
            delete block.targets;
            block.ready = true;
            // check if this block can be emitted directly
            if (this.lastEmittedBlock === 0 || this.nextBlockRequested === block.blockInfo.this_block.block_num) {
                if (this.nextBlockRequested === block.blockInfo.this_block.block_num) {
                    this.nextBlockRequested = 0;
                }
                delete block.ready;
                this.lastEmittedBlock = block.blockInfo.this_block.block_num;
                this.blockCollector.delete(block.blockInfo.this_block.block_num);
                this.events.emit('block', block);
            }
        }
    }

    private resumeReading() {
        this.inputQueue.resume();
        this.paused = false;
        this.ackBlockRange(this.pendingAck);
        this.log('info', 'Reader resumed!');
    }
}
