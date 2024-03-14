import {RawData} from "ws";
import {EventEmitter} from "events";
import * as path from "path";
import {fileURLToPath} from "node:url";
import {queue, QueueObject} from "async";
import fetch from "node-fetch";
import * as process from "process";
import * as console from "console";

import {addOnBlockToABI, logLevelToInt, ThroughputMeasurer} from "./utils.js";
import {ActionDSMessage, ActionDSResponse, DeltaDSMessage, ShipDSMessage} from "./ds-worker.js";
import {StateHistorySocket} from "./state-history.js";
import {OrderedSet} from "./orderedset.js";

import {ABI, ABIDecoder, APIClient, Serializer} from "@greymass/eosio";
import {SharedObject, SharedObjectStore} from "shm-store";
import workerpool, {Pool, Promise as PoolPromise} from "workerpool";
import {ActionTrace, ActionWithExtras, DecodedAction, DecodedBlock, TableDelta} from "./types/antelope.js";
import FastPriorityQueue from "fastpriorityqueue";

const HIST_TIME = 15 * 60 * 2;  // 15 minutes in blocks

export interface HyperionSequentialReaderOptions {
    shipApi: string;
    chainApi: string;
    poolSize: number;
    irreversibleOnly?: boolean;
    maxMessagesInFlight?: number;
    blockConcurrency?: number;
    blockHistorySize?: number;
    startBlock?: number;
    endBlock?: number;
    logLevel?: string;
    workerLogLevel?: string;
    maxPayloadMb?: number;
    actionWhitelist?: {[key: string]: string[]};  // key is code name, value is list of actions
    tableWhitelist?: {[key: string]: string[]};   // key is code name, value is list of tables,
    speedMeasureConf?: {
        windowSizeMs: number;
        deltaMs: number;
    };
    skipInitialBlockCheck?: boolean;
}

export class HyperionSequentialReader {
    ship: StateHistorySocket;
    max_payload_mb: number;
    reconnectCount = 0;
    private connecting = false;
    private forked: boolean = false;
    private lastFork: number;
    private shipAbi?: ABI;
    private shipAbiReady = false;
    events = new EventEmitter();

    private dsPool: Pool;

    private sharedABIStore: SharedObjectStore<ABI.Def>;

    private contracts: Map<string, ABI.Def> = new Map();
    private actionWhitelist: Map<string, string[]>;
    private tableWhitelist: Map<string, string[]>;

    startBlock: number;
    endBlock: number;

    blockConcurrency: number;
    decodingQueue: QueueObject<any>;

    maxMessagesInFlight: number;

    perfMetrics: ThroughputMeasurer;
    readonly speedMeasureWindowSize: number;
    readonly speedMeasureDeltaMs: number;
    private blocksSinceLastMeasure: number = 0;
    private _perfMetricTask;

    private readonly skipInitialBlockCheck: boolean;

    blockCollector: FastPriorityQueue<DecodedBlock> = new FastPriorityQueue(
        (a: DecodedBlock, b: DecodedBlock) => a.blockNum < b.blockNum
    );

    blockHistory: OrderedSet<number>;
    blockHistorySize: number;
    logLevel: string;

    api: APIClient;

    shipApi: string;

    // last block emitted to user
    private lastEmittedBlock: number;

    private irreversibleOnly: boolean;

    onConnected: () => void = null;
    onDisconnect: () => void = null;
    onError: (err) => void = null;

    constructor(private options: HyperionSequentialReaderOptions) {

        this.logLevel = options.logLevel || 'warning';
        this.shipApi = options.shipApi;
        this.max_payload_mb = options.maxPayloadMb || 256;
        this.ship = new StateHistorySocket(this.shipApi, this.max_payload_mb);

        this.blockHistorySize = options.blockHistorySize || HIST_TIME;
        this.blockHistory = new OrderedSet<number>(this.blockHistorySize);

        // placeholder store
        this.sharedABIStore = new SharedObjectStore({bufferLengthBytes: 0});

        this.api = new APIClient({
            url: options.chainApi,
            fetch
        });

        this.createWorkers({
            poolSize: options.poolSize || 1,
            logLevel: options.workerLogLevel || 'warning'
        });

        this.irreversibleOnly = options.irreversibleOnly || false

        this.startBlock = options.startBlock || -1;
        this.lastEmittedBlock = this.startBlock - 1;
        this.endBlock = options.endBlock || -1;

        this.blockConcurrency = options.blockConcurrency || 2;
        this.maxMessagesInFlight = options.maxMessagesInFlight || 1000;

        if (options.actionWhitelist)
            this.actionWhitelist = new Map(Object.entries(options.actionWhitelist));

        if (options.tableWhitelist)
            this.tableWhitelist = new Map(Object.entries(options.tableWhitelist));

        this.speedMeasureDeltaMs = 1000;
        this.speedMeasureWindowSize = 10 * 1000;
        if (options.speedMeasureConf) {
            if (options.speedMeasureConf.windowSizeMs)
                this.speedMeasureWindowSize = options.speedMeasureConf.windowSizeMs;

            if (options.speedMeasureConf.deltaMs)
                this.speedMeasureDeltaMs = options.speedMeasureConf.deltaMs;
        }
        this.perfMetrics = new ThroughputMeasurer({windowSizeMs: this.speedMeasureWindowSize})
        this._perfMetricTask = setInterval(() => {
            this.perfMetrics.measure(this.blocksSinceLastMeasure);
            this.blocksSinceLastMeasure = 0;
        }, this.speedMeasureDeltaMs);

        this.skipInitialBlockCheck = !!options.skipInitialBlockCheck;

        this.decodingQueue = queue(
            this.decodeShipData.bind(this), this.blockConcurrency);
    }

    get isShipAbiReady(): boolean {
        return this.shipAbiReady;
    }

    isActionRelevant(account: string, name: string): boolean {
        return (
            this.contracts.has(account) && (
                !this.actionWhitelist ||
                (this.actionWhitelist.has(account) &&
                 this.actionWhitelist.get(account).includes(name))
            )
        );
    }

    isDeltaRelevant(code: string, table: string): boolean {
        return (
            this.contracts.has(code) && (
                !this.tableWhitelist ||
                (this.tableWhitelist.has(code) &&
                 this.tableWhitelist.get(code).includes(table))
            )
        );
    }

    log(level: string, message?: any, ...optionalParams: any[]): void {
        if (logLevelToInt(this.logLevel) >= logLevelToInt(level))
            console.log(`[${(new Date()).toISOString().slice(0, -1)}][READER][${level.toUpperCase()}]`, message, ...optionalParams);
    }

    async start() {
        if (this.connecting)
            throw new Error('Reader already connecting');

        if (!this.skipInitialBlockCheck) {
            // check if target node is up & contains requested range
            await this.api.v1.chain.get_info();

            if (this.startBlock > 0)
                await this.api.v1.chain.get_block(this.startBlock);

            if (this.endBlock > 0)
                await this.api.v1.chain.get_block(this.endBlock);
        }

        this.log('info', 'Node range check done!');
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

   async stop() {
        this.log('info', 'Stopping...');
        clearInterval(this._perfMetricTask);
        this.ship.close();
        this.shipAbiReady = false;
        this.blockHistory.clear();
        this.blockCollector.removeMany(b => true);
        this.blockCollector.trim();
        await this.dsPool.terminate();
    }

    restart(ms: number = 3000, forceBlock?: number) {
        this.log('info', 'Restarting...');
        this.ship.close();
        this.shipAbiReady = false;
        this.blockHistory.clear()
        this.blockCollector.removeMany(b => true);
        this.blockCollector.trim();
        this.decodingQueue.remove(task => true);

        // TODO:
        // found block emission gap on translator, so add api to
        // force a specific block to be the restart block, this is only
        // a mitigation, needs to be investigated further
        const restartBlock = forceBlock ? forceBlock : this.lastEmittedBlock + 1;
        setTimeout(async () => {
            this.reconnectCount++;
            this.startBlock = restartBlock;
            await this.start();
            this.events.emit('restarted');
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
                    this.decodingQueue.push(result[1], null);
                } catch (e) {
                    this.log('error', '[decodeShipData]', e.message);
                    this.log('error', e);
                }
                break;
            }
            case 'get_status_result_v0': {
                const data = Serializer.objectify(result[1]) as any;
                this.log('info', `Head block: ${data.head.block_num}`);
                const beginShipState = data.chain_state_begin_block;
                const endShipState = data.chain_state_end_block;
                if (this.startBlock < 0) {
                    this.startBlock = (this.irreversibleOnly ? data.last_irreversible.block_num : data.head.block_num) + this.startBlock;
                } else {
                    if (this.irreversibleOnly && this.startBlock > data.last_irreversible.block_num)
                        throw new Error(`irreversibleOnly true but startBlock > ship LIB`);
                }

                // only care about end state if end block < 0 or end block is max posible
                if (this.endBlock != 0xffffffff - 1)
                    if (this.endBlock < 0)
                        this.endBlock = 0xffffffff - 1;
                    else if (this.endBlock > endShipState)
                        throw new Error(`End block ${this.endBlock} not in chain_state, end state: ${endShipState}`);

                if (this.startBlock <= beginShipState)
                    throw new Error(`Start block ${this.startBlock} not in chain_state, begin state: ${beginShipState} (must be +1 to startBlock)`);

                this.lastEmittedBlock = this.startBlock - 1;
                this.requestBlocks(this.startBlock, this.endBlock);
                break;
            }
        }
    }

    private loadShipAbi(data: Buffer) {
        this.log('info', `loading ship abi of size: ${data.length}`)
        const abi = JSON.parse(data.toString());
        this.shipAbi = ABI.from(abi);
        this.addContract('shipAbi', abi);
        this.shipAbiReady = true;
        this.send(['get_status_request_v0', {}]);
        this.ackBlockRange(1);
    }

    private requestBlocks(from: number = -1, to: number = 0xffffffff) {
        this.log('info', `Requesting blocks from ${from} to ${to}`);
        this.send(['get_blocks_request_v0', {
            start_block_num: from - 1,
            end_block_num: to,
            max_messages_in_flight: this.maxMessagesInFlight,
            have_positions: [],
            irreversible_only: this.irreversibleOnly,
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true,
        }]);
    }

    private async decodeShipData(resultElement: any) {
        const startDecodeTime = performance.now();
        const blockInfo = Serializer.objectify({
            head: resultElement.head,
            last_irreversible: resultElement.last_irreversible,
            this_block: resultElement.this_block,
            prev_block: resultElement.prev_block
        });

        const blockNum = blockInfo.this_block.block_num;

        if (blockNum < this.startBlock) {
            this.log('warning', `received block (${blockNum}) which is < startBlock (${this.startBlock})... skip...`);
            return;
        }

        const blockId = blockInfo.this_block.block_id;
        const prevBlockNum = blockInfo.prev_block.block_num;
        const prevBlockId = blockInfo.prev_block.block_id;

        this.log('debug', '[decodeShipData]:');
        this.log('debug', `prev: #${prevBlockNum} - ${prevBlockId}`);
        this.log('debug', `this: #${blockNum} - ${blockId}`);

        // fork handling;
        if (this.blockHistory.has(blockNum)) {
            this.forked = true;
            this.log('debug', 'FORK detected!');
            this.lastFork = blockNum;
            this.log('debug', 'blockHistory last 40 values before handling:');
            this.log('debug', this.blockHistory.queue.slice(-40));

            this.log('debug', `purging block collector from ${blockNum}`);

            let higestCleaned = blockNum;
            this.blockCollector.removeMany(b => {
                const thisNum = b.blockInfo.this_block.block_num;
                if (thisNum > higestCleaned)
                    higestCleaned = thisNum;
                return thisNum >= blockNum;
            });

            this.log('debug', `done, purged up to ${higestCleaned}`);
            this.log('debug', `purging block history from ${blockNum}`);

            this.blockHistory.deleteFrom(blockNum);

            this.log('debug', 'blockHistory last 40 values after handling:');
            this.log('debug', this.blockHistory.queue.slice(-40));

            this.log('debug', `blockHistory has #${blockNum}? ${this.blockHistory.has(blockNum)}`);

            this.log('debug', 'done.');

            const lastNonForked = blockNum - 1;
            this.lastEmittedBlock = this.lastEmittedBlock > lastNonForked ? lastNonForked : this.lastEmittedBlock;
            this.forked = false;
        }

        if (this.forked && blockNum > this.lastFork)
            return;

        this.blockHistory.add(blockNum);

        if (resultElement.block && blockNum) {

            const blockDecodeParams: ShipDSMessage = {
                shmRef: this.sharedABIStore.sharedMem,
                memMap: this.sharedABIStore.getMemoryMap(),
                blockId, blockNum,
                type: 'signed_block',
                data: resultElement.block.array as Uint8Array
            };
            const block = await this.dsPool.exec('decodeShip', [blockDecodeParams]);

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

            const workerTasks: PoolPromise<any, any>[] = [];
            const deltas: TableDelta[] = [];
            const actions: ActionWithExtras[] = [];

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
                        abiRows.forEach((abiRow) => {
                            if (this.contracts.has(abiRow.name)) {
                                this.log('info', abiRow.name, `block_num: ${blockNum}`, abiRow.creation_date, `abi hex len: ${abiRow.abi.length}`);
                                if (abiRow.abi.length == 0)
                                    return;
                                console.time(`abiDecoding-${abiRow.name}-${blockNum}`);
                                const abiBin = new Uint8Array(Buffer.from(abiRow.abi, 'hex'));
                                const abi: ABI.Def = Serializer.objectify(ABI.fromABI(new ABIDecoder(abiBin)));
                                this.addContract(abiRow.name, abi);
                                console.timeEnd(`abiDecoding-${abiRow.name}-${blockNum}`);
                            }
                        });
                    }


                    if (deltaArray[1].name === 'contract_row') {
                        deltaArray[1].rows.forEach(row => {
                            const deltaRow = Serializer.decode({
                                data: row.data.array,
                                type: 'contract_row',
                                abi: this.shipAbi
                            })[1];
                            const delta: TableDelta = Serializer.objectify(deltaRow);
                            if (!this.isDeltaRelevant(delta.code, delta.table))
                                return;

                            const deltaDSParams: DeltaDSMessage = {
                                shmRef: this.sharedABIStore.sharedMem,
                                memMap: this.sharedABIStore.getMemoryMap(),
                                blockId, blockNum,
                                data: delta
                            };
                            workerTasks.push(
                                this.dsPool.exec(
                                    'processDelta', [deltaDSParams]
                                ).then(delta => deltas.push(delta))
                            );
                        });
                    }
                }
            }

            if (resultElement.traces) {
                const traces = Serializer.decode({
                    type: 'transaction_trace[]',
                    data: resultElement.traces.array as Uint8Array,
                    abi: this.shipAbi,
                    ignoreInvalidUTF8: true
                }) as [string, any][];

                const relevantTraces: [any, ActionTrace, string[]][] = []
                traces.forEach((trace: [string, any]) => {
                    const rt = Serializer.objectify(trace[1]);
                    if (!rt.partial || rt.partial.length < 2)
                        return;

                    const signatures: string[] = rt.partial[1].signatures;

                    for (const [dType, actionTrace] of rt.action_traces as [string, ActionTrace][]) {
                        if (actionTrace.receipt === null) {
                            this.log('warning', `action trace with receipt null! maybe hard_fail'ed deferred tx? block: ${blockNum}`);
                            continue;
                        }
                        if (this.isActionRelevant(actionTrace.act.account, actionTrace.act.name))
                            relevantTraces.push([rt, actionTrace, signatures]);
                    }
                });

                relevantTraces.forEach((actionData: [any, ActionTrace, string[]]) => {
                    const [trace, actionTrace, signatures] = actionData;
                    const abiActionNames = this.contracts.get(actionTrace.act.account).actions.map(a => a.name);
                    if (!abiActionNames.includes(actionTrace.act.name)) {
                        this.log(
                            'warning',
                            `action ${actionTrace.act.name} not found in ${actionTrace.act.account}'s abi, ignoring tx ${trace.id}...`);
                        return;
                    }
                    const actionDSParams: ActionDSMessage = {
                        shmRef: this.sharedABIStore.sharedMem,
                        memMap: this.sharedABIStore.getMemoryMap(),
                        blockId, blockNum,
                        data: actionTrace.act
                    };
                    workerTasks.push(
                        this.dsPool.exec(
                            'processAction', [actionDSParams]
                        ).then((decodedAct: ActionDSResponse) => {
                            actions.push({
                                actionOrdinal: actionTrace.action_ordinal,
                                creatorActionOrdinal: actionTrace.creator_action_ordinal,
                                trxId: trace.id,
                                cpu: trace.cpu_usage_us,
                                net: trace.net_usage_words,
                                ram: actionTrace.account_ram_deltas,
                                receipt: actionTrace.receipt[1],
                                receiver: actionTrace.receiver,
                                console: actionTrace.console,
                                signatures: signatures,
                                act: decodedAct.data
                            });
                        })
                    );
                });
            }

            await Promise.all(workerTasks);

            if (this.forked && blockNum > this.lastFork)
                return;

            const decodeElapsed: number = performance.now() - startDecodeTime;

            this.blockCollector.add({
                blockNum, blockInfo, blockHeader,
                deltas, actions,
                decodeElapsed
            })

            this.maybeEmitBlock();
        }
    }

    createWorkers(param: { poolSize: number, logLevel: string }) {
        const __dirname = fileURLToPath(new URL('.', import.meta.url));
        const workerModule = path.join(__dirname, 'ds-worker.js');
        process.env.WORKER_LOG_LEVEL = param.logLevel;
        this.dsPool = workerpool.pool(
            workerModule, {
                minWorkers: param.poolSize,
                maxWorkers: param.poolSize,
                workerType: 'thread'
        });
        this.log('info', `Pool created with ${param.poolSize} workers`);
    }

    private updateSharedABIStore() {
        const objectMap: {[keys: string]: SharedObject<ABI.Def>} = {};
        for (const [account, abi] of this.contracts.entries())
            objectMap[account] = SharedObject.fromObject<ABI.Def>(abi);

        this.sharedABIStore = SharedObjectStore.fromObjects<ABI.Def>(objectMap);
    }

    addContract(account: string, abi: ABI.Def) {
        if (account == 'eosio')
            addOnBlockToABI(abi);

        this.contracts.set(account, abi);
        this.updateSharedABIStore();
    }

    addContracts(contracts: {account: string, abi: ABI.Def}[]) {
        for (const contract of contracts) {
            if (contract.account == 'eosio')
                addOnBlockToABI(contract.abi);

            this.contracts.set(contract.account, contract.abi);
        }
        this.updateSharedABIStore();
    }

    private maybeEmitBlock() {
        if (this.blockCollector.isEmpty())
            return;

        const topBlock = this.blockCollector.peek();

        if (topBlock.blockNum != this.lastEmittedBlock + 1)
            return;

        this.events.emit('block', this.blockCollector.poll())
        this.lastEmittedBlock = topBlock.blockNum;
        this.blocksSinceLastMeasure++;

        if (topBlock.blockNum == this.endBlock) {
            this.log('info', `Finished reading range ${this.startBlock} to ${this.endBlock}`);
            this.stop().then(() => this.events.emit('stop'));
        }
    }

    ack(range: number = 1) {
        this.maybeEmitBlock();
        this.ackBlockRange(range);
    }
}
