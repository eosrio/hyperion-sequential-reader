import {SharedObject, SharedObjectStore} from "../shm.js";
import {readFileSync} from "node:fs";
import {ABI} from "@greymass/eosio";
import {BSON} from "bson";
import {expect} from "chai";
import workerpool from "workerpool";
import {fileURLToPath} from "node:url";
import path from "path";
const __dirname = fileURLToPath(new URL('.', import.meta.url));

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    throw new Error(`Unhandled Rejection: ${reason}`);
});


const abis = ['eosio', 'telos.evm', 'eosio.token'].map((abiFileNames) => {
    const jsonAbi = JSON.parse(readFileSync(`./${abiFileNames}.abi`).toString())
    return {account: jsonAbi.account_name, abi: ABI.from(jsonAbi.abi)};
});

async function testSOSCreation() {
    // source data from .abi files also create shared objects
    const testData: {[key: string]: {abi: ABI.Def, shared: SharedObject<ABI.Def>}} = {};
    for (const abiInfo of abis) {
        testData[abiInfo.account] = {
            abi: abiInfo.abi,
            shared: SharedObject.fromObject<ABI.Def>(abiInfo.abi)
        };
    }

    // make sure buffers in shared objects make sense
    for (const data of Object.values(testData)) {
        expect(data.shared.obj).to.be.deep.eq(data.abi);
        expect(data.shared.raw).to.be.deep.eq(BSON.serialize(data.abi));
    }

    // create SOS at main thread
    const objMapping = {};
    for (const [account, data] of Object.entries(testData))
        objMapping[account] = data.shared;

    const sos = SharedObjectStore.fromObjects<ABI.Def>(objMapping);

    for (const [account, data] of Object.entries(testData))
        expect(sos.get(account)).to.be.deep.eq(data.abi);

    const pool = workerpool.pool(
        path.join(__dirname, 'testWorker.js'),
        {
            minWorkers: 3,
            maxWorkers: 3,
            workerType: 'thread'
        }
    );
    const fetchOneFromWorkerSOS = async (key: string) => {
        let result: ABI.Def;
        let error: any | undefined;
        try {
            result = await pool.exec('sosFetchObject', [{
                sharedMem: sos.sharedMem,
                memoryMap: sos.getMemoryMap(),
                key
            }]);
        } catch (e) {
            error = e;
            console.error(e.message);
            console.error(e.stack);
        }
        return {result, error};
    };

    await Promise.all(
        Object.entries(testData).map(async ([account, data]) => {
            const fetchResult = await fetchOneFromWorkerSOS(account);
            expect(fetchResult.error, `worker returned error while fetch ${account}!`).to.be.undefined;
            expect(fetchResult.result, `worker result different from expected!`).to.be.deep.eq(data.abi);
        })
    );

    await pool.terminate();
}

await testSOSCreation();