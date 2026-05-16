const fs   = require("fs");
const path = require("path");
const { ethers } = require("ethers");

/////////////////////////////
// CONFIG
/////////////////////////////

const RPC_URL            = "https://arc-testnet.g.alchemy.com/v2/o1k50yOLGXHrczBA8KDOf";
const NFT_CONTRACT       = "0x9e05c6075f9e890fc515ef86091414c77036f8fa";
const NFT_CREATION_BLOCK = 9435462;
const BLOCK_BATCH        = 2000;
const CONCURRENCY        = 20;
const MAX_RETRIES        = 5;
const RETRY_DELAY_MS     = 1000;

const OUTPUT_FILE     = path.resolve(__dirname, "holders.csv");
const CHECKPOINT_FILE = path.resolve(__dirname, ".checkpoint.json");

const TRANSFER_TOPIC  = ethers.id("Transfer(address,address,uint256)");
const ZERO_ADDR       = "0x0000000000000000000000000000000000000000";

/////////////////////////////
// CHECKPOINT SCHEMA
//
// {
//   contract    : string    — NFT contract address (guards against wrong file)
//   nextBlock   : number    — next block to scan on the next run
//   balances    : { [addr]: string }  — all resolved balances (bal > 0 only)
//   failed      : string[]  — addresses that exhausted all retries
// }
//
// The checkpoint is NEVER deleted after a successful run.
// nextBlock advances to latestBlock+1 so the next run picks up from there.
// Re-running with a higher block range extends the existing results.
/////////////////////////////

function loadCheckpoint() {
    if (!fs.existsSync(CHECKPOINT_FILE)) return null;
    try { return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8")); }
    catch { return null; }
}

function saveCheckpoint(data) {
    // Atomic write: tmp file then rename prevents corrupt checkpoint on kill
    const tmp = CHECKPOINT_FILE + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify(data), "utf8");
    fs.renameSync(tmp, CHECKPOINT_FILE);
}

/////////////////////////////
// PROVIDER + CONTRACT
/////////////////////////////

const provider = new ethers.JsonRpcProvider(RPC_URL);

const contract = new ethers.Contract(
    NFT_CONTRACT,
    ["function balanceOf(address) view returns (uint256)"],
    provider
);

/////////////////////////////
// RETRY WRAPPER
/////////////////////////////

async function withRetry(fn, label) {
    let lastErr;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try { return await fn(); }
        catch (err) {
            lastErr = err;
            const delay = RETRY_DELAY_MS * Math.pow(2, attempt - 1);
            process.stderr.write(
                `\n[retry ${attempt}/${MAX_RETRIES}] ${label} — ${err.message} — waiting ${delay}ms\n`
            );
            await new Promise(r => setTimeout(r, delay));
        }
    }
    throw lastErr;
}

/////////////////////////////
// SAFE LOG FETCH
/////////////////////////////

async function getLogs(fromBlock, toBlock) {
    try {
        return await withRetry(
            () => provider.getLogs({
                address: NFT_CONTRACT,
                fromBlock,
                toBlock,
                topics: [TRANSFER_TOPIC]
            }),
            `getLogs(${fromBlock}-${toBlock})`
        );
    } catch (err) {
        if (fromBlock === toBlock) throw err;
        const mid = Math.floor((fromBlock + toBlock) / 2);
        const [left, right] = await Promise.all([
            getLogs(fromBlock, mid),
            getLogs(mid + 1, toBlock)
        ]);
        return [...left, ...right];
    }
}

/////////////////////////////
// SEMAPHORE — caps concurrent balanceOf calls
/////////////////////////////

function makeSemaphore(limit) {
    let active = 0;
    const queue = [];
    return {
        acquire() {
            return new Promise(resolve => {
                if (active < limit) { active++; resolve(); }
                else queue.push(resolve);
            });
        },
        release() {
            active--;
            if (queue.length > 0) { active++; queue.shift()(); }
        }
    };
}

/////////////////////////////
// CSV WRITER
// Rewrites the full sorted CSV to disk.
// Called after every resolved balance so the file is always current.
/////////////////////////////

async function writeCSV(balances) {
    const holders = Array.from(balances.entries())
        .map(([addr, bal]) => [addr, BigInt(bal)])
        .sort(([addrA, balA], [addrB, balB]) => {
            if (balB !== balA) return Number(balB - balA);
            return addrA.localeCompare(addrB);
        });

    // Write to tmp then rename — readers always see a complete file
    const tmp = OUTPUT_FILE + ".tmp";
    const lines = ["address,balance", ...holders.map(([a, b]) => `${a},${b}`)];
    fs.writeFileSync(tmp, lines.join("\n") + "\n", "utf8");
    fs.renameSync(tmp, OUTPUT_FILE);

    return holders.length;
}

/////////////////////////////
// MAIN
/////////////////////////////

async function main() {

    const latestBlock = process.argv[2]
        ? Number(process.argv[2])
        : await withRetry(() => provider.getBlockNumber(), "getBlockNumber");

    console.log(`Target block: ${latestBlock}\n`);

    // Load checkpoint — valid for any previous run of the same contract
    const cp = loadCheckpoint();

    // Reject checkpoint if it belongs to a different contract
    if (cp && cp.contract && cp.contract !== NFT_CONTRACT.toLowerCase()) {
        console.error(`[checkpoint] Contract mismatch — expected ${NFT_CONTRACT}, got ${cp.contract}. Delete .checkpoint.json manually to reset.`);
        process.exit(1);
    }

    // Restore balances and seen set from checkpoint
    const balances = new Map(cp ? Object.entries(cp.balances) : []);
    const failed   = new Set(cp ? (cp.failed || []) : []);
    // seen = every address we've already dispatched (resolved or failed)
    const seen     = new Set([...balances.keys(), ...failed]);

    // Resume from where the last run ended, or from the creation block
    const resumeFrom = (cp && cp.nextBlock) ? cp.nextBlock : NFT_CREATION_BLOCK;

    if (resumeFrom > NFT_CREATION_BLOCK) {
        console.log(`Resuming from block ${resumeFrom}`);
        console.log(`  ${balances.size} holders already found, ${seen.size} addresses already processed\n`);
    } else {
        console.log(`Starting fresh scan from block ${NFT_CREATION_BLOCK}\n`);
    }

    if (resumeFrom > latestBlock) {
        console.log(`Already up to date (checkpoint nextBlock ${resumeFrom} > target ${latestBlock})`);
        console.log(`Run without a block argument to scan to the latest block, or pass a higher block number.`);
        process.exit(0);
    }

    const sem      = makeSemaphore(CONCURRENCY);
    const inFlight = [];

    /////////////////////////////
    // dispatch — fires balanceOf the instant an address is seen in a log.
    // Updates balances map, rewrites CSV, and saves checkpoint immediately.
    /////////////////////////////

    function dispatch(addr) {
        if (seen.has(addr)) return;
        seen.add(addr);

        const p = (async () => {
            await sem.acquire();
            try {
                const bal = await withRetry(
                    () => contract.balanceOf(addr),
                    `balanceOf(${addr})`
                );

                if (bal > 0n) {
                    balances.set(addr, bal.toString());
                    console.log(`${addr}  =>  ${bal.toString()}`);

                    // Rewrite CSV immediately so it's always up to date
                    await writeCSV(balances);
                }

            } catch (err) {
                failed.add(addr);
                process.stderr.write(`[FAILED] ${addr}: ${err.message}\n`);
            } finally {
                sem.release();
            }
        })();

        inFlight.push(p);
    }

    /////////////////////////////
    // LOG SCAN
    // Saves checkpoint after every batch with the current nextBlock.
    /////////////////////////////

    for (let from = resumeFrom; from <= latestBlock; from += BLOCK_BATCH) {
        const to = Math.min(from + BLOCK_BATCH - 1, latestBlock);
        process.stdout.write(`\r[scan] Logs ${from} → ${to}  (${seen.size} dispatched, ${balances.size} holders)   `);

        const logs = await getLogs(from, to);

        for (const log of logs) {
            const fromAddr = "0x" + log.topics[1].slice(26).toLowerCase();
            const toAddr   = "0x" + log.topics[2].slice(26).toLowerCase();
            if (fromAddr !== ZERO_ADDR) dispatch(fromAddr);
            if (toAddr   !== ZERO_ADDR) dispatch(toAddr);
        }

        // Save scan position after every batch
        saveCheckpoint({
            contract : NFT_CONTRACT.toLowerCase(),
            nextBlock: from + BLOCK_BATCH,
            balances : Object.fromEntries(balances),
            failed   : Array.from(failed)
        });
    }

    console.log(`\n[scan] Complete — ${seen.size} addresses dispatched`);
    console.log(`Waiting for ${inFlight.length} in-flight balanceOf calls to settle...`);

    await Promise.allSettled(inFlight);

    // Final CSV write after all in-flight calls finish
    const totalHolders = await writeCSV(balances);

    // Save final checkpoint — nextBlock = latestBlock+1 so the next run
    // continues from the block after where this run ended
    saveCheckpoint({
        contract : NFT_CONTRACT.toLowerCase(),
        nextBlock: latestBlock + 1,
        balances : Object.fromEntries(balances),
        failed   : Array.from(failed)
    });

    // Write failed addresses if any
    if (failed.size > 0) {
        fs.writeFileSync(
            path.resolve(__dirname, "failed.txt"),
            Array.from(failed).join("\n") + "\n"
        );
    }

    console.log(`\n--- Summary ---`);
    console.log(`Blocks scanned      : ${resumeFrom} → ${latestBlock}`);
    console.log(`Addresses processed : ${seen.size}`);
    console.log(`Holders (bal > 0)   : ${totalHolders}`);
    console.log(`Failed (all retries): ${failed.size}${failed.size > 0 ? "  → see failed.txt" : ""}`);
    console.log(`CSV                 : ${OUTPUT_FILE}`);
    console.log(`Next run resumes at : block ${latestBlock + 1}`);
}

main().catch(err => {
    console.error("\nFatal error:", err.message);
    console.error("Progress saved — re-run to resume.");
    process.exit(1);
});
