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

const OUTPUT_FILE        = path.resolve(__dirname, "holders.csv");
const CHECKPOINT_FILE    = path.resolve(__dirname, ".checkpoint.json");

const TRANSFER_TOPIC     = ethers.id("Transfer(address,address,uint256)");
const ZERO_ADDR          = "0x0000000000000000000000000000000000000000";

/////////////////////////////
// CHECKPOINT SCHEMA
//
// {
//   latestBlock  : number   — the target block this run is scanning to
//   phase        : 1 | 2   — which phase was in progress when saved
//
//   // Phase 1 fields
//   nextBlock    : number   — next block batch to fetch (updated after every batch)
//   addresses    : string[] — all unique addresses collected so far
//
//   // Phase 2 fields (written once Phase 1 completes)
//   addrList     : string[] — full deduplicated address list to query
//   nextAddrIdx  : number   — next index in addrList to query
//   holders      : [string, string][] — [address, balanceAsString] collected so far
// }
/////////////////////////////

function loadCheckpoint() {
    if (!fs.existsSync(CHECKPOINT_FILE)) return null;
    try {
        return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8"));
    } catch {
        return null;
    }
}

function saveCheckpoint(data) {
    // Write to a temp file then rename — prevents a corrupt checkpoint if the
    // process is killed mid-write
    const tmp = CHECKPOINT_FILE + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify(data), "utf8");
    fs.renameSync(tmp, CHECKPOINT_FILE);
}

function deleteCheckpoint() {
    if (fs.existsSync(CHECKPOINT_FILE))     fs.unlinkSync(CHECKPOINT_FILE);
    if (fs.existsSync(CHECKPOINT_FILE + ".tmp")) fs.unlinkSync(CHECKPOINT_FILE + ".tmp");
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
// Retries any async fn up to MAX_RETRIES with exponential backoff.
/////////////////////////////

async function withRetry(fn, label) {
    let lastErr;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            return await fn();
        } catch (err) {
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
// Retries first, then splits range on persistent failure.
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
// PHASE 1 — COLLECT ADDRESSES
// Scans Transfer logs to build the complete set of addresses that ever
// held this NFT. Saves checkpoint after every batch so it can resume.
/////////////////////////////

async function phase1(latestBlock, cp) {
    // Only reuse saved addresses if the checkpoint is for the same target block
    const addresses = new Set(
        (cp && cp.latestBlock === latestBlock && cp.phase === 1 && cp.addresses)
            ? cp.addresses
            : []
    );

    const resumeFrom = (cp && cp.latestBlock === latestBlock && cp.phase === 1)
        ? cp.nextBlock
        : NFT_CREATION_BLOCK;

    if (resumeFrom > NFT_CREATION_BLOCK) {
        console.log(`[Phase 1] Resuming from block ${resumeFrom} (${addresses.size} addresses already collected)`);
    } else {
        console.log(`[Phase 1] Scanning from block ${NFT_CREATION_BLOCK} → ${latestBlock}`);
    }

    for (let from = resumeFrom; from <= latestBlock; from += BLOCK_BATCH) {
        const to = Math.min(from + BLOCK_BATCH - 1, latestBlock);
        process.stdout.write(`\r[Phase 1] Logs ${from} → ${to}  (${addresses.size} addresses)   `);

        const logs = await getLogs(from, to);

        for (const log of logs) {
            const fromAddr = "0x" + log.topics[1].slice(26).toLowerCase();
            const toAddr   = "0x" + log.topics[2].slice(26).toLowerCase();
            if (fromAddr !== ZERO_ADDR) addresses.add(fromAddr);
            if (toAddr   !== ZERO_ADDR) addresses.add(toAddr);
        }

        // Save after every completed batch — nextBlock is the start of the NEXT batch
        saveCheckpoint({
            latestBlock,
            phase    : 1,
            nextBlock: from + BLOCK_BATCH,
            addresses: Array.from(addresses)
        });
    }

    console.log(`\n[Phase 1] Complete — ${addresses.size} unique addresses`);
    return Array.from(addresses);
}

/////////////////////////////
// PHASE 2 — QUERY balanceOf
// Calls balanceOf for every address. Prints results immediately.
// Saves checkpoint after every address so it can resume mid-query.
/////////////////////////////

async function phase2(latestBlock, addrList, cp) {
    // Resume from saved holders + next index if checkpoint is for Phase 2
    // of the same target block and same address list
    const canResume = (
        cp &&
        cp.latestBlock  === latestBlock &&
        cp.phase        === 2 &&
        cp.addrList     &&
        cp.addrList.length === addrList.length
    );

    let holders     = canResume ? cp.holders.map(([a, b]) => [a.toLowerCase(), BigInt(b)]) : [];
    let nextAddrIdx = canResume ? cp.nextAddrIdx : 0;
    const failed    = [];

    if (canResume) {
        console.log(`[Phase 2] Resuming from address index ${nextAddrIdx}/${addrList.length} (${holders.length} holders found so far)`);
    } else {
        console.log(`[Phase 2] Querying balanceOf for ${addrList.length} addresses with ${CONCURRENCY} workers`);
    }

    // Shared mutable state — safe in JS (single-threaded event loop)
    let idx     = nextAddrIdx;
    const seen  = new Set(holders.map(([a]) => a.toLowerCase())); // don't re-query already resolved addresses
    let pending = 0; // how many queries are in-flight right now

    // Checkpoint flush: save current state periodically
    // We batch checkpoint writes — every 50 completions — to avoid hammering disk
    let completedSinceLastSave = 0;
    const CHECKPOINT_EVERY = 50;

    function flushCheckpoint() {
        saveCheckpoint({
            latestBlock,
            phase       : 2,
            addrList,
            nextAddrIdx : idx,
            holders     : holders.map(([a, b]) => [a, b.toString()])
        });
        completedSinceLastSave = 0;
    }

    async function worker() {
        while (true) {
            const i = idx++;
            if (i >= addrList.length) break;

            const addr = addrList[i].toLowerCase();

            if (seen.has(addr)) continue;
            seen.add(addr);

            try {
                const bal = await withRetry(
                    () => contract.balanceOf(addr),
                    `balanceOf(${addr})`
                );

                if (bal > 0n) {
                    console.log(`${addr}  =>  ${bal.toString()}`);
                    holders.push([addr, bal]);
                }

            } catch (err) {
                failed.push(addr);
                process.stderr.write(`[FAILED] ${addr}: ${err.message}\n`);
            }

            completedSinceLastSave++;
            if (completedSinceLastSave >= CHECKPOINT_EVERY) {
                flushCheckpoint();
            }
        }
    }

    await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));

    // Final checkpoint flush for any remainder
    flushCheckpoint();

    return { holders, failed };
}

/////////////////////////////
// MAIN
/////////////////////////////

async function main() {

    const latestBlock = process.argv[2]
        ? Number(process.argv[2])
        : await withRetry(() => provider.getBlockNumber(), "getBlockNumber");

    console.log(`Target block: ${latestBlock}\n`);

    // Load checkpoint once — passed explicitly to each phase
    let cp = loadCheckpoint();

    // If checkpoint is for a different target block, discard it
    if (cp && cp.latestBlock !== latestBlock) {
        console.log(`[checkpoint] Discarding stale checkpoint (was for block ${cp.latestBlock}, now targeting ${latestBlock})`);
        deleteCheckpoint();
        cp = null;
    }

    /////////////////////////////
    // PHASE 1
    /////////////////////////////

    let addrList;

    // Skip Phase 1 entirely if checkpoint already has a complete Phase 2 addrList
    if (cp && cp.phase === 2 && cp.addrList) {
        // Re-deduplicate after lowercasing — guards against any mixed-case entries
        addrList = Array.from(new Set(cp.addrList.map(a => a.toLowerCase())));
        console.log(`[Phase 1] Already complete — ${addrList.length} addresses loaded from checkpoint`);
    } else {
        addrList = await phase1(latestBlock, cp);
        // Reload checkpoint after phase1 so phase2 gets the updated state
        cp = loadCheckpoint();
    }

    /////////////////////////////
    // PHASE 2
    /////////////////////////////

    // Transition checkpoint from phase 1 → phase 2 before starting queries
    // This ensures phase 2 always has the full addrList saved
    if (!cp || cp.phase !== 2) {
        saveCheckpoint({
            latestBlock,
            phase       : 2,
            addrList,
            nextAddrIdx : 0,
            holders     : []
        });
        cp = loadCheckpoint();
    }

    const { holders, failed } = await phase2(latestBlock, addrList, cp);

    /////////////////////////////
    // PHASE 3 — SORT + WRITE CSV
    // Highest balance first, alphabetical on ties.
    // Written entirely after sort — one row per address, no duplicates.
    /////////////////////////////

    holders.sort(([addrA, balA], [addrB, balB]) => {
        if (balB !== balA) return Number(balB - balA);
        return addrA.localeCompare(addrB);
    });

    const writeStream = fs.createWriteStream(OUTPUT_FILE);

    function writeLine(line) {
        return new Promise(resolve => {
            const ok = writeStream.write(line + "\n");
            if (ok) resolve();
            else writeStream.once("drain", resolve);
        });
    }

    await writeLine("address,balance");
    for (const [addr, bal] of holders) {
        await writeLine(`${addr},${bal}`);
    }
    await new Promise(resolve => writeStream.end(resolve));

    /////////////////////////////
    // PHASE 4 — CLEANUP + SUMMARY
    /////////////////////////////

    deleteCheckpoint();

    if (failed.length > 0) {
        fs.writeFileSync(path.resolve(__dirname, "failed.txt"), failed.join("\n") + "\n");
    }

    console.log(`\n--- Summary ---`);
    console.log(`Total addresses scanned : ${addrList.length}`);
    console.log(`Holders (balance > 0)   : ${holders.length}`);
    console.log(`Failed (all retries)    : ${failed.length}${failed.length > 0 ? "  → see failed.txt" : ""}`);
    console.log(`CSV saved               : ${OUTPUT_FILE}`);
}

main().catch(err => {
    console.error("\nFatal error:", err.message);
    console.error("Progress has been saved — re-run to resume.");
    process.exit(1);
});
