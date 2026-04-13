/**
 * Standalone test that replicates the CSV Export node query.
 *
 * Usage:  npx tsx tests/fetch-companies.ts
 */

import { readFileSync } from 'fs';
import { join } from 'path';
import { createClient } from '@supabase/supabase-js';
import { computeBatchSize } from '../nodes/Supabase/utils/supabaseClient';

// Load .env
const envFile = readFileSync(join(process.cwd(), '.env'), 'utf-8');
for (const line of envFile.split(/\r?\n/)) {
	const idx = line.indexOf('=');
	if (idx < 1 || line.startsWith('#')) continue;
	process.env[line.substring(0, idx).trim()] = line.substring(idx + 1).trim();
}

const SUPABASE_HOST = process.env.MARKETING_SUPABASE_HOST || process.env.SUPABASE_HOST!;
const SUPABASE_KEY = process.env.MARKETING_SUPABASE_SECRET_KEY || process.env.SUPABASE_SECRET_KEY!;

if (!SUPABASE_HOST || !SUPABASE_KEY) {
	console.error('Missing SUPABASE_HOST or SUPABASE_SECRET_KEY in .env');
	process.exit(1);
}

console.log('Connecting to:', SUPABASE_HOST);

const supabase = createClient(SUPABASE_HOST, SUPABASE_KEY, {
	auth: { persistSession: false },
	global: {
		fetch: (url: RequestInfo | URL, init?: RequestInit) => {
			const controller = new AbortController();
			const timeout = setTimeout(() => controller.abort(), 120_000); // 120s for testing
			return fetch(url, { ...init, signal: controller.signal }).finally(() =>
				clearTimeout(timeout),
			);
		},
	},
});

// ── Select string (same as returnFields in the node) ──────────────────
const returnFields =
	'id,name,sector(id,german_name),street,postal_code(code,city,country(german_name)),' +
	'fax,website,is_homepage,salutation,title,first_name,last_name,blocklisted,' +
	'is_customer,is_state_owned,is_recruiter,duplicate_of';

// ── Joins (same as the node config) ───────────────────────────────────
const joins = [
	{ table: 'company_email', columns: 'email,main', joinType: 'left' as const },
	{ table: 'company_phone', columns: 'phone,is_fax', joinType: 'left' as const },
	{
		table: 'job_posting',
		columns: 'created_at,job_title(id,article,insertion),post_date',
		joinType: 'inner' as const,
		orderBy: 'created_at',
		orderAscending: true,
		limit: 1,
	},
];

// Build select string with join fragments
let selectWithJoins = returnFields;
for (const j of joins) {
	const cols = j.columns || '*';
	const hint = j.joinType === 'inner' ? `${j.table}!inner` : j.table;
	selectWithJoins += `,${hint}(${cols})`;
}

const batchSize = 10; // Override for testing — find a working size
const joinCount = (selectWithJoins.match(/\(/g) || []).length;

console.log('Join count (parens): ', joinCount);
console.log('Computed batch size: ', batchSize);
console.log('Select:', selectWithJoins, '\n');

// ── Keyset-paginated fetch with all filters from the node ─────────────

async function fetchAll() {
	let lastId: number | string | null = null;
	let totalRows = 0;
	let batchNum = 0;
	const startTime = Date.now();

	while (true) {
		let query = supabase
			.from('company')
			.select(selectWithJoins)
			// Same filters as in the node
			.filter('blocklisted', 'neq', 'true')
			.filter('is_state_owned', 'eq', 'false')
			.filter('is_recruiter', 'eq', 'false')
			.filter('is_customer', 'eq', 'false')
			.filter('last_intend_export', 'lte', '2026-04-07');

		// Join ordering & limiting (job_posting: order by created_at asc, limit 1)
		for (const j of joins) {
			if ('orderBy' in j && j.orderBy) {
				query = query.order(j.orderBy, {
					ascending: 'orderAscending' in j ? j.orderAscending : false,
					referencedTable: j.table,
				});
			}
			if ('limit' in j && j.limit && j.limit > 0) {
				query = query.limit(j.limit, { referencedTable: j.table });
			}
		}

		// Keyset pagination
		if (lastId !== null) {
			query = query.gt('id', lastId);
		}
		query = query.order('id', { ascending: true }).limit(batchSize);

		const batchStart = Date.now();
		const { data, error } = await query;
		const batchMs = Date.now() - batchStart;

		if (error) {
			console.error(`Batch ${batchNum + 1} FAILED (${batchMs}ms):`, error.message);
			break;
		}

		if (!data || data.length === 0) break;

		totalRows += data.length;
		lastId = (data[data.length - 1] as any).id;
		batchNum++;

		console.log(
			`Batch ${batchNum}: ${data.length} rows (${(batchMs / 1000).toFixed(2)}s) — total ${totalRows}`,
		);

		// Print first row of first batch as sample
		if (batchNum === 1) {
			console.log('\nSample row:');
			console.log(JSON.stringify(data[0], null, 2));
			console.log('');
		}

		if (data.length < batchSize) break;
	}

	const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(1);
	console.log(`\nDone: ${totalRows} rows in ${batchNum} batches (${totalElapsed}s)`);
}

fetchAll().catch((err) => {
	console.error('Fatal error:', err);
	process.exit(1);
});
