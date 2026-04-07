import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { ISupabaseCredentials, IRowFilter, IRowSort } from '../types';

/**
 * Creates a Supabase client instance with the provided credentials
 */
export function createSupabaseClient(credentials: ISupabaseCredentials): SupabaseClient {
	const client = createClient(credentials.host, credentials.serviceKey, {
		auth: {
			autoRefreshToken: true,
			persistSession: false,
			detectSessionInUrl: false,
		},
		global: {
			fetch: (url: RequestInfo | URL, init?: RequestInit) => {
				// 60-second timeout prevents requests from hanging indefinitely
				const controller = new AbortController();
				const timeout = setTimeout(() => controller.abort(), 60_000);
				return fetch(url, { ...init, signal: controller.signal }).finally(() => clearTimeout(timeout));
			},
		},
	});

	return client;
}

/**
 * Validates Supabase credentials
 */
export function validateCredentials(credentials: ISupabaseCredentials): void {
	if (!credentials.host) {
		throw new Error('Supabase Project URL is required');
	}

	if (!credentials.serviceKey) {
		throw new Error('Supabase API Key is required');
	}

	// Validate URL format
	try {
		new URL(credentials.host);
	} catch {
		throw new Error('Invalid Supabase Project URL format');
	}

	// Check if URL is a Supabase URL
	if (!credentials.host.includes('supabase.co') && !credentials.host.includes('localhost')) {
		throw new Error('URL must be a valid Supabase project URL');
	}
}

/**
 * Gets the storage URL for a Supabase project
 */
export function getStorageUrl(projectUrl: string): string {
	const url = new URL(projectUrl);
	return `${url.protocol}//${url.host}/storage/v1`;
}

/**
 * Gets the database REST URL for a Supabase project
 */
export function getDatabaseUrl(projectUrl: string): string {
	const url = new URL(projectUrl);
	return `${url.protocol}//${url.host}/rest/v1`;
}

/**
 * Formats error messages from Supabase responses
 */
export function formatSupabaseError(error: any): string {
	if (!error) return 'Unknown error occurred';
	
	if (typeof error === 'string') return error;
	
	if (error.message) return error.message;
	
	if (error.error_description) return error.error_description;
	
	if (error.details) return error.details;
	
	return JSON.stringify(error);
}

/**
 * Checks if an error is a Supabase authentication error
 */
export function isAuthError(error: any): boolean {
	if (!error) return false;
	
	const authErrorCodes = [
		'invalid_api_key',
		'insufficient_privileges',
		'unauthorized',
		'forbidden',
	];
	
	return authErrorCodes.some(code => 
		error.code === code || 
		error.message?.toLowerCase().includes(code) ||
		error.statusCode === 401 ||
		error.statusCode === 403
	);
}

/**
 * Checks if an error is a network/connection error
 */
export function isNetworkError(error: any): boolean {
	if (!error) return false;
	
	const networkErrorPatterns = [
		'network',
		'connection',
		'timeout',
		'unreachable',
		'dns',
		'econnrefused',
		'enotfound',
	];
	
	const errorMessage = error.message?.toLowerCase() || '';
	
	return networkErrorPatterns.some(pattern => errorMessage.includes(pattern));
}

/**
 * Sanitizes column names for SQL operations
 */
export function sanitizeColumnName(columnName: string): string {
	// Remove any characters that aren't alphanumeric, underscore, or hyphen
	return columnName.replace(/[^a-zA-Z0-9_-]/g, '');
}

/**
 * Validates table name format
 */
export function validateTableName(tableName: string): void {
	if (!tableName) {
		throw new Error('Table name is required');
	}
	
	if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(tableName)) {
		throw new Error('Table name must start with a letter and contain only letters, numbers, and underscores');
	}
	
	if (tableName.length > 63) {
		throw new Error('Table name must be 63 characters or less');
	}
}

/**
 * Validates column name format
 */
export function validateColumnName(columnName: string): void {
	if (!columnName) {
		throw new Error('Column name is required');
	}
	
	if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(columnName)) {
		throw new Error('Column name must start with a letter and contain only letters, numbers, and underscores');
	}
	
	if (columnName.length > 63) {
		throw new Error('Column name must be 63 characters or less');
	}
}

/**
 * Converts filter operator to Supabase PostgREST format
 */
export function convertFilterOperator(operator: string): string {
	const operatorMap: Record<string, string> = {
		'eq': 'eq',
		'neq': 'neq',
		'gt': 'gt',
		'gte': 'gte',
		'lt': 'lt',
		'lte': 'lte',
		'like': 'like',
		'ilike': 'ilike',
		'is': 'is',
		'in': 'in',
		'cs': 'cs', // contains
		'cd': 'cd', // contained by
	};

	return operatorMap[operator] || 'eq';
}

/**
 * Normalizes a filter value for PostgREST.
 * The `in` operator requires values wrapped in parentheses: (val1,val2,val3)
 * The `cs` and `cd` operators require values wrapped in braces: {val1,val2,val3}
 */
export function normalizeFilterValue(operator: string, value: string | number | boolean | null | unknown[]): string | number | boolean | null {
	if (operator === 'in') {
		if (Array.isArray(value)) {
			return `(${value.join(',')})`;
		}
		if (typeof value === 'string') {
			const trimmed = value.trim();
			if (!trimmed.startsWith('(')) {
				return `(${trimmed})`;
			}
			return trimmed;
		}
	}
	return value as string | number | boolean | null;
}

/**
 * Supabase uses Kong/nginx which typically allows ~8KB (8192 byte) URLs.
 * We use 7500 chars as a safe ceiling to leave room for encoding overhead.
 */
export const MAX_SAFE_URL_LENGTH = 7500;

/**
 * Absolute minimum chars allowed per IN-filter chunk, even when the rest of
 * the URL eats into the budget.  Prevents degenerate 1-value chunks.
 */
const MIN_IN_CHUNK_CHARS = 500;

/**
 * Computes a per-chunk ID limit based on query complexity.
 *
 * PostgREST translates nested selects like `sector(id,name)` into JOINs.
 * More JOINs means each row is far more expensive, so we scale the limit
 * down as joins increase.  A plain `select=*` (0 joins) is capped only by
 * URL length; 6 joins caps at ~200 IDs.
 *
 * Formula: 2000 / (1 + joinCount * 1.5)
 *   0 joins → 2000 (URL-limited in practice)
 *   1 join  → 800
 *   2 joins → 500
 *   3 joins → 363
 *   6 joins → 200
 */
export function computeMaxIdsPerChunk(selectFields?: string): number {
	const BASE_LIMIT = 2000;

	if (!selectFields || selectFields === '*') return BASE_LIMIT;

	// Count opening parens → each one is a relation/JOIN
	const joinCount = (selectFields.match(/\(/g) || []).length;

	if (joinCount === 0) return BASE_LIMIT;

	return Math.max(100, Math.floor(BASE_LIMIT / (1 + joinCount * 1.5)));
}

/**
 * Computes a pagination batch size based on query complexity.
 *
 * More embedded resources (JOINs) means each batch is heavier for
 * PostgREST to resolve.  With many joins a batch of 1000 rows can
 * easily exceed the 60-second request timeout, so we scale down.
 *
 * Formula: 1000 / (1 + joinCount * 0.5)
 *   0 joins → 1000
 *   1 join  → 666
 *   2 joins → 500
 *   3 joins → 400
 *   5 joins → 285
 *   7 joins → 222
 */
export function computeBatchSize(selectFields?: string): number {
	const BASE = 2000;

	if (!selectFields || selectFields === '*') return BASE;

	const joinCount = (selectFields.match(/\(/g) || []).length;

	if (joinCount === 0) return BASE;

	return Math.max(50, Math.floor(BASE / (1 + joinCount * 0.5)));
}

/**
 * Estimates how many URL characters the non-IN-value parts of a PostgREST
 * query will consume.  The caller subtracts this from MAX_SAFE_URL_LENGTH
 * to obtain the budget available for IN filter values.
 */
export function estimateUrlOverhead(
	hostUrl: string,
	table: string,
	selectFields?: string,
	filters?: IRowFilter[],
	sort?: IRowSort[],
): number {
	// {host}/rest/v1/{table}?
	let overhead = hostUrl.length + '/rest/v1/'.length + table.length + 1;

	// select={fields}&
	if (selectFields) {
		overhead += 'select='.length + selectFields.length + 1;
	}

	if (filters) {
		for (const f of filters) {
			if (f.operator === 'in') {
				// Only the fixed prefix: {col}=in.()&
				overhead += f.column.length + '=in.()&'.length;
			} else {
				const val = normalizeFilterValue(f.operator, f.value);
				// {col}={op}.{value}&
				overhead += f.column.length + 1 + f.operator.length + 1 + String(val).length + 1;
			}
		}
	}

	if (sort) {
		for (const s of sort) {
			// order={col}.asc&  or  order={col}.desc&
			overhead += 'order='.length + s.column.length + 1 + (s.ascending ? 3 : 4) + 1;
		}
	}

	// Pagination params (offset + limit/range) + safety margin
	overhead += 230;

	return overhead;
}

/**
 * Splits an array of values into chunks where each chunk's comma-joined
 * string representation stays within `maxChars` AND each chunk has at most
 * `maxItems` entries (whichever limit is hit first).
 */
export function chunkInFilterValues(
	values: unknown[],
	maxChars: number,
	maxItems: number = Infinity,
): unknown[][] {
	const chunks: unknown[][] = [];
	let currentChunk: unknown[] = [];
	let currentLength = 0;

	for (const value of values) {
		const valueStr = String(value);
		const addedLength = currentChunk.length === 0 ? valueStr.length : valueStr.length + 1;

		if ((currentLength + addedLength > maxChars || currentChunk.length >= maxItems) && currentChunk.length > 0) {
			chunks.push(currentChunk);
			currentChunk = [value];
			currentLength = valueStr.length;
		} else {
			currentChunk.push(value);
			currentLength += addedLength;
		}
	}

	if (currentChunk.length > 0) {
		chunks.push(currentChunk);
	}

	return chunks;
}

/**
 * Expands filters with large IN arrays into multiple filter sets (one per chunk).
 * Each returned filter set can be used for an independent query, and the results
 * should be merged.  Chunks are disjoint, so no deduplication is needed.
 *
 * @param maxInChars  Maximum chars available for IN values in the URL.
 *                    When multiple IN filters need chunking the budget is
 *                    split equally among them.  Falls back to a safe default
 *                    when omitted (useful in non-read contexts like delete).
 * @param maxItems    Maximum number of values per IN chunk (query-complexity
 *                    cap).  Derived from `computeMaxIdsPerChunk()` by callers.
 */
export function expandChunkedFilters(
	filters: IRowFilter[],
	maxInChars?: number,
	maxItems?: number,
): IRowFilter[][] {
	const staticFilters: IRowFilter[] = [];

	// First pass: normalise IN values to arrays and collect those that exceed the budget
	interface InEntry { filter: IRowFilter; values: unknown[]; serializedLength: number }
	const inEntries: InEntry[] = [];

	for (const filter of filters) {
		if (filter.operator === 'in') {
			let values: unknown[];
			if (Array.isArray(filter.value)) {
				values = filter.value;
			} else if (typeof filter.value === 'string') {
				let str = filter.value.trim();
				if (str.startsWith('(') && str.endsWith(')')) str = str.slice(1, -1);
				if (str.startsWith('[') && str.endsWith(']')) str = str.slice(1, -1);
				values = str.split(',').map(v => v.trim()).filter(v => v.length > 0);
			} else {
				staticFilters.push(filter);
				continue;
			}

			const serializedLength = values.map(String).join(',').length;
			inEntries.push({ filter: { ...filter, value: values }, values, serializedLength });
			continue;
		}
		staticFilters.push(filter);
	}

	if (inEntries.length === 0) {
		return [filters];
	}

	// Determine per-IN-filter budget.
	// When maxInChars is provided the budget is split across all IN filters;
	// otherwise fall back to a conservative default per filter.
	const defaultBudget = MAX_SAFE_URL_LENGTH - 500; // rough fallback
	const totalBudget = maxInChars ?? defaultBudget;
	const perFilterBudget = Math.max(MIN_IN_CHUNK_CHARS, Math.floor(totalBudget / inEntries.length));

	const chunkedEntries: { filter: IRowFilter; chunks: unknown[][] }[] = [];

	const itemCap = maxItems ?? Infinity;

	for (const entry of inEntries) {
		if (entry.serializedLength > perFilterBudget || entry.values.length > itemCap) {
			chunkedEntries.push({
				filter: entry.filter,
				chunks: chunkInFilterValues(entry.values, perFilterBudget, itemCap),
			});
		} else {
			// Fits within budget — no chunking needed
			staticFilters.push(entry.filter);
		}
	}

	if (chunkedEntries.length === 0) {
		return [filters.map(f => {
			// Return normalised IN entries (array values) alongside originals
			const inEntry = inEntries.find(e => e.filter.column === f.column && f.operator === 'in');
			return inEntry ? inEntry.filter : f;
		})];
	}

	// Build Cartesian product of all chunk combinations
	let combinations: IRowFilter[][] = [staticFilters];

	for (const { filter, chunks } of chunkedEntries) {
		const newCombinations: IRowFilter[][] = [];
		for (const existing of combinations) {
			for (const chunk of chunks) {
				newCombinations.push([
					...existing,
					{ ...filter, value: chunk },
				]);
			}
		}
		combinations = newCombinations;
	}

	return combinations;
}
