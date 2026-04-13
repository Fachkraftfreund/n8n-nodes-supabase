import { SupabaseClient } from '@supabase/supabase-js';
import { IDataObject, IExecuteFunctions, INodeExecutionData } from 'n8n-workflow';
import {
	IRowFilter,
	IRowSort,
	IColumnDefinition,
	IIndexDefinition,
	DatabaseOperation,
} from '../../types';
import {
	validateTableName,
	validateColumnName,
	convertFilterOperator,
	normalizeFilterValue,
	expandChunkedFilters,
	estimateUrlOverhead,
	computeMaxIdsPerChunk,
	computeBatchSize,
	MAX_SAFE_URL_LENGTH,
	formatSupabaseError,
} from '../../utils/supabaseClient';

/**
 * Execute per-item database operations (read, delete, schema ops, etc.)
 */
export async function executeDatabaseOperation(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	operation: DatabaseOperation,
	itemIndex: number,
	hostUrl: string,
): Promise<INodeExecutionData[]> {
	const returnData: INodeExecutionData[] = [];

	try {
		switch (operation) {
			case 'read': {
				const rows = await handleRead.call(this, supabase, itemIndex, hostUrl);
				for (const r of rows) returnData.push(r);
				break;
			}
			case 'delete':
				returnData.push(...await handleDelete.call(this, supabase, itemIndex, hostUrl));
				break;
			case 'createTable':
				returnData.push(...await handleCreateTable.call(this, supabase, itemIndex));
				break;
			case 'dropTable':
				returnData.push(...await handleDropTable.call(this, supabase, itemIndex));
				break;
			case 'addColumn':
				returnData.push(...await handleAddColumn.call(this, supabase, itemIndex));
				break;
			case 'dropColumn':
				returnData.push(...await handleDropColumn.call(this, supabase, itemIndex));
				break;
			case 'createIndex':
				returnData.push(...await handleCreateIndex.call(this, supabase, itemIndex));
				break;
			case 'dropIndex':
				returnData.push(...await handleDropIndex.call(this, supabase, itemIndex));
				break;
			case 'customQuery':
				returnData.push(...await handleCustomQuery.call(this, supabase, itemIndex));
				break;
			case 'findOrCreate':
				returnData.push(...await handleFindOrCreate.call(this, supabase, itemIndex));
				break;
			case 'updateByQuery':
				returnData.push(...await handleUpdateByQuery.call(this, supabase, itemIndex, hostUrl));
				break;
			case 'count':
				returnData.push(...await handleCount.call(this, supabase, itemIndex, hostUrl));
				break;
			default:
				throw new Error(`Unknown database operation: ${operation}`);
		}
	} catch (error) {
		throw new Error(`Database operation failed: ${formatSupabaseError(error)}`);
	}

	return returnData;
}

/** Maximum rows per bulk API call to limit lock duration and payload size. */
const BULK_BATCH_SIZE = 500;

/** Maximum retry attempts for transient failures (lock timeout, rate limit, etc.) */
const MAX_RETRIES = 3;

/** Base delay in ms for exponential backoff between retries. */
const RETRY_BASE_DELAY_MS = 1000;

/**
 * Deduplicate rows by conflict column(s), keeping the last occurrence.
 * This prevents the Postgres "ON CONFLICT DO UPDATE command cannot affect
 * row a second time" error that occurs when a single batch contains
 * multiple rows with the same conflict key values.
 */
function deduplicateByConflictKeys(rows: IDataObject[], conflictColumns: string): IDataObject[] {
	const keys = conflictColumns.split(',').map((k) => k.trim()).filter(Boolean);
	if (keys.length === 0) return rows;

	const seen = new Map<string, number>();
	for (let i = 0; i < rows.length; i++) {
		const row = rows[i]!;
		// Build composite key using the conflict column values.
		// Normalize to match how Postgres would compare: numeric strings are
		// compared as numbers (e.g. "01067" and "1067" both become 1067 in an
		// integer column), and text is trimmed + lowercased.
		const compositeKey = keys.map((k) => {
			const val = row[k];
			if (val === null || val === undefined) return '\x00null';
			const str = String(val).trim();
			const num = Number(str);
			if (str !== '' && !isNaN(num)) return String(num);
			return str.toLowerCase();
		}).join('\0');
		seen.set(compositeKey, i);
	}
	return Array.from(seen.values()).sort((a, b) => a - b).map((i) => rows[i]!);
}

/**
 * Returns true if the error message looks like a transient / retryable failure.
 */
function isRetryableError(msg: string): boolean {
	const lower = msg.toLowerCase();
	return (
		lower.includes('lock timeout') ||
		lower.includes('canceling statement due to lock') ||
		lower.includes('deadlock') ||
		lower.includes('too many connections') ||
		lower.includes('rate limit') ||
		lower.includes('could not serialize access') ||
		lower.includes('connection terminated') ||
		lower.includes('connection reset') ||
		lower.includes('econnreset') ||
		lower.includes('timeout')
	);
}

/**
 * Execute an async operation with exponential-backoff retry for transient errors.
 */
async function withRetry<T>(fn: () => Promise<T>, label: string): Promise<T> {
	let lastError: Error | undefined;
	for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
		try {
			return await fn();
		} catch (err: any) {
			const msg = err?.message ?? String(err);
			if (attempt < MAX_RETRIES && isRetryableError(msg)) {
				const delay = RETRY_BASE_DELAY_MS * Math.pow(2, attempt);
				console.log(`[Supabase ${label}] transient error (attempt ${attempt + 1}/${MAX_RETRIES + 1}), retrying in ${delay}ms: ${msg}`);
				await new Promise((r) => setTimeout(r, delay));
				lastError = err instanceof Error ? err : new Error(msg);
			} else {
				throw err;
			}
		}
	}
	// Should be unreachable, but just in case:
	throw lastError!;
}

/**
 * Collect row data from all input items
 */
/**
 * Sanitize a string value for Postgres: strips null bytes, unicode escape
 * sequences, and stray backslashes that cause "unsupported Unicode escape
 * sequence" errors.
 */
function sanitizeString(value: string): string {
	return value
		.replace(/\x00/g, '')                // null bytes
		.replace(/\\u[0-9a-fA-F]{4}/g, '')   // unicode escape sequences
		.replace(/\\/g, '');                  // stray backslashes
}

/**
 * Recursively sanitize all string values in an object or array.
 */
function sanitizeRow(obj: IDataObject): IDataObject {
	for (const key of Object.keys(obj)) {
		const val = obj[key];
		if (typeof val === 'string') {
			obj[key] = sanitizeString(val);
		} else if (Array.isArray(val)) {
			for (let i = 0; i < val.length; i++) {
				if (typeof val[i] === 'string') {
					val[i] = sanitizeString(val[i] as string);
				} else if (val[i] && typeof val[i] === 'object') {
					sanitizeRow(val[i] as IDataObject);
				}
			}
		} else if (val && typeof val === 'object') {
			sanitizeRow(val as IDataObject);
		}
	}
	return obj;
}

function collectRowData(
	context: IExecuteFunctions,
	itemCount: number,
): IDataObject[] {
	const rows: IDataObject[] = [];
	for (let i = 0; i < itemCount; i++) {
		const uiMode = context.getNodeParameter('uiMode', i, 'simple') as string;
		let row: IDataObject;

		if (uiMode === 'advanced') {
			const jsonData = context.getNodeParameter('jsonData', i, '{}') as string;
			try {
				row = JSON.parse(jsonData);
			} catch {
				throw new Error(`Invalid JSON data at item ${i}`);
			}
		} else {
			const columns = context.getNodeParameter('columns.column', i, []) as Array<{
				name: string;
				value: any;
			}>;
			row = {};
			for (const column of columns) {
				if (column.name && column.value !== undefined) {
					row[column.name] = column.value;
				}
			}
		}
		rows.push(sanitizeRow(row));
	}
	return rows;
}

/**
 * Execute bulk database operations (single API call for all items)
 */
export async function executeBulkDatabaseOperation(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	operation: DatabaseOperation,
	itemCount: number,
): Promise<INodeExecutionData[]> {
	try {
		switch (operation) {
			case 'create':
				return await handleBulkCreate.call(this, supabase, itemCount);
			case 'upsert':
				return await handleBulkUpsert.call(this, supabase, itemCount);
			case 'update':
				return await handleBulkUpdate.call(this, supabase, itemCount);
			case 'count':
				return await handleBatchCount.call(this, supabase, itemCount);
			default:
				throw new Error(`Operation ${operation} does not support bulk mode`);
		}
	} catch (error) {
		throw new Error(`Database operation failed: ${formatSupabaseError(error)}`);
	}
}

/**
 * Handle bulk CREATE — batched .insert() calls with retry
 */
async function handleBulkCreate(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemCount: number,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', 0) as string;
	validateTableName(table);

	const rows = collectRowData(this, itemCount);
	const returnData: INodeExecutionData[] = [];

	for (let offset = 0; offset < rows.length; offset += BULK_BATCH_SIZE) {
		const batch = rows.slice(offset, offset + BULK_BATCH_SIZE);

		const data = await withRetry(async () => {
			const { data, error } = await supabase
				.from(table)
				.insert(batch)
				.select();
			if (error) throw new Error(formatSupabaseError(error));
			return data;
		}, `CREATE ${table} batch ${Math.floor(offset / BULK_BATCH_SIZE) + 1}`);

		if (Array.isArray(data)) {
			for (const row of data) returnData.push({ json: row });
		}
	}

	return returnData.length > 0
		? returnData
		: [{ json: { data: [], operation: 'create', table } }];
}

/**
 * Handle bulk UPSERT — batched .upsert() calls with retry
 */
async function handleBulkUpsert(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemCount: number,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', 0) as string;
	const onConflict = this.getNodeParameter('onConflict', 0, '') as string;
	const deduplicate = this.getNodeParameter('deduplicateByConflict', 0, false) as boolean;
	validateTableName(table);

	let rows = collectRowData(this, itemCount);

	const options: any = {};
	if (onConflict) options.onConflict = onConflict;

	// Deduplicate rows by conflict key(s) before sending to Supabase
	if (deduplicate && onConflict) {
		const before = rows.length;
		rows = deduplicateByConflictKeys(rows, onConflict);
		if (rows.length < before) {
			console.log(`[Supabase UPSERT ${table}] dedup by "${onConflict}": ${before} → ${rows.length} rows`);
		}
	}

	const returnData: INodeExecutionData[] = [];

	for (let offset = 0; offset < rows.length; offset += BULK_BATCH_SIZE) {
		const batch = rows.slice(offset, offset + BULK_BATCH_SIZE);
		const batchLabel = `UPSERT ${table} batch ${Math.floor(offset / BULK_BATCH_SIZE) + 1}`;

		const data = await withRetry(async () => {
			const { data, error } = await supabase
				.from(table)
				.upsert(batch, options)
				.select();
			if (error) throw new Error(formatSupabaseError(error));
			return data;
		}, batchLabel);

		if (Array.isArray(data)) {
			for (const row of data) returnData.push({ json: row });
		}
	}

	return returnData.length > 0
		? returnData
		: [{ json: { data: [], operation: 'upsert', table } }];
}

/**
 * Handle bulk UPDATE — batched .upsert() calls with onConflict match column and retry
 * Each item must include the match column value in its data.
 */
async function handleBulkUpdate(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemCount: number,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', 0) as string;
	const matchColumn = this.getNodeParameter('matchColumn', 0) as string;
	const returnFields = this.getNodeParameter('returnFields', 0, '*') as string;
	const selectFields = returnFields && returnFields !== '*' ? returnFields : '*';
	validateTableName(table);

	if (!matchColumn) {
		throw new Error('Match Column is required for update operations');
	}

	const deduplicate = this.getNodeParameter('deduplicateByConflict', 0, false) as boolean;
	let rows = collectRowData(this, itemCount);

	// Validate every row includes the match column
	for (let i = 0; i < rows.length; i++) {
		const row = rows[i];
		if (!row || row[matchColumn] === undefined) {
			throw new Error(`Item ${i} is missing the match column "${matchColumn}"`);
		}
	}

	if (deduplicate) {
		const before = rows.length;
		rows = deduplicateByConflictKeys(rows, matchColumn);
		console.log(`[Supabase UPDATE ${table}] dedup by "${matchColumn}": ${before} → ${rows.length} rows`);
	}

	const returnData: INodeExecutionData[] = [];

	for (let offset = 0; offset < rows.length; offset += BULK_BATCH_SIZE) {
		const batch = rows.slice(offset, offset + BULK_BATCH_SIZE);
		const batchLabel = `UPDATE ${table} batch ${Math.floor(offset / BULK_BATCH_SIZE) + 1}`;

		const data = await withRetry(async () => {
			const { data, error } = await supabase
				.from(table)
				.upsert(batch, { onConflict: matchColumn })
				.select(selectFields);
			if (error) throw new Error(formatSupabaseError(error));
			return data;
		}, batchLabel);

		if (Array.isArray(data)) {
			for (const row of data) returnData.push({ json: row });
		}
	}

	return returnData.length > 0
		? returnData
		: [{ json: { data: [], operation: 'update', table } }];
}

/**
 * Extract filters from node parameters (simple or advanced mode) into a unified array.
 */
function getFilters(context: IExecuteFunctions, itemIndex: number): IRowFilter[] {
	const uiMode = context.getNodeParameter('uiMode', itemIndex, 'simple') as string;

	if (uiMode === 'simple') {
		return context.getNodeParameter('filters.filter', itemIndex, []) as IRowFilter[];
	}

	const advancedFilters = context.getNodeParameter('advancedFilters', itemIndex, '') as string;
	if (!advancedFilters) return [];

	try {
		const parsed = JSON.parse(advancedFilters);
		const filters: IRowFilter[] = [];
		for (const [column, condition] of Object.entries(parsed)) {
			if (typeof condition === 'object' && condition !== null) {
				const [operator, value] = Object.entries(condition)[0] as [string, any];
				filters.push({ column, operator: operator as IRowFilter['operator'], value });
			} else {
				filters.push({ column, operator: 'eq', value: condition as any });
			}
		}
		return filters;
	} catch {
		throw new Error('Invalid advanced filters JSON');
	}
}

/**
 * Build a SELECT query with filters and sorting applied.
 */
interface IJoinConfig {
	table: string;
	columns: string;
	joinType: string;
	orderBy?: string;
	orderAscending?: boolean;
	limit?: number;
}

function buildReadQuery(
	supabase: SupabaseClient,
	table: string,
	returnFields: string,
	filters: IRowFilter[],
	sort: IRowSort[],
	options?: { count?: 'exact' },
	joins?: IJoinConfig[],
) {
	const selectFields = returnFields && returnFields !== '*' ? returnFields : '*';
	let query = supabase.from(table).select(selectFields, options);

	for (const filter of filters) {
		const operator = convertFilterOperator(filter.operator);
		query = query.filter(filter.column, operator, normalizeFilterValue(filter.operator, filter.value));
	}

	for (const sortField of sort) {
		query = query.order(sortField.column, { ascending: sortField.ascending });
	}

	// Apply order/limit on joined (embedded) tables
	if (joins) {
		for (const j of joins) {
			if (!j.table) continue;
			if (j.orderBy) {
				query = query.order(j.orderBy, {
					ascending: j.orderAscending ?? false,
					referencedTable: j.table,
				});
			}
			if (j.limit && j.limit > 0) {
				query = query.limit(j.limit, { referencedTable: j.table });
			}
		}
	}

	return query;
}

/**
 * Handle READ operation
 */
async function handleRead(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
	hostUrl: string,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', itemIndex) as string;
	validateTableName(table);

	const returnFields = this.getNodeParameter('returnFields', itemIndex, '*') as string;
	const returnAll = this.getNodeParameter('returnAll', itemIndex, false) as boolean;
	const singleItem = this.getNodeParameter('singleItem', itemIndex, false) as boolean;
	const filters = getFilters(this, itemIndex);
	const sort = this.getNodeParameter('sort.sortField', itemIndex, []) as IRowSort[];

	// Build join fragments from the Joins UI and append to the select string
	const joins = this.getNodeParameter('joins.join', itemIndex, []) as IJoinConfig[];
	let selectWithJoins = returnFields;
	for (const j of joins) {
		if (!j.table) continue;
		const cols = j.columns || '*';
		const hint = j.joinType === 'inner' ? `${j.table}!inner` : j.table;
		selectWithJoins += `,${hint}(${cols})`;
	}

	// Debug: log filter details so we can diagnose chunking issues
	for (const f of filters) {
		const valType = Array.isArray(f.value) ? `array[${(f.value as unknown[]).length}]` : typeof f.value;
		const valLen = typeof f.value === 'string' ? f.value.length : Array.isArray(f.value) ? (f.value as unknown[]).length : 0;
		console.log(`[Supabase READ] item=${itemIndex} filter: ${f.column} ${f.operator} (${valType}, len=${valLen})`);
	}

	const overhead = estimateUrlOverhead(hostUrl, table, selectWithJoins, filters, sort);
	const maxInChars = Math.max(500, MAX_SAFE_URL_LENGTH - overhead);
	const maxItems = computeMaxIdsPerChunk(selectWithJoins);
	const filterChunks = expandChunkedFilters(filters, maxInChars, maxItems);
	const batchSize = computeBatchSize(selectWithJoins);

	console.log(`[Supabase READ] item=${itemIndex} table=${table} returnAll=${returnAll} chunks=${filterChunks.length} maxItems=${maxItems} maxInChars=${maxInChars} batchSize=${batchSize}`);

	const returnData: INodeExecutionData[] = [];

	if (returnAll) {
		// Check if select includes 'id' — if so use fast keyset pagination, otherwise OFFSET
		const selectFields = returnFields && returnFields !== '*' ? returnFields : '*';
		const hasIdColumn = selectFields === '*' || selectFields.split(',').some(f => f.trim() === 'id');

		for (let ci = 0; ci < filterChunks.length; ci++) {
			const chunkFilters = filterChunks[ci]!;
			const inFilter = chunkFilters.find(f => f.operator === 'in');
			const chunkIds = inFilter && Array.isArray(inFilter.value) ? (inFilter.value as unknown[]).length : '?';
			console.log(`[Supabase READ] chunk ${ci + 1}/${filterChunks.length} (${chunkIds} IDs) keyset=${hasIdColumn} - starting...`);
			const chunkStart = Date.now();
			let batchCount = 0;
			let hasMore = true;

			if (hasIdColumn) {
				// Keyset pagination: WHERE id > lastId ORDER BY id — O(1) per batch
				let lastId: number | string | null = null;

				while (hasMore) {
					let query = buildReadQuery(supabase, table, selectWithJoins, chunkFilters, [], undefined, joins);
					if (lastId !== null) {
						query = query.gt('id', lastId);
					}
					query = query.order('id', { ascending: true }).limit(batchSize);

					const { data, error } = await query;
					if (error) {
						console.log(`[Supabase READ] chunk ${ci + 1} batch ${batchCount + 1} FAILED after ${Date.now() - chunkStart}ms: ${formatSupabaseError(error)}`);
						throw new Error(formatSupabaseError(error));
					}

					if (Array.isArray(data) && data.length > 0) {
						for (const row of data) returnData.push({ json: row });
						lastId = data[data.length - 1].id;
						hasMore = data.length === batchSize;
					} else {
						hasMore = false;
					}
					batchCount++;
					if (batchCount % 50 === 0) {
						console.log(`[Supabase READ] chunk ${ci + 1} progress: ${batchCount} batches, ${returnData.length} rows, ${Date.now() - chunkStart}ms`);
					}
				}
			} else {
				// OFFSET pagination fallback for tables without an id column
				let batchOffset = 0;

				while (hasMore) {
					const query = buildReadQuery(supabase, table, selectWithJoins, chunkFilters, sort, undefined, joins);
					const { data, error } = await query.range(batchOffset, batchOffset + batchSize - 1);
					if (error) {
						console.log(`[Supabase READ] chunk ${ci + 1} batch ${batchCount + 1} FAILED after ${Date.now() - chunkStart}ms: ${formatSupabaseError(error)}`);
						throw new Error(formatSupabaseError(error));
					}

					if (Array.isArray(data) && data.length > 0) {
						for (const row of data) returnData.push({ json: row });
						hasMore = data.length === batchSize;
					} else {
						hasMore = false;
					}
					batchOffset += batchSize;
					batchCount++;
					if (batchCount % 50 === 0) {
						console.log(`[Supabase READ] chunk ${ci + 1} progress: ${batchCount} batches, ${returnData.length} rows, ${Date.now() - chunkStart}ms`);
					}
				}
			}

			console.log(`[Supabase READ] chunk ${ci + 1}/${filterChunks.length} done in ${Date.now() - chunkStart}ms — ${batchCount} batches, ${returnData.length} total rows`);
		}

		// Apply user's sort when keyset pagination was used (it overrides user sort with id ordering)
		if (hasIdColumn && sort.length > 0) {
			returnData.sort((a, b) => {
				for (const s of sort) {
					const aVal = a.json[s.column] ?? null;
					const bVal = b.json[s.column] ?? null;
					if (aVal === bVal) continue;
					if (aVal === null) return 1;
					if (bVal === null) return -1;
					if (aVal < bVal) return s.ascending ? -1 : 1;
					if (aVal > bVal) return s.ascending ? 1 : -1;
				}
				return 0;
			});
		}
	} else {
		const limit = this.getNodeParameter('limit', itemIndex, 100) as number;
		const userOffset = this.getNodeParameter('offset', itemIndex, 0) as number;
		const isMultiChunk = filterChunks.length > 1;

		for (const chunkFilters of filterChunks) {
			let query = buildReadQuery(supabase, table, selectWithJoins, chunkFilters, sort, undefined, joins);

			if (isMultiChunk) {
				// Multiple chunks: fetch enough per chunk so we can apply global offset+limit after merging
				query = query.limit(userOffset + limit);
			} else {
				// Single chunk: apply offset and limit directly to the query
				if (userOffset > 0) {
					query = query.range(userOffset, userOffset + limit - 1);
				} else {
					query = query.limit(limit);
				}
			}

			const { data, error } = await query;

			if (error) {
				throw new Error(formatSupabaseError(error));
			}

			if (Array.isArray(data)) {
				for (const row of data) {
					returnData.push({ json: row });
				}
			}
		}

		// For multiple chunks, apply global offset and limit to the combined results
		if (isMultiChunk && (userOffset > 0 || returnData.length > limit)) {
			const sliced = returnData.slice(userOffset, userOffset + limit);
			returnData.length = 0;
			for (const r of sliced) returnData.push(r);
		}
	}

	// If no data found, return empty result with metadata
	if (returnData.length === 0) {
		returnData.push({
			json: {
				data: [],
				count: 0,
				operation: 'read',
				table,
				message: 'No records found',
			},
		});
	}

	// Wrap all rows into a single n8n item if requested
	if (singleItem && returnData.length > 0) {
		const count = returnData.length;
		// Reuse .json references — no copy needed
		const allRows = new Array(count);
		for (let i = 0; i < count; i++) allRows[i] = returnData[i]!.json;
		// Release the item wrappers so GC can reclaim them
		returnData.length = 0;
		return [{ json: { data: allRows, count } }];
	}

	return returnData;
}

/**
 * Handle DELETE operation
 */
async function handleDelete(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
	hostUrl: string,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', itemIndex) as string;

	validateTableName(table);

	// Apply filters to determine which rows to delete
	const filters = this.getNodeParameter('filters.filter', itemIndex, []) as IRowFilter[];
	if (filters.length === 0) {
		throw new Error('At least one filter is required for delete operations to prevent accidental data loss');
	}

	const overhead = estimateUrlOverhead(hostUrl, table, undefined, filters);
	const maxInChars = Math.max(500, MAX_SAFE_URL_LENGTH - overhead);
	const filterChunks = expandChunkedFilters(filters, maxInChars);
	const allDeleted: any[] = [];

	for (const chunkFilters of filterChunks) {
		let query = supabase.from(table).delete();

		for (const filter of chunkFilters) {
			const operator = convertFilterOperator(filter.operator);
			query = query.filter(filter.column, operator, normalizeFilterValue(filter.operator, filter.value));
		}

		const { data, error } = await query.select();

		if (error) {
			throw new Error(formatSupabaseError(error));
		}

		if (Array.isArray(data)) {
			allDeleted.push(...data);
		}
	}

	return [{ json: { data: allDeleted, operation: 'delete', table, deleted: allDeleted.length } }];
}

/**
 * Handle CREATE TABLE operation
 */
async function handleCreateTable(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const tableName = this.getNodeParameter('tableName', itemIndex) as string;
	const columns = this.getNodeParameter('columnDefinitions.column', itemIndex, []) as IColumnDefinition[];
	
	validateTableName(tableName);

	if (columns.length === 0) {
		throw new Error('At least one column definition is required');
	}

	// Build CREATE TABLE SQL
	let sql = `CREATE TABLE "${tableName}" (`;
	const columnDefs: string[] = [];

	for (const column of columns) {
		validateColumnName(column.name);
		
		let columnDef = `"${column.name}" ${column.type}`;
		
		if (!column.nullable) {
			columnDef += ' NOT NULL';
		}
		
		if (column.defaultValue) {
			columnDef += ` DEFAULT ${column.defaultValue}`;
		}
		
		if (column.primaryKey) {
			columnDef += ' PRIMARY KEY';
		}
		
		if (column.unique && !column.primaryKey) {
			columnDef += ' UNIQUE';
		}
		
		columnDefs.push(columnDef);
	}

	sql += columnDefs.join(', ') + ')';

	const { error } = await supabase.rpc('exec_sql', { sql });

	if (error) {
		throw new Error(formatSupabaseError(error));
	}

	return [{ json: { operation: 'createTable', tableName, sql, success: true } }];
}

/**
 * Handle DROP TABLE operation
 */
async function handleDropTable(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const tableName = this.getNodeParameter('tableName', itemIndex) as string;
	const cascade = this.getNodeParameter('cascade', itemIndex, false) as boolean;
	
	validateTableName(tableName);

	const sql = `DROP TABLE "${tableName}"${cascade ? ' CASCADE' : ''}`;

	const { error } = await supabase.rpc('exec_sql', { sql });

	if (error) {
		throw new Error(formatSupabaseError(error));
	}

	return [{ json: { operation: 'dropTable', tableName, sql, success: true } }];
}

/**
 * Handle ADD COLUMN operation
 */
async function handleAddColumn(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const tableName = this.getNodeParameter('tableName', itemIndex) as string;
	const columnDefinition = this.getNodeParameter('columnDefinition', itemIndex) as IColumnDefinition;
	
	validateTableName(tableName);
	validateColumnName(columnDefinition.name);

	let sql = `ALTER TABLE "${tableName}" ADD COLUMN "${columnDefinition.name}" ${columnDefinition.type}`;
	
	if (!columnDefinition.nullable) {
		sql += ' NOT NULL';
	}
	
	if (columnDefinition.defaultValue) {
		sql += ` DEFAULT ${columnDefinition.defaultValue}`;
	}

	const { error } = await supabase.rpc('exec_sql', { sql });

	if (error) {
		throw new Error(formatSupabaseError(error));
	}

	return [{ json: { operation: 'addColumn', tableName, columnName: columnDefinition.name, sql, success: true } }];
}

/**
 * Handle DROP COLUMN operation
 */
async function handleDropColumn(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const tableName = this.getNodeParameter('tableName', itemIndex) as string;
	const columnName = this.getNodeParameter('columnName', itemIndex) as string;
	const cascade = this.getNodeParameter('cascade', itemIndex, false) as boolean;
	
	validateTableName(tableName);
	validateColumnName(columnName);

	const sql = `ALTER TABLE "${tableName}" DROP COLUMN "${columnName}"${cascade ? ' CASCADE' : ''}`;

	const { error } = await supabase.rpc('exec_sql', { sql });

	if (error) {
		throw new Error(formatSupabaseError(error));
	}

	return [{ json: { operation: 'dropColumn', tableName, columnName, sql, success: true } }];
}

/**
 * Handle CREATE INDEX operation
 */
async function handleCreateIndex(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const tableName = this.getNodeParameter('tableName', itemIndex) as string;
	const indexDefinition = this.getNodeParameter('indexDefinition', itemIndex) as IIndexDefinition;
	
	validateTableName(tableName);

	if (!indexDefinition.name || indexDefinition.columns.length === 0) {
		throw new Error('Index name and columns are required');
	}

	const uniqueKeyword = indexDefinition.unique ? 'UNIQUE ' : '';
	const method = indexDefinition.method ? ` USING ${indexDefinition.method}` : '';
	const columnList = indexDefinition.columns.map(col => `"${col}"`).join(', ');

	const sql = `CREATE ${uniqueKeyword}INDEX "${indexDefinition.name}" ON "${tableName}"${method} (${columnList})`;

	const { error } = await supabase.rpc('exec_sql', { sql });

	if (error) {
		throw new Error(formatSupabaseError(error));
	}

	return [{ json: { operation: 'createIndex', tableName, indexName: indexDefinition.name, sql, success: true } }];
}

/**
 * Handle DROP INDEX operation
 */
async function handleDropIndex(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const indexName = this.getNodeParameter('indexName', itemIndex) as string;
	const cascade = this.getNodeParameter('cascade', itemIndex, false) as boolean;

	if (!indexName) {
		throw new Error('Index name is required');
	}

	const sql = `DROP INDEX "${indexName}"${cascade ? ' CASCADE' : ''}`;

	const { error } = await supabase.rpc('exec_sql', { sql });

	if (error) {
		throw new Error(formatSupabaseError(error));
	}

	return [{ json: { operation: 'dropIndex', indexName, sql, success: true } }];
}

/**
 * Handle CUSTOM QUERY operation
 */
async function handleCustomQuery(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const customSql = this.getNodeParameter('customSql', itemIndex) as string;

	if (!customSql?.trim()) {
		throw new Error('SQL query is required');
	}

	// For queries that return rows: SELECT, CTEs (WITH), EXPLAIN, TABLE
	const returnsRows = /^\s*(select|with|explain|table)\b/i.test(customSql);
	if (returnsRows) {
		const { data, error } = await supabase.rpc('exec_sql_select', { sql: customSql });

		if (error) {
			throw new Error(formatSupabaseError(error));
		}

		const returnData: INodeExecutionData[] = [];
		if (Array.isArray(data)) {
			for (const row of data) {
				returnData.push({ json: row });
			}
		}

		if (returnData.length === 0) {
			returnData.push({
				json: {
					data: [],
					operation: 'customQuery',
					sql: customSql,
					message: 'Query executed successfully, no results returned',
				},
			});
		}

		return returnData;
	} else {
		// For other queries (INSERT, UPDATE, DELETE, DDL), use exec_sql
		const { data, error } = await supabase.rpc('exec_sql', { sql: customSql });

		if (error) {
			throw new Error(formatSupabaseError(error));
		}

		return [{ json: { operation: 'customQuery', sql: customSql, result: data, success: true } }];
	}
}

/**
 * Handle FIND OR CREATE operation
 */
async function handleFindOrCreate(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', itemIndex) as string;
	validateTableName(table);

	const matchCols = this.getNodeParameter('matchColumns.column', itemIndex, []) as Array<{ name: string; value: any }>;
	if (matchCols.length === 0) {
		throw new Error('At least one Match Column is required for Find or Create');
	}

	// Build SELECT query from match columns
	let query = supabase.from(table).select('*');
	for (const col of matchCols) {
		if (!col.name) continue;
		validateColumnName(col.name);
		query = query.eq(col.name, col.value);
	}
	query = query.limit(1);

	const { data: existing, error: readError } = await query;
	if (readError) throw new Error(formatSupabaseError(readError));

	if (existing && existing.length > 0) {
		return [{ json: { ...existing[0], _found: true, _created: false } }];
	}

	// Not found — build insert data from match + additional columns
	const additionalCols = this.getNodeParameter('additionalColumns.column', itemIndex, []) as Array<{ name: string; value: any }>;
	const dataToInsert: IDataObject = {};
	for (const col of [...matchCols, ...additionalCols]) {
		if (col.name) dataToInsert[col.name] = col.value;
	}

	const { data: created, error: insertError } = await supabase
		.from(table)
		.insert(dataToInsert)
		.select()
		.single();

	if (insertError) throw new Error(formatSupabaseError(insertError));

	return [{ json: { ...created, _found: false, _created: true } }];
}

/**
 * Handle UPDATE BY QUERY operation — update all rows matching filter conditions.
 * Uses Supabase .update() with filters (e.g. WHERE id IN (...)).
 * Supports URL chunking for large IN filter values.
 */
async function handleUpdateByQuery(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
	hostUrl: string,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', itemIndex) as string;
	const returnFields = this.getNodeParameter('returnFields', itemIndex, '*') as string;
	const selectFields = returnFields && returnFields !== '*' ? returnFields : '*';
	validateTableName(table);

	// Collect the values to set
	const uiMode = this.getNodeParameter('uiMode', itemIndex, 'simple') as string;
	let updateData: IDataObject;

	if (uiMode === 'advanced') {
		const jsonData = this.getNodeParameter('jsonData', itemIndex, '{}') as string;
		try {
			updateData = JSON.parse(jsonData);
		} catch {
			throw new Error('Invalid JSON data for update values');
		}
	} else {
		const columns = this.getNodeParameter('columns.column', itemIndex, []) as Array<{
			name: string;
			value: any;
		}>;
		updateData = {};
		for (const column of columns) {
			if (column.name && column.value !== undefined) {
				updateData[column.name] = column.value;
			}
		}
	}

	if (Object.keys(updateData).length === 0) {
		throw new Error('At least one column value is required for update');
	}

	sanitizeRow(updateData);

	// Get filters
	const filters = getFilters(this, itemIndex);
	if (filters.length === 0) {
		throw new Error('At least one filter is required for Update by Query to prevent accidental full-table updates');
	}

	// Chunk large IN filters to stay within URL length limits
	const overhead = estimateUrlOverhead(hostUrl, table, undefined, filters);
	const maxInChars = Math.max(500, MAX_SAFE_URL_LENGTH - overhead);
	const filterChunks = expandChunkedFilters(filters, maxInChars);

	const returnData: INodeExecutionData[] = [];

	for (const chunkFilters of filterChunks) {
		const data = await withRetry(async () => {
			let query = supabase.from(table).update(updateData);

			for (const filter of chunkFilters) {
				const operator = convertFilterOperator(filter.operator);
				query = query.filter(filter.column, operator, normalizeFilterValue(filter.operator, filter.value));
			}

			const { data, error } = await query.select(selectFields);
			if (error) throw new Error(formatSupabaseError(error));
			return data;
		}, `UPDATE_BY_QUERY ${table}`);

		if (Array.isArray(data)) {
			for (const row of data) returnData.push({ json: row });
		}
	}

	if (returnData.length === 0) {
		return [{ json: { data: [], count: 0, operation: 'updateByQuery', table, message: 'No rows matched the filter conditions' } }];
	}

	return returnData;
}

/**
 * Handle COUNT operation — returns the number of rows matching filters
 * without fetching actual row data (uses head: true).
 */
async function handleCount(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
	hostUrl: string,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', itemIndex) as string;
	validateTableName(table);

	const filters = getFilters(this, itemIndex);

	// Build join fragments from the Joins UI (inner join filters related rows)
	const joins = this.getNodeParameter('joins.join', itemIndex, []) as IJoinConfig[];
	let selectWithJoins = '*';
	for (const j of joins) {
		if (!j.table) continue;
		const cols = j.columns || '*';
		const hint = j.joinType === 'inner' ? `${j.table}!inner` : j.table;
		selectWithJoins += `,${hint}(${cols})`;
	}

	// Chunk large IN filters to stay within URL length limits
	const overhead = estimateUrlOverhead(hostUrl, table, selectWithJoins, filters);
	const maxInChars = Math.max(500, MAX_SAFE_URL_LENGTH - overhead);
	const maxItems = computeMaxIdsPerChunk(selectWithJoins);
	const filterChunks = expandChunkedFilters(filters, maxInChars, maxItems);

	let totalCount = 0;

	for (const chunkFilters of filterChunks) {
		let query = supabase.from(table).select(selectWithJoins, { count: 'exact', head: true });

		for (const filter of chunkFilters) {
			const operator = convertFilterOperator(filter.operator);
			query = query.filter(filter.column, operator, normalizeFilterValue(filter.operator, filter.value));
		}

		const { count, error } = await query;
		if (error) throw new Error(formatSupabaseError(error));
		totalCount += count ?? 0;
	}

	return [{ json: { count: totalCount, table } }];
}

/**
 * Handle BATCH COUNT — collects groupBy values from all input items,
 * runs a single GROUP BY SQL query via exec_sql_select, and returns
 * one item per group with its count.
 */
async function handleBatchCount(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemCount: number,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', 0) as string;
	const groupByColumn = this.getNodeParameter('groupByColumn', 0) as string;
	validateTableName(table);
	validateColumnName(groupByColumn);

	// Collect all filters that are constant across items (non-expression filters from item 0)
	const baseFilters = getFilters(this, 0);

	// Collect unique groupBy values from all input items' filters
	const groupValues = new Set<string>();
	for (let i = 0; i < itemCount; i++) {
		const itemFilters = getFilters(this, i);
		for (const f of itemFilters) {
			if (f.column === groupByColumn) {
				groupValues.add(String(f.value));
			}
		}
	}

	// Build WHERE clauses from base filters (excluding the groupBy column)
	const whereClauses: string[] = [];
	const staticFilters = baseFilters.filter(f => f.column !== groupByColumn);
	for (const f of staticFilters) {
		const col = `"${f.column}"`;
		switch (f.operator) {
			case 'eq': whereClauses.push(`${col} = '${String(f.value).replace(/'/g, "''")}'`); break;
			case 'neq': whereClauses.push(`${col} != '${String(f.value).replace(/'/g, "''")}'`); break;
			case 'gt': whereClauses.push(`${col} > '${String(f.value).replace(/'/g, "''")}'`); break;
			case 'gte': whereClauses.push(`${col} >= '${String(f.value).replace(/'/g, "''")}'`); break;
			case 'lt': whereClauses.push(`${col} < '${String(f.value).replace(/'/g, "''")}'`); break;
			case 'lte': whereClauses.push(`${col} <= '${String(f.value).replace(/'/g, "''")}'`); break;
			case 'is': whereClauses.push(`${col} IS ${f.value}`); break;
			case 'like': whereClauses.push(`${col} LIKE '${String(f.value).replace(/'/g, "''")}'`); break;
			case 'ilike': whereClauses.push(`${col} ILIKE '${String(f.value).replace(/'/g, "''")}'`); break;
			case 'in': {
				const vals = Array.isArray(f.value) ? f.value : String(f.value).split(',');
				const escaped = vals.map(v => `'${String(v).trim().replace(/'/g, "''")}'`).join(',');
				whereClauses.push(`${col} IN (${escaped})`);
				break;
			}
			default: whereClauses.push(`${col} = '${String(f.value).replace(/'/g, "''")}'`);
		}
	}

	// Add IN clause for the collected groupBy values
	if (groupValues.size > 0) {
		const escaped = Array.from(groupValues).map(v => `'${v.replace(/'/g, "''")}'`).join(',');
		whereClauses.push(`"${groupByColumn}" IN (${escaped})`);
	}

	// Build join clauses from Joins UI
	const joins = this.getNodeParameter('joins.join', 0, []) as IJoinConfig[];
	const joinClauses: string[] = [];
	for (const j of joins) {
		if (!j.table) continue;
		validateTableName(j.table);
		const joinType = j.joinType === 'inner' ? 'INNER JOIN' : 'LEFT JOIN';
		joinClauses.push(`${joinType} "${j.table}" ON "${j.table}"."${table.replace(/s$/, '')}_id" = "${table}"."id"`);
	}

	const whereStr = whereClauses.length > 0 ? ` WHERE ${whereClauses.join(' AND ')}` : '';
	const joinStr = joinClauses.length > 0 ? ` ${joinClauses.join(' ')}` : '';

	const sql = `SELECT "${groupByColumn}", COUNT(*) as count FROM "${table}"${joinStr}${whereStr} GROUP BY "${groupByColumn}" ORDER BY count DESC`;

	console.log(`[Supabase BATCH COUNT] sql: ${sql}`);

	const { data, error } = await supabase.rpc('exec_sql_select', { sql });
	if (error) throw new Error(formatSupabaseError(error));

	if (!Array.isArray(data) || data.length === 0) {
		return [{ json: { table, groupByColumn, counts: [], message: 'No rows matched' } }];
	}

	return data.map((row: any) => ({
		json: {
			[groupByColumn]: row[groupByColumn],
			count: Number(row.count),
			table,
		},
	}));
}
