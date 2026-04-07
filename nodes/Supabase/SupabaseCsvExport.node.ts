import {
	IExecuteFunctions,
	ILoadOptionsFunctions,
	INodeExecutionData,
	INodePropertyOptions,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';
import { SupabaseClient } from '@supabase/supabase-js';
import { createWriteStream, createReadStream, unlinkSync, WriteStream } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { randomBytes } from 'crypto';

import { createSupabaseClient, validateCredentials } from './utils/supabaseClient';
import {
	validateTableName,
	convertFilterOperator,
	normalizeFilterValue,
	expandChunkedFilters,
	estimateUrlOverhead,
	computeMaxIdsPerChunk,
	computeBatchSize,
	MAX_SAFE_URL_LENGTH,
	formatSupabaseError,
} from './utils/supabaseClient';
import { ISupabaseCredentials, IRowFilter, IRowSort } from './types';

// ── CSV helpers ────────────────────────────────────────────────────────

interface CsvOptions {
	delimiter: string;
	quoteChar: string;
	includeHeaders: boolean;
	fileName: string;
}

function escapeRegExp(s: string): string {
	return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function escapeCsvField(value: unknown, delimiter: string, quoteChar: string): string {
	if (value === null || value === undefined) return '';
	const str = typeof value === 'object' ? JSON.stringify(value) : String(value);
	if (
		str.includes(delimiter) ||
		str.includes(quoteChar) ||
		str.includes('\n') ||
		str.includes('\r')
	) {
		const escaped = quoteChar + str.replace(new RegExp(escapeRegExp(quoteChar), 'g'), quoteChar + quoteChar) + quoteChar;
		return escaped;
	}
	return str;
}

/**
 * Collect all unique keys from a set of rows, preserving first-seen order.
 */
function discoverHeaders(rows: Record<string, unknown>[]): string[] {
	const set = new Set<string>();
	for (const row of rows) {
		for (const key of Object.keys(row)) set.add(key);
	}
	return [...set];
}

/**
 * Convert a batch of rows to CSV lines (string).
 * Includes a trailing newline so batches can be concatenated.
 */
function batchToCsvLines(
	rows: Record<string, unknown>[],
	headers: string[],
	delimiter: string,
	quoteChar: string,
): string {
	const lines = new Array<string>(rows.length);
	for (let i = 0; i < rows.length; i++) {
		const row = rows[i]!;
		lines[i] = headers
			.map((h) => escapeCsvField(row[h], delimiter, quoteChar))
			.join(delimiter);
	}
	return lines.join('\n') + '\n';
}

/**
 * Write to a stream with backpressure handling.
 * Waits for `drain` if the internal buffer is full.
 */
function streamWrite(stream: WriteStream, data: string): Promise<void> {
	return new Promise((resolve, reject) => {
		const ok = stream.write(data, 'utf-8');
		if (ok) {
			resolve();
		} else {
			stream.once('drain', resolve);
			stream.once('error', reject);
		}
	});
}

// ── Query helpers ──────────────────────────────────────────────────────

interface IJoinConfig {
	table: string;
	columns: string;
	joinType: string;
	orderBy?: string;
	orderAscending?: boolean;
	limit?: number;
}

function buildSelectQuery(
	supabase: SupabaseClient,
	table: string,
	selectFields: string,
	filters: IRowFilter[],
	sort: IRowSort[],
	joins?: IJoinConfig[],
) {
	let query = supabase.from(table).select(selectFields);

	for (const filter of filters) {
		const operator = convertFilterOperator(filter.operator);
		query = query.filter(
			filter.column,
			operator,
			normalizeFilterValue(filter.operator, filter.value),
		);
	}

	for (const s of sort) {
		query = query.order(s.column, { ascending: s.ascending });
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

function parseFilters(context: IExecuteFunctions, itemIndex: number): IRowFilter[] {
	const uiMode = context.getNodeParameter('uiMode', itemIndex, 'simple') as string;

	if (uiMode === 'simple') {
		return context.getNodeParameter('filters.filter', itemIndex, []) as IRowFilter[];
	}

	const raw = context.getNodeParameter('advancedFilters', itemIndex, '') as string;
	if (!raw) return [];

	try {
		const parsed = JSON.parse(raw);
		const filters: IRowFilter[] = [];
		for (const [column, condition] of Object.entries(parsed)) {
			if (typeof condition === 'object' && condition !== null) {
				const [operator, value] = Object.entries(condition)[0] as [string, unknown];
				filters.push({ column, operator: operator as IRowFilter['operator'], value: value as any });
			} else {
				filters.push({ column, operator: 'eq', value: condition as any });
			}
		}
		return filters;
	} catch {
		throw new Error('Invalid advanced filters JSON');
	}
}

// ── Batched fetch (async generator) ────────────────────────────────────

const DEFAULT_BATCH_SIZE = 1000;

/**
 * Yields rows in batches of ~1000 using keyset pagination (O(1) per batch)
 * when an `id` column is available, falling back to OFFSET only when it isn't.
 */
async function* fetchBatches(
	supabase: SupabaseClient,
	table: string,
	selectFields: string,
	filters: IRowFilter[],
	sort: IRowSort[],
	hostUrl: string,
	returnAll: boolean,
	limit: number,
	joins?: IJoinConfig[],
): AsyncGenerator<Record<string, unknown>[]> {
	const overhead = estimateUrlOverhead(hostUrl, table, selectFields, filters, sort);
	const maxInChars = Math.max(500, MAX_SAFE_URL_LENGTH - overhead);
	const maxItems = computeMaxIdsPerChunk(selectFields);
	const filterChunks = expandChunkedFilters(filters, maxInChars, maxItems);
	const BATCH_SIZE = computeBatchSize(selectFields);

	const hasIdColumn =
		selectFields === '*' || selectFields.split(',').some((f) => f.trim() === 'id');

	let totalYielded = 0;
	const maxRows = returnAll ? Infinity : limit;
	const startTime = Date.now();

	console.log(`[Supabase CSV] starting export table=${table} returnAll=${returnAll} chunks=${filterChunks.length} keyset=${hasIdColumn} batchSize=${BATCH_SIZE}`);

	for (let ci = 0; ci < filterChunks.length; ci++) {
		const chunkFilters = filterChunks[ci]!;
		if (totalYielded >= maxRows) break;

		if (returnAll) {
			let hasMore = true;
			let batchNum = 0;

			if (hasIdColumn) {
				// Keyset pagination — O(1) per batch regardless of offset
				let lastId: number | string | null = null;

				while (hasMore) {
					let query = buildSelectQuery(supabase, table, selectFields, chunkFilters, [], joins);
					if (lastId !== null) query = query.gt('id', lastId);
					query = query.order('id', { ascending: true }).limit(BATCH_SIZE);

					const { data, error } = await query;
					if (error) throw new Error(formatSupabaseError(error));

					if (Array.isArray(data) && data.length > 0) {
						yield data;
						totalYielded += data.length;
						lastId = data[data.length - 1].id;
						hasMore = data.length === BATCH_SIZE;
					} else {
						hasMore = false;
					}

					batchNum++;
					if (batchNum % 50 === 0) {
						const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
						console.log(`[Supabase CSV] chunk ${ci + 1}/${filterChunks.length} batch ${batchNum} — ${totalYielded} rows fetched (${elapsed}s)`);
					}
				}
			} else {
				// OFFSET fallback — only for tables without id column
				let offset = 0;

				while (hasMore) {
					const query = buildSelectQuery(
						supabase, table, selectFields, chunkFilters, sort, joins,
					);
					const { data, error } = await query.range(offset, offset + BATCH_SIZE - 1);
					if (error) throw new Error(formatSupabaseError(error));

					if (Array.isArray(data) && data.length > 0) {
						yield data;
						totalYielded += data.length;
						hasMore = data.length === BATCH_SIZE;
					} else {
						hasMore = false;
					}
					offset += BATCH_SIZE;

					batchNum++;
					if (batchNum % 50 === 0) {
						const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
						console.log(`[Supabase CSV] chunk ${ci + 1}/${filterChunks.length} batch ${batchNum} (offset) — ${totalYielded} rows fetched (${elapsed}s)`);
					}
				}
			}
		} else {
			// Limited fetch — single request per filter-chunk
			const remaining = maxRows - totalYielded;
			if (remaining <= 0) break;

			const query = buildSelectQuery(supabase, table, selectFields, chunkFilters, sort, joins);
			const { data, error } = await query.limit(remaining);
			if (error) throw new Error(formatSupabaseError(error));

			if (Array.isArray(data) && data.length > 0) {
				yield data;
				totalYielded += data.length;
			}
		}
	}

	const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
	console.log(`[Supabase CSV] fetch complete — ${totalYielded} rows in ${elapsed}s`);
}

// ── Node definition ────────────────────────────────────────────────────

export class SupabaseCsvExport implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Supabase CSV Export',
		name: 'supabaseCsvExport',
		icon: 'file:icons/supabase.svg',
		group: ['output'],
		version: 1,
		subtitle: '={{$parameter["table"]}}',
		description: 'Fetch data from Supabase, transform with JavaScript, and export as CSV file',
		defaults: {
			name: 'Supabase CSV Export',
		},
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'supabaseExtendedApi',
				required: true,
			},
		],
		properties: [
			// ── Data source ────────────────────────────────────────────
			{
				displayName: 'Table',
				name: 'table',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getTables',
				},
				required: true,
				default: '',
				description: 'Table to export data from',
			},
			{
				displayName: 'Return Fields',
				name: 'returnFields',
				type: 'string',
				default: '*',
				placeholder: 'id,name,email',
				description: 'Comma-separated list of columns to fetch (* for all)',
			},

			// ── Joins ──────────────────────────────────────────────────
			{
				displayName: 'Joins',
				name: 'joins',
				type: 'fixedCollection',
				typeOptions: { multipleValues: true },
				default: {},
				placeholder: 'Add Join',
				description: 'Join related tables via foreign keys (PostgREST resource embedding)',
				options: [
					{
						displayName: 'Join',
						name: 'join',
						values: [
							{
								displayName: 'Table',
								name: 'table',
								type: 'string',
								default: '',
								placeholder: 'related_table',
								description: 'Related table to join',
							},
							{
								displayName: 'Columns',
								name: 'columns',
								type: 'string',
								default: '*',
								placeholder: 'col1,col2',
								description:
									'Comma-separated columns from the joined table (* for all)',
							},
							{
								displayName: 'Join Type',
								name: 'joinType',
								type: 'options',
								options: [
									{ name: 'Left Join', value: 'left' },
									{ name: 'Inner Join', value: 'inner' },
								],
								default: 'left',
							},
							{
								displayName: 'Order By',
								name: 'orderBy',
								type: 'string',
								default: '',
								placeholder: 'created_at',
								description: 'Column in the joined table to order by (leave empty for no ordering)',
							},
							{
								displayName: 'Order Ascending',
								name: 'orderAscending',
								type: 'boolean',
								default: false,
								description: 'Whether to sort in ascending order (default descending, useful for "latest first")',
								displayOptions: {
									hide: {
										orderBy: [''],
									},
								},
							},
							{
								displayName: 'Limit',
								name: 'limit',
								type: 'number',
								typeOptions: {
									minValue: 0,
								},
								default: 0,
								description: 'Max rows from the joined table per parent row (0 = no limit, 1 = latest only)',
							},
						],
					},
				],
			},

			// ── Filters ────────────────────────────────────────────────
			{
				displayName: 'UI Mode',
				name: 'uiMode',
				type: 'options',
				options: [
					{ name: 'Simple', value: 'simple', description: 'Use form fields' },
					{ name: 'Advanced', value: 'advanced', description: 'Use JSON filters' },
				],
				default: 'simple',
			},
			{
				displayName: 'Filters',
				name: 'filters',
				type: 'fixedCollection',
				typeOptions: { multipleValues: true },
				default: {},
				placeholder: 'Add Filter',
				displayOptions: { show: { uiMode: ['simple'] } },
				options: [
					{
						displayName: 'Filter',
						name: 'filter',
						values: [
							{
								displayName: 'Column',
								name: 'column',
								type: 'options',
								typeOptions: { loadOptionsMethod: 'getColumns' },
								default: '',
							},
							{
								displayName: 'Operator',
								name: 'operator',
								type: 'options',
								options: [
									{ name: 'Equals', value: 'eq' },
									{ name: 'Not Equals', value: 'neq' },
									{ name: 'Greater Than', value: 'gt' },
									{ name: 'Greater Than or Equal', value: 'gte' },
									{ name: 'Less Than', value: 'lt' },
									{ name: 'Less Than or Equal', value: 'lte' },
									{ name: 'Like', value: 'like' },
									{ name: 'Case Insensitive Like', value: 'ilike' },
									{ name: 'Is', value: 'is' },
									{ name: 'In', value: 'in' },
									{ name: 'Contains', value: 'cs' },
									{ name: 'Contained By', value: 'cd' },
								],
								default: 'eq',
							},
							{
								displayName: 'Value',
								name: 'value',
								type: 'string',
								default: '',
							},
						],
					},
				],
			},
			{
				displayName: 'Advanced Filters',
				name: 'advancedFilters',
				type: 'json',
				default: '{}',
				description: 'JSON object with filter conditions',
				displayOptions: { show: { uiMode: ['advanced'] } },
			},

			// ── Sort ───────────────────────────────────────────────────
			{
				displayName: 'Sort',
				name: 'sort',
				type: 'fixedCollection',
				typeOptions: { multipleValues: true },
				default: {},
				placeholder: 'Add Sort Field',
				options: [
					{
						displayName: 'Sort Field',
						name: 'sortField',
						values: [
							{
								displayName: 'Column',
								name: 'column',
								type: 'options',
								typeOptions: { loadOptionsMethod: 'getColumns' },
								default: '',
							},
							{
								displayName: 'Ascending',
								name: 'ascending',
								type: 'boolean',
								default: true,
							},
						],
					},
				],
			},

			// ── Pagination ─────────────────────────────────────────────
			{
				displayName: 'Return All',
				name: 'returnAll',
				type: 'boolean',
				default: true,
				description: 'Whether to fetch all matching rows',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				default: 100,
				description: 'Maximum number of rows to fetch',
				displayOptions: { show: { returnAll: [false] } },
			},

			// ── Transform ──────────────────────────────────────────────
			{
				displayName: 'Transform Data',
				name: 'enableTransform',
				type: 'boolean',
				default: false,
				description:
					'Whether to apply a JavaScript transform before generating the CSV. ' +
					'The transform runs per batch (~1000 rows) so it stays memory-efficient ' +
					'even for very large exports. Use .filter() and .map() to shape your data.',
			},
			{
				displayName: 'Transform Parameters',
				name: 'transformParams',
				type: 'fixedCollection',
				typeOptions: { multipleValues: true },
				default: {},
				placeholder: 'Add Parameter',
				displayOptions: { show: { enableTransform: [true] } },
				description:
					'Key-value pairs passed into the transform code as a "params" object. ' +
					'Values support n8n expressions, so you can reference previous nodes ' +
					'(e.g. {{ $json.threshold }} or {{ $(\'My Node\').item.json.value }}).',
				options: [
					{
						displayName: 'Parameter',
						name: 'param',
						values: [
							{
								displayName: 'Name',
								name: 'name',
								type: 'string',
								default: '',
								placeholder: 'threshold',
								description: 'Name used in transform code as params.<name>',
							},
							{
								displayName: 'Value',
								name: 'value',
								type: 'string',
								default: '',
								placeholder: '={{ $json.minScore }}',
								description:
									'Value (supports n8n expressions to reference previous nodes)',
							},
						],
					},
				],
			},
			{
				displayName: 'Transform Code',
				name: 'transformCode',
				type: 'string',
				typeOptions: {
					editor: 'codeNodeEditor',
					editorLanguage: 'javaScript',
				},
				displayOptions: { show: { enableTransform: [true] } },
				default:
					'// "rows"   — array of objects from the database\n' +
					'// "params" — values passed in from Transform Parameters above\n' +
					'//            (use n8n expressions there to reference previous nodes)\n' +
					'\n' +
					'return rows\n' +
					'  .filter(row => row.score >= (params.threshold || 0))\n' +
					'  .map(row => ({\n' +
					'    id: row.id,\n' +
					'    name: row.name,\n' +
					'    email: row.email,\n' +
					'  }));\n',
				description:
					'JavaScript code that receives "rows" (array) and "params" (object). ' +
					'Must return an array of objects. Each object becomes a CSV row.',
			},

			// ── ID extraction ──────────────────────────────────────────
			{
				displayName: 'ID Column',
				name: 'idColumn',
				type: 'string',
				default: 'id',
				description:
					'Column to extract row IDs from (returned alongside the CSV). ' +
					'Uses the final (post-transform) data — make sure your transform preserves this column.',
			},

			// ── CSV options ────────────────────────────────────────────
			{
				displayName: 'CSV Options',
				name: 'csvOptions',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Delimiter',
						name: 'delimiter',
						type: 'string',
						default: ',',
						description: 'Column separator character',
					},
					{
						displayName: 'Include Headers',
						name: 'includeHeaders',
						type: 'boolean',
						default: true,
						description: 'Whether to include a header row',
					},
					{
						displayName: 'File Name',
						name: 'fileName',
						type: 'string',
						default: 'export.csv',
						description: 'Name of the generated CSV file',
					},
					{
						displayName: 'Quote Character',
						name: 'quoteChar',
						type: 'string',
						default: '"',
						description: 'Character used to quote fields containing special characters',
					},
				],
			},
		],
	};

	// ── Load options (reused from main node) ───────────────────────────

	methods = {
		loadOptions: {
			async getTables(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const credentials = (await this.getCredentials(
					'supabaseExtendedApi',
				)) as unknown as ISupabaseCredentials;
				validateCredentials(credentials);
				const host = credentials.host.replace(/\/$/, '');
				try {
					const response = await this.helpers.request({
						method: 'GET',
						url: `${host}/rest/v1/`,
						headers: {
							apikey: credentials.serviceKey,
							Authorization: `Bearer ${credentials.serviceKey}`,
						},
						json: true,
					});
					const definitions = response.definitions || {};
					const tables = Object.keys(definitions).sort();
					if (tables.length === 0) {
						return [
							{
								name: 'No tables found',
								value: '',
								description: 'No tables are exposed via the REST API',
							},
						];
					}
					return tables.map((t: string) => ({ name: t, value: t }));
				} catch (error) {
					const msg = error instanceof Error ? error.message : 'Unknown error';
					return [{ name: `Error: ${msg}`, value: '' }];
				}
			},

			async getColumns(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const credentials = (await this.getCredentials(
					'supabaseExtendedApi',
				)) as unknown as ISupabaseCredentials;
				validateCredentials(credentials);
				const table = this.getCurrentNodeParameter('table') as string;
				if (!table) {
					return [{ name: 'Select a table first', value: '' }];
				}
				const host = credentials.host.replace(/\/$/, '');
				try {
					const response = await this.helpers.request({
						method: 'GET',
						url: `${host}/rest/v1/`,
						headers: {
							apikey: credentials.serviceKey,
							Authorization: `Bearer ${credentials.serviceKey}`,
						},
						json: true,
					});
					const tableSchema = response.definitions?.[table];
					if (!tableSchema?.properties) {
						return [{ name: 'No columns found', value: '' }];
					}
					return Object.keys(tableSchema.properties)
						.sort()
						.map((col: string) => {
							const def = tableSchema.properties[col];
							const typeLabel = def.format ? `${def.type} (${def.format})` : def.type;
							return { name: col, value: col, description: typeLabel };
						});
				} catch (error) {
					const msg = error instanceof Error ? error.message : 'Unknown error';
					return [{ name: `Error: ${msg}`, value: '' }];
				}
			},
		},
	};

	// ── Execute ────────────────────────────────────────────────────────

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const credentials = (await this.getCredentials(
			'supabaseExtendedApi',
		)) as unknown as ISupabaseCredentials;

		try {
			validateCredentials(credentials);
		} catch (error) {
			const msg = error instanceof Error ? error.message : 'Unknown error';
			throw new NodeOperationError(this.getNode(), `Invalid credentials: ${msg}`);
		}

		const supabase = createSupabaseClient(credentials);

		// ── 1. Gather parameters ───────────────────────────────────
		const table = this.getNodeParameter('table', 0) as string;
		validateTableName(table);

		const returnFields = this.getNodeParameter('returnFields', 0, '*') as string;
		const returnAll = this.getNodeParameter('returnAll', 0, true) as boolean;
		const limit = returnAll ? 0 : (this.getNodeParameter('limit', 0, 100) as number);
		const filters = parseFilters(this, 0);
		const sort = this.getNodeParameter('sort.sortField', 0, []) as IRowSort[];
		const enableTransform = this.getNodeParameter('enableTransform', 0, false) as boolean;
		const idColumn = this.getNodeParameter('idColumn', 0, 'id') as string;

		const csvOpts = this.getNodeParameter('csvOptions', 0, {}) as Record<string, unknown>;
		const csvOptions: CsvOptions = {
			delimiter: (csvOpts.delimiter as string) || ',',
			quoteChar: (csvOpts.quoteChar as string) || '"',
			includeHeaders: csvOpts.includeHeaders !== false,
			fileName: (csvOpts.fileName as string) || 'export.csv',
		};

		// Build select string with joins
		const joins = this.getNodeParameter('joins.join', 0, []) as IJoinConfig[];
		let selectWithJoins = returnFields;
		for (const j of joins) {
			if (!j.table) continue;
			const cols = j.columns || '*';
			const hint = j.joinType === 'inner' ? `${j.table}!inner` : j.table;
			selectWithJoins += `,${hint}(${cols})`;
		}

		const { delimiter, quoteChar } = csvOptions;

		// ── Stream CSV to a temp file ──────────────────────────────
		//
		// Writing to disk keeps memory flat regardless of row count.
		// Only ~1000 rows (one batch) are in memory at any time.
		// At the end we pass a ReadStream to prepareBinaryData so
		// n8n stores the file without loading it all into memory.

		const tmpPath = join(tmpdir(), `n8n-csv-${randomBytes(8).toString('hex')}.csv`);
		const fileStream = createWriteStream(tmpPath, { encoding: 'utf-8' });

		const ids: unknown[] = [];
		let rowCount = 0;
		let headers: string[] | null = null;

		try {
			// Build the transform function once (if enabled)
			type TransformFn = (rows: Record<string, unknown>[], params: Record<string, unknown>) => Record<string, unknown>[];
			let transformFn: TransformFn | null = null;
			let params: Record<string, unknown> = {};

			if (enableTransform) {
				const paramEntries = this.getNodeParameter(
					'transformParams.param', 0, [],
				) as Array<{ name: string; value: unknown }>;
				for (const entry of paramEntries) {
					if (entry.name) params[entry.name] = entry.value;
				}

				const code = this.getNodeParameter('transformCode', 0, 'return rows;') as string;
				try {
					// eslint-disable-next-line @typescript-eslint/no-implied-eval
					transformFn = new Function('rows', 'params', code) as TransformFn;
				} catch (error) {
					const msg = error instanceof Error ? error.message : 'Unknown error';
					throw new NodeOperationError(this.getNode(), `Transform code syntax error: ${msg}`);
				}
			}

			for await (const batch of fetchBatches(
				supabase, table, selectWithJoins, filters, sort,
				credentials.host, returnAll, limit, joins,
			)) {
				// Apply per-batch transform
				let rows = batch;
				if (transformFn) {
					try {
						const result = transformFn(batch, params);
						if (!Array.isArray(result)) {
							throw new Error('Transform code must return an array. Got: ' + typeof result);
						}
						rows = result;
					} catch (error) {
						if (error instanceof NodeOperationError) throw error;
						const msg = error instanceof Error ? error.message : 'Unknown error';
						throw new NodeOperationError(this.getNode(), `Transform code error: ${msg}`);
					}
				}

				if (rows.length === 0) continue;

				// Discover headers from first non-empty batch
				if (headers === null) {
					headers = discoverHeaders(rows);

					if (csvOptions.includeHeaders) {
						const headerLine = headers
							.map((h) => escapeCsvField(h, delimiter, quoteChar))
							.join(delimiter) + '\n';
						await streamWrite(fileStream, headerLine);
					}
				}

				// Extract IDs
				for (const row of rows) {
					if (row[idColumn] != null) ids.push(row[idColumn]);
				}

				// Write CSV lines to temp file
				await streamWrite(fileStream, batchToCsvLines(rows, headers, delimiter, quoteChar));

				rowCount += rows.length;
			}

			// Close the write stream
			await new Promise<void>((resolve, reject) => {
				fileStream.end(() => resolve());
				fileStream.on('error', reject);
			});

			console.log(`[Supabase CSV] wrote ${rowCount} rows to temp file, passing to n8n binary storage`);

			// Stream the temp file into n8n's binary data storage —
			// prepareBinaryData accepts a Readable, so the full CSV
			// is never loaded into Node.js memory.
			const readStream = createReadStream(tmpPath);
			const binaryData = await this.helpers.prepareBinaryData(
				readStream,
				csvOptions.fileName,
				'text/csv',
			);

			return [[
				{
					json: {
						table,
						rowCount,
						ids,
						fileName: csvOptions.fileName,
					},
					binary: {
						data: binaryData,
					},
				},
			]];
		} catch (error) {
			const msg = error instanceof Error ? error.message : 'Unknown error';
			if (error instanceof NodeOperationError) throw error;
			throw new NodeOperationError(this.getNode(), `Export failed: ${msg}`);
		} finally {
			// Clean up temp file — stream may or may not be open
			try { fileStream.destroy(); } catch { /* ignore */ }
			try { unlinkSync(tmpPath); } catch { /* ignore if already gone */ }
		}
	}
}
