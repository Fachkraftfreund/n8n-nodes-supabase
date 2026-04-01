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

import { createSupabaseClient, validateCredentials } from './utils/supabaseClient';
import {
	validateTableName,
	convertFilterOperator,
	normalizeFilterValue,
	expandChunkedFilters,
	estimateUrlOverhead,
	computeMaxIdsPerChunk,
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

function escapeCsvField(value: unknown, delimiter: string, quoteChar: string): string {
	if (value === null || value === undefined) return '';
	const str = typeof value === 'object' ? JSON.stringify(value) : String(value);
	if (
		str.includes(delimiter) ||
		str.includes(quoteChar) ||
		str.includes('\n') ||
		str.includes('\r')
	) {
		return quoteChar + str.replace(new RegExp(escapeRegExp(quoteChar), 'g'), quoteChar + quoteChar) + quoteChar;
	}
	return str;
}

function escapeRegExp(s: string): string {
	return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function generateCsv(rows: Record<string, unknown>[], options: CsvOptions): string {
	if (rows.length === 0) return '';

	const { delimiter, quoteChar, includeHeaders } = options;

	// Collect all unique keys preserving first-seen order
	const headerSet = new Set<string>();
	for (const row of rows) {
		for (const key of Object.keys(row)) headerSet.add(key);
	}
	const headers = [...headerSet];

	const lines: string[] = [];
	if (includeHeaders) {
		lines.push(headers.map((h) => escapeCsvField(h, delimiter, quoteChar)).join(delimiter));
	}
	for (const row of rows) {
		lines.push(
			headers.map((h) => escapeCsvField(row[h], delimiter, quoteChar)).join(delimiter),
		);
	}

	return lines.join('\n');
}

// ── Query helpers (mirrors read operation logic) ───────────────────────

function buildSelectQuery(
	supabase: SupabaseClient,
	table: string,
	selectFields: string,
	filters: IRowFilter[],
	sort: IRowSort[],
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

// ── Paginated fetch ────────────────────────────────────────────────────

async function fetchAllRows(
	supabase: SupabaseClient,
	table: string,
	selectFields: string,
	filters: IRowFilter[],
	sort: IRowSort[],
	hostUrl: string,
	returnAll: boolean,
	limit: number,
): Promise<Record<string, unknown>[]> {
	const overhead = estimateUrlOverhead(hostUrl, table, selectFields, filters, sort);
	const maxInChars = Math.max(500, MAX_SAFE_URL_LENGTH - overhead);
	const maxItems = computeMaxIdsPerChunk(selectFields);
	const filterChunks = expandChunkedFilters(filters, maxInChars, maxItems);

	const allRows: Record<string, unknown>[] = [];

	if (returnAll) {
		const batchSize = 1000;
		const hasIdColumn =
			selectFields === '*' || selectFields.split(',').some((f) => f.trim() === 'id');

		for (const chunkFilters of filterChunks) {
			let hasMore = true;

			if (hasIdColumn) {
				// Keyset pagination
				let lastId: number | string | null = null;

				while (hasMore) {
					let query = buildSelectQuery(supabase, table, selectFields, chunkFilters, []);
					if (lastId !== null) {
						query = query.gt('id', lastId);
					}
					query = query.order('id', { ascending: true }).limit(batchSize);

					const { data, error } = await query;
					if (error) throw new Error(formatSupabaseError(error));

					if (Array.isArray(data) && data.length > 0) {
						for (const row of data) allRows.push(row);
						lastId = data[data.length - 1].id;
						hasMore = data.length === batchSize;
					} else {
						hasMore = false;
					}
				}
			} else {
				// OFFSET pagination fallback
				let offset = 0;

				while (hasMore) {
					const query = buildSelectQuery(supabase, table, selectFields, chunkFilters, sort);
					const { data, error } = await query.range(offset, offset + batchSize - 1);
					if (error) throw new Error(formatSupabaseError(error));

					if (Array.isArray(data) && data.length > 0) {
						for (const row of data) allRows.push(row);
						hasMore = data.length === batchSize;
					} else {
						hasMore = false;
					}
					offset += batchSize;
				}
			}
		}

		// Re-apply user sort when keyset pagination overrode it
		if (hasIdColumn && sort.length > 0) {
			allRows.sort((a, b) => {
				for (const s of sort) {
					const aVal = (a[s.column] ?? null) as any;
					const bVal = (b[s.column] ?? null) as any;
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
		// Limited fetch
		for (const chunkFilters of filterChunks) {
			const query = buildSelectQuery(supabase, table, selectFields, chunkFilters, sort);
			const { data, error } = await query.limit(limit);
			if (error) throw new Error(formatSupabaseError(error));
			if (Array.isArray(data)) {
				for (const row of data) allRows.push(row);
			}
		}

		// Trim to limit when multiple chunks exceeded it
		if (allRows.length > limit) {
			allRows.length = limit;
		}
	}

	return allRows;
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
				description: 'Whether to apply a JavaScript transform before generating the CSV',
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

		// Build select string with joins
		const joins = this.getNodeParameter('joins.join', 0, []) as Array<{
			table: string;
			columns: string;
			joinType: string;
		}>;
		let selectWithJoins = returnFields;
		for (const j of joins) {
			if (!j.table) continue;
			const cols = j.columns || '*';
			const hint = j.joinType === 'inner' ? `${j.table}!inner` : j.table;
			selectWithJoins += `,${hint}(${cols})`;
		}

		// ── 2. Fetch data ──────────────────────────────────────────
		let rows: Record<string, unknown>[];
		try {
			rows = await fetchAllRows(
				supabase,
				table,
				selectWithJoins,
				filters,
				sort,
				credentials.host,
				returnAll,
				limit,
			);
		} catch (error) {
			const msg = error instanceof Error ? error.message : 'Unknown error';
			throw new NodeOperationError(this.getNode(), `Failed to fetch data: ${msg}`);
		}

		// ── 3. Transform ───────────────────────────────────────────
		const enableTransform = this.getNodeParameter('enableTransform', 0, false) as boolean;

		if (enableTransform) {
			// Build params object from the Transform Parameters collection
			const paramEntries = this.getNodeParameter(
				'transformParams.param',
				0,
				[],
			) as Array<{ name: string; value: unknown }>;
			const params: Record<string, unknown> = {};
			for (const entry of paramEntries) {
				if (entry.name) {
					params[entry.name] = entry.value;
				}
			}

			const code = this.getNodeParameter('transformCode', 0, 'return rows;') as string;
			try {
				const transformFn = new Function('rows', 'params', code);
				const result = transformFn(rows, params);
				if (!Array.isArray(result)) {
					throw new Error(
						'Transform code must return an array. Got: ' + typeof result,
					);
				}
				rows = result;
			} catch (error) {
				const msg = error instanceof Error ? error.message : 'Unknown error';
				throw new NodeOperationError(
					this.getNode(),
					`Transform code error: ${msg}`,
				);
			}
		}

		// ── 4. Extract IDs ─────────────────────────────────────────
		const idColumn = this.getNodeParameter('idColumn', 0, 'id') as string;
		const ids: unknown[] = [];
		for (const row of rows) {
			if (row[idColumn] !== undefined && row[idColumn] !== null) {
				ids.push(row[idColumn]);
			}
		}

		// ── 5. Generate CSV ────────────────────────────────────────
		const csvOpts = this.getNodeParameter('csvOptions', 0, {}) as Record<string, unknown>;
		const csvOptions: CsvOptions = {
			delimiter: (csvOpts.delimiter as string) || ',',
			quoteChar: (csvOpts.quoteChar as string) || '"',
			includeHeaders: csvOpts.includeHeaders !== false,
			fileName: (csvOpts.fileName as string) || 'export.csv',
		};

		const csvContent = generateCsv(rows, csvOptions);
		const csvBuffer = Buffer.from(csvContent, 'utf-8');

		// ── 6. Return binary CSV + JSON metadata ───────────────────
		const binaryData = await this.helpers.prepareBinaryData(
			csvBuffer,
			csvOptions.fileName,
			'text/csv',
		);

		const returnItem: INodeExecutionData = {
			json: {
				table,
				rowCount: rows.length,
				ids,
				fileName: csvOptions.fileName,
			},
			binary: {
				data: binaryData,
			},
		};

		return [[returnItem]];
	}
}
