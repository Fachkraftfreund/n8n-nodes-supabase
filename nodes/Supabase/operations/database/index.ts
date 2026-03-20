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
): Promise<INodeExecutionData[]> {
	const returnData: INodeExecutionData[] = [];

	try {
		switch (operation) {
			case 'read':
				returnData.push(...await handleRead.call(this, supabase, itemIndex));
				break;
			case 'delete':
				returnData.push(...await handleDelete.call(this, supabase, itemIndex));
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
			default:
				throw new Error(`Unknown database operation: ${operation}`);
		}
	} catch (error) {
		throw new Error(`Database operation failed: ${formatSupabaseError(error)}`);
	}

	return returnData;
}

/**
 * Collect row data from all input items
 */
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
		rows.push(row);
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
			default:
				throw new Error(`Operation ${operation} does not support bulk mode`);
		}
	} catch (error) {
		throw new Error(`Database operation failed: ${formatSupabaseError(error)}`);
	}
}

/**
 * Handle bulk CREATE — single .insert() call for all items
 */
async function handleBulkCreate(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemCount: number,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', 0) as string;
	validateTableName(table);

	const rows = collectRowData(this, itemCount);

	const { data, error } = await supabase
		.from(table)
		.insert(rows)
		.select();

	if (error) throw new Error(formatSupabaseError(error));

	if (Array.isArray(data)) {
		return data.map((row) => ({ json: row }));
	}
	return [{ json: { data, operation: 'create', table } }];
}

/**
 * Handle bulk UPSERT — single .upsert() call for all items
 */
async function handleBulkUpsert(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemCount: number,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', 0) as string;
	const onConflict = this.getNodeParameter('onConflict', 0, '') as string;
	validateTableName(table);

	const rows = collectRowData(this, itemCount);

	const options: any = {};
	if (onConflict) options.onConflict = onConflict;

	const { data, error } = await supabase
		.from(table)
		.upsert(rows, options)
		.select();

	if (error) throw new Error(formatSupabaseError(error));

	if (Array.isArray(data)) {
		return data.map((row) => ({ json: row }));
	}
	return [{ json: { data, operation: 'upsert', table } }];
}

/**
 * Handle bulk UPDATE — uses .upsert() with onConflict match column
 * Each item must include the match column value in its data.
 */
async function handleBulkUpdate(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemCount: number,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', 0) as string;
	const matchColumn = this.getNodeParameter('matchColumn', 0) as string;
	validateTableName(table);

	if (!matchColumn) {
		throw new Error('Match Column is required for update operations');
	}

	const rows = collectRowData(this, itemCount);

	// Validate every row includes the match column
	for (let i = 0; i < rows.length; i++) {
		const row = rows[i];
		if (!row || row[matchColumn] === undefined) {
			throw new Error(`Item ${i} is missing the match column "${matchColumn}"`);
		}
	}

	const { data, error } = await supabase
		.from(table)
		.upsert(rows, { onConflict: matchColumn })
		.select();

	if (error) throw new Error(formatSupabaseError(error));

	if (Array.isArray(data)) {
		return data.map((row) => ({ json: row }));
	}
	return [{ json: { data, operation: 'update', table } }];
}

/**
 * Build a SELECT query with filters and sorting applied.
 */
function buildReadQuery(
	context: IExecuteFunctions,
	supabase: SupabaseClient,
	table: string,
	returnFields: string,
	itemIndex: number,
	options?: { count?: 'exact' },
) {
	const selectFields = returnFields && returnFields !== '*' ? returnFields : '*';
	let query = supabase.from(table).select(selectFields, options);

	const uiMode = context.getNodeParameter('uiMode', itemIndex, 'simple') as string;

	// Apply filters
	if (uiMode === 'simple') {
		const filters = context.getNodeParameter('filters.filter', itemIndex, []) as IRowFilter[];
		for (const filter of filters) {
			const operator = convertFilterOperator(filter.operator);
			query = query.filter(filter.column, operator, normalizeFilterValue(filter.operator, filter.value));
		}
	} else {
		const advancedFilters = context.getNodeParameter('advancedFilters', itemIndex, '') as string;
		if (advancedFilters) {
			try {
				const filters = JSON.parse(advancedFilters);
				for (const [column, condition] of Object.entries(filters)) {
					if (typeof condition === 'object' && condition !== null) {
						const [operator, value] = Object.entries(condition)[0] as [string, any];
						query = query.filter(column, convertFilterOperator(operator), normalizeFilterValue(operator, value));
					} else {
						query = query.eq(column, condition);
					}
				}
			} catch {
				throw new Error('Invalid advanced filters JSON');
			}
		}
	}

	// Apply sorting
	const sort = context.getNodeParameter('sort.sortField', itemIndex, []) as IRowSort[];
	for (const sortField of sort) {
		query = query.order(sortField.column, { ascending: sortField.ascending });
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
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', itemIndex) as string;
	validateTableName(table);

	const returnFields = this.getNodeParameter('returnFields', itemIndex, '*') as string;
	const returnAll = this.getNodeParameter('returnAll', itemIndex, false) as boolean;

	const returnData: INodeExecutionData[] = [];

	if (returnAll) {
		// Fetch all rows by paginating in batches
		const batchSize = 1000;
		let offset = 0;
		let hasMore = true;

		while (hasMore) {
			const batchQuery = buildReadQuery(this, supabase, table, returnFields, itemIndex, { count: 'exact' });
			const { data: batchData, error: batchError } = await batchQuery.range(offset, offset + batchSize - 1);

			if (batchError) {
				throw new Error(formatSupabaseError(batchError));
			}

			if (Array.isArray(batchData)) {
				for (const row of batchData) {
					returnData.push({ json: row });
				}
				hasMore = batchData.length === batchSize;
			} else {
				hasMore = false;
			}

			offset += batchSize;
		}
	} else {
		const limit = this.getNodeParameter('limit', itemIndex, undefined) as number | undefined;
		const offset = this.getNodeParameter('offset', itemIndex, undefined) as number | undefined;

		let query = buildReadQuery(this, supabase, table, returnFields, itemIndex);

		if (limit !== undefined) {
			query = query.limit(limit);
		}
		if (offset !== undefined) {
			query = query.range(offset, offset + (limit || 1000) - 1);
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

	return returnData;
}

/**
 * Handle DELETE operation
 */
async function handleDelete(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', itemIndex) as string;
	
	validateTableName(table);

	let query = supabase.from(table).delete();

	// Apply filters to determine which rows to delete
	const filters = this.getNodeParameter('filters.filter', itemIndex, []) as IRowFilter[];
	if (filters.length === 0) {
		throw new Error('At least one filter is required for delete operations to prevent accidental data loss');
	}

	for (const filter of filters) {
		const operator = convertFilterOperator(filter.operator);
		query = query.filter(filter.column, operator, normalizeFilterValue(filter.operator, filter.value));
	}

	const { data, error } = await query.select();

	if (error) {
		throw new Error(formatSupabaseError(error));
	}

	return [{ json: { data, operation: 'delete', table, deleted: data?.length || 0 } }];
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
