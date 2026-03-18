import { SupabaseClient } from '@supabase/supabase-js';
import { IDataObject, IExecuteFunctions, INodeExecutionData } from 'n8n-workflow';
import {
	IRowFilter,
	IRowSort,
	IColumnDefinition,
	IIndexDefinition,
	ISupabaseResponse,
	DatabaseOperation,
} from '../../types';
import {
	validateTableName,
	validateColumnName,
	convertFilterOperator,
	formatSupabaseError,
} from '../../utils/supabaseClient';

/**
 * Execute database operations
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
			case 'create':
				returnData.push(...await handleCreate.call(this, supabase, itemIndex));
				break;
			case 'read':
				returnData.push(...await handleRead.call(this, supabase, itemIndex));
				break;
			case 'update':
				returnData.push(...await handleUpdate.call(this, supabase, itemIndex));
				break;
			case 'delete':
				returnData.push(...await handleDelete.call(this, supabase, itemIndex));
				break;
			case 'upsert':
				returnData.push(...await handleUpsert.call(this, supabase, itemIndex));
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
 * Handle CREATE operation
 */
async function handleCreate(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', itemIndex) as string;
	const uiMode = this.getNodeParameter('uiMode', itemIndex, 'simple') as string;
	
	validateTableName(table);

	let dataToInsert: IDataObject;

	if (uiMode === 'advanced') {
		const jsonData = this.getNodeParameter('jsonData', itemIndex, '{}') as string;
		try {
			dataToInsert = JSON.parse(jsonData);
		} catch {
			throw new Error('Invalid JSON data provided');
		}
	} else {
		const columns = this.getNodeParameter('columns.column', itemIndex, []) as Array<{
			name: string;
			value: any;
		}>;
		
		dataToInsert = {};
		for (const column of columns) {
			if (column.name && column.value !== undefined) {
				dataToInsert[column.name] = column.value;
			}
		}
	}

	const { data, error } = await supabase
		.from(table)
		.insert(dataToInsert)
		.select();

	if (error) {
		throw new Error(formatSupabaseError(error));
	}

	return [{ json: { data, operation: 'create', table } }];
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
	const uiMode = this.getNodeParameter('uiMode', itemIndex, 'simple') as string;
	
	validateTableName(table);

	let query = supabase.from(table).select('*');

	// Handle columns selection
	const returnFields = this.getNodeParameter('returnFields', itemIndex, '*') as string;
	if (returnFields && returnFields !== '*') {
		query = supabase.from(table).select(returnFields);
	}

	// Handle filters
	if (uiMode === 'simple') {
		const filters = this.getNodeParameter('filters.filter', itemIndex, []) as IRowFilter[];
		for (const filter of filters) {
			const operator = convertFilterOperator(filter.operator);
			query = query.filter(filter.column, operator, filter.value);
		}
	} else {
		const advancedFilters = this.getNodeParameter('advancedFilters', itemIndex, '') as string;
		if (advancedFilters) {
			// Parse and apply advanced filters (JSON format)
			try {
				const filters = JSON.parse(advancedFilters);
				for (const [column, condition] of Object.entries(filters)) {
					if (typeof condition === 'object' && condition !== null) {
						const [operator, value] = Object.entries(condition)[0] as [string, any];
						query = query.filter(column, convertFilterOperator(operator), value);
					} else {
						query = query.eq(column, condition);
					}
				}
			} catch {
				throw new Error('Invalid advanced filters JSON');
			}
		}
	}

	// Handle sorting
	const sort = this.getNodeParameter('sort.sortField', itemIndex, []) as IRowSort[];
	for (const sortField of sort) {
		query = query.order(sortField.column, { ascending: sortField.ascending });
	}

	// Handle pagination
	const limit = this.getNodeParameter('limit', itemIndex, undefined) as number | undefined;
	const offset = this.getNodeParameter('offset', itemIndex, undefined) as number | undefined;

	if (limit !== undefined) {
		query = query.limit(limit);
	}
	if (offset !== undefined) {
		query = query.range(offset, offset + (limit || 1000) - 1);
	}

	const { data, error, count } = await query;

	if (error) {
		throw new Error(formatSupabaseError(error));
	}

	// Return each row as a separate item for n8n processing
	const returnData: INodeExecutionData[] = [];
	if (Array.isArray(data)) {
		for (const row of data) {
			returnData.push({ json: row });
		}
	}

	// If no data found, return empty result with metadata
	if (returnData.length === 0) {
		returnData.push({
			json: {
				data: [],
				count: count || 0,
				operation: 'read',
				table,
				message: 'No records found',
			},
		});
	}

	return returnData;
}

/**
 * Handle UPDATE operation
 */
async function handleUpdate(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', itemIndex) as string;
	const uiMode = this.getNodeParameter('uiMode', itemIndex, 'simple') as string;
	
	validateTableName(table);

	let dataToUpdate: IDataObject;

	if (uiMode === 'advanced') {
		const jsonData = this.getNodeParameter('jsonData', itemIndex, '{}') as string;
		try {
			dataToUpdate = JSON.parse(jsonData);
		} catch {
			throw new Error('Invalid JSON data provided');
		}
	} else {
		const columns = this.getNodeParameter('columns.column', itemIndex, []) as Array<{
			name: string;
			value: any;
		}>;
		
		dataToUpdate = {};
		for (const column of columns) {
			if (column.name && column.value !== undefined) {
				dataToUpdate[column.name] = column.value;
			}
		}
	}

	let query = supabase.from(table).update(dataToUpdate);

	// Apply filters to determine which rows to update
	const filters = this.getNodeParameter('filters.filter', itemIndex, []) as IRowFilter[];
	for (const filter of filters) {
		const operator = convertFilterOperator(filter.operator);
		query = query.filter(filter.column, operator, filter.value);
	}

	const { data, error } = await query.select();

	if (error) {
		throw new Error(formatSupabaseError(error));
	}

	return [{ json: { data, operation: 'update', table, updated: data?.length || 0 } }];
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
		query = query.filter(filter.column, operator, filter.value);
	}

	const { data, error } = await query.select();

	if (error) {
		throw new Error(formatSupabaseError(error));
	}

	return [{ json: { data, operation: 'delete', table, deleted: data?.length || 0 } }];
}

/**
 * Handle UPSERT operation
 */
async function handleUpsert(
	this: IExecuteFunctions,
	supabase: SupabaseClient,
	itemIndex: number,
): Promise<INodeExecutionData[]> {
	const table = this.getNodeParameter('table', itemIndex) as string;
	const uiMode = this.getNodeParameter('uiMode', itemIndex, 'simple') as string;
	const onConflict = this.getNodeParameter('onConflict', itemIndex, '') as string;
	
	validateTableName(table);

	let dataToUpsert: IDataObject;

	if (uiMode === 'advanced') {
		const jsonData = this.getNodeParameter('jsonData', itemIndex, '{}') as string;
		try {
			dataToUpsert = JSON.parse(jsonData);
		} catch {
			throw new Error('Invalid JSON data provided');
		}
	} else {
		const columns = this.getNodeParameter('columns.column', itemIndex, []) as Array<{
			name: string;
			value: any;
		}>;
		
		dataToUpsert = {};
		for (const column of columns) {
			if (column.name && column.value !== undefined) {
				dataToUpsert[column.name] = column.value;
			}
		}
	}

	const options: any = {};
	if (onConflict) {
		options.onConflict = onConflict;
	}

	const { data, error } = await supabase
		.from(table)
		.upsert(dataToUpsert, options)
		.select();

	if (error) {
		throw new Error(formatSupabaseError(error));
	}

	return [{ json: { data, operation: 'upsert', table } }];
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

	const { data, error } = await supabase.rpc('exec_sql', { sql });

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

	const { data, error } = await supabase.rpc('exec_sql', { sql });

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

	const { data, error } = await supabase.rpc('exec_sql', { sql });

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

	const { data, error } = await supabase.rpc('exec_sql', { sql });

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

	const { data, error } = await supabase.rpc('exec_sql', { sql });

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

	const { data, error } = await supabase.rpc('exec_sql', { sql });

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
