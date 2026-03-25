import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { ISupabaseCredentials, IRowFilter } from '../types';

/**
 * Creates a Supabase client instance with the provided credentials
 */
export function createSupabaseClient(credentials: ISupabaseCredentials): SupabaseClient {
	// Create client with minimal options to avoid deep type instantiation
	const client = createClient(credentials.host, credentials.serviceKey, {
		auth: {
			autoRefreshToken: true,
			persistSession: false,
			detectSessionInUrl: false,
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
 * Maximum character length for the serialized values of a single IN filter.
 * Supabase uses Kong/nginx which typically allows ~8KB URLs. We use 4000 chars
 * as a safe limit per IN clause, leaving room for the base URL, other query
 * params, and additional filters.
 */
export const IN_FILTER_MAX_CHAR_LENGTH = 4000;

/**
 * Splits an array of values into chunks where each chunk's comma-joined
 * string representation stays within the character length limit.
 */
export function chunkInFilterValues(values: unknown[]): unknown[][] {
	const chunks: unknown[][] = [];
	let currentChunk: unknown[] = [];
	let currentLength = 0;

	for (const value of values) {
		const valueStr = String(value);
		// +1 for the comma separator between values
		const addedLength = currentChunk.length === 0 ? valueStr.length : valueStr.length + 1;

		if (currentLength + addedLength > IN_FILTER_MAX_CHAR_LENGTH && currentChunk.length > 0) {
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
 * should be merged. Chunks are disjoint, so no deduplication is needed.
 */
export function expandChunkedFilters(filters: IRowFilter[]): IRowFilter[][] {
	const staticFilters: IRowFilter[] = [];
	const chunkedEntries: { filter: IRowFilter; chunks: unknown[][] }[] = [];

	for (const filter of filters) {
		if (filter.operator === 'in' && Array.isArray(filter.value)) {
			const serialized = (filter.value as unknown[]).map(String).join(',');
			if (serialized.length > IN_FILTER_MAX_CHAR_LENGTH) {
				chunkedEntries.push({
					filter,
					chunks: chunkInFilterValues(filter.value as unknown[]),
				});
				continue;
			}
		}
		staticFilters.push(filter);
	}

	if (chunkedEntries.length === 0) {
		return [filters];
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
