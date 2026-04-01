// Database operation types
export interface ISupabaseCredentials {
	host: string;
	serviceKey: string;
	additionalOptions?: {
		schema?: string;
	};
}

export interface IRowFilter {
	column: string;
	operator: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'like' | 'ilike' | 'is' | 'in' | 'cs' | 'cd';
	value: string | number | boolean | null | unknown[];
}

export interface IRowSort {
	column: string;
	ascending: boolean;
}

export interface IColumnDefinition {
	name: string;
	type: string;
	nullable?: boolean;
	defaultValue?: string;
	primaryKey?: boolean;
	unique?: boolean;
}

export interface IIndexDefinition {
	name: string;
	columns: string[];
	unique?: boolean;
	method?: 'btree' | 'hash' | 'gist' | 'gin';
}

// Storage operation types
export interface IStorageFile {
	name: string;
	id?: string;
	updated_at?: string;
	created_at?: string;
	last_accessed_at?: string;
	metadata?: Record<string, any>;
}

export interface IStorageBucket {
	id: string;
	name: string;
	owner?: string;
	public?: boolean;
	file_size_limit?: number;
	allowed_mime_types?: string[];
	created_at?: string;
	updated_at?: string;
}

export interface IFileUploadOptions {
	cacheControl?: string;
	contentType?: string;
	upsert?: boolean;
	metadata?: Record<string, any>;
}

export interface IFileListOptions {
	limit?: number;
	offset?: number;
	sortBy?: {
		column: 'name' | 'id' | 'updated_at' | 'created_at' | 'last_accessed_at';
		order: 'asc' | 'desc';
	};
	search?: string;
}

// Operation result types
export interface ISupabaseResponse<T = any> {
	data: T | null;
	error: any;
	count?: number;
	status?: number;
	statusText?: string;
}

export interface ISupabaseStorageResponse<T = any> {
	data: T | null;
	error: {
		message: string;
		statusCode?: string;
	} | null;
}

// Node operation types
export type DatabaseOperation = 
	| 'create'
	| 'read'
	| 'update'
	| 'delete'
	| 'upsert'
	| 'createTable'
	| 'dropTable'
	| 'addColumn'
	| 'dropColumn'
	| 'createIndex'
	| 'dropIndex'
	| 'customQuery'
	| 'findOrCreate'
	| 'updateByQuery';

export type StorageOperation =
	| 'uploadFile'
	| 'downloadFile'
	| 'listFiles'
	| 'deleteFile'
	| 'moveFile'
	| 'copyFile'
	| 'createBucket'
	| 'deleteBucket'
	| 'listBuckets'
	| 'getBucketDetails'
	| 'getFileInfo'
	| 'generateSignedUrl';

export type SupabaseResource = 'database' | 'storage';

export interface ISupabaseNodeParameters {
	resource: SupabaseResource;
	operation: DatabaseOperation | StorageOperation;
	
	// Database parameters
	table?: string;
	columns?: string;
	filters?: IRowFilter[];
	sort?: IRowSort[];
	limit?: number;
	offset?: number;
	returnFields?: string;
	
	// Schema management parameters
	tableName?: string;
	columnDefinitions?: IColumnDefinition[];
	indexDefinition?: IIndexDefinition;
	customSql?: string;
	
	// Storage parameters
	bucket?: string;
	fileName?: string;
	filePath?: string;
	fileContent?: string;
	fileOptions?: IFileUploadOptions;
	listOptions?: IFileListOptions;
	signedUrlOptions?: {
		expiresIn: number;
		download?: boolean;
	};
	
	// UI mode
	uiMode?: 'simple' | 'advanced';
	
	// Additional options
	additionalOptions?: Record<string, any>;
}
