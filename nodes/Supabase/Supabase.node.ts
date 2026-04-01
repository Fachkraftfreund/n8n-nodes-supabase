import {
	IExecuteFunctions,
	ILoadOptionsFunctions,
	INodeExecutionData,
	INodeListSearchItems,
	INodeListSearchResult,
	INodePropertyOptions,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';

import { createSupabaseClient, validateCredentials } from './utils/supabaseClient';
import { executeDatabaseOperation, executeBulkDatabaseOperation } from './operations/database';
import { executeStorageOperation } from './operations/storage';
import { ISupabaseCredentials, SupabaseResource, DatabaseOperation, StorageOperation } from './types';

export class Supabase implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Supabase Extended',
		name: 'supabaseExtended',
		icon: 'file:icons/supabase.svg',
		group: ['database'],
		version: 1,
		subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
		description: 'Interact with Supabase database and storage',
		defaults: {
			name: 'Supabase Extended',
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
			{
				displayName: 'Resource',
				name: 'resource',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Database',
						value: 'database',
						description: 'Perform database operations (CRUD, schema management)',
					},
					{
						name: 'Storage',
						value: 'storage',
						description: 'Perform storage operations (files, buckets)',
					},
				],
				default: 'database',
			},

			// Database Operations
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: {
					show: {
						resource: ['database'],
					},
				},
				options: [
					{
						name: 'Create',
						value: 'create',
						description: 'Create a new row',
						action: 'Create a row',
					},
					{
						name: 'Read',
						value: 'read',
						description: 'Read rows from table',
						action: 'Read rows',
					},
					{
						name: 'Update',
						value: 'update',
						description: 'Update existing rows',
						action: 'Update rows',
					},
					{
						name: 'Delete',
						value: 'delete',
						description: 'Delete rows from table',
						action: 'Delete rows',
					},
					{
						name: 'Upsert',
						value: 'upsert',
						description: 'Insert or update rows',
						action: 'Upsert rows',
					},
					{
						name: 'Create Table',
						value: 'createTable',
						description: 'Create a new table',
						action: 'Create table',
					},
					{
						name: 'Drop Table',
						value: 'dropTable',
						description: 'Drop an existing table',
						action: 'Drop table',
					},
					{
						name: 'Add Column',
						value: 'addColumn',
						description: 'Add a column to existing table',
						action: 'Add column',
					},
					{
						name: 'Drop Column',
						value: 'dropColumn',
						description: 'Drop a column from table',
						action: 'Drop column',
					},
					{
						name: 'Create Index',
						value: 'createIndex',
						description: 'Create an index on table',
						action: 'Create index',
					},
					{
						name: 'Drop Index',
						value: 'dropIndex',
						description: 'Drop an existing index',
						action: 'Drop index',
					},
					{
						name: 'Custom Query',
						value: 'customQuery',
						description: 'Execute custom SQL query',
						action: 'Execute custom query',
					},
					{
						name: 'Find or Create',
						value: 'findOrCreate',
						description: 'Return existing row matching criteria, or create it if not found',
						action: 'Find or create row',
					},
					{
						name: 'Update by Query',
						value: 'updateByQuery',
						description: 'Update all rows matching filter conditions (e.g. update where id IN list)',
						action: 'Update rows by query',
					},
				],
				default: 'read',
			},

			// Storage Operations
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: {
					show: {
						resource: ['storage'],
					},
				},
				options: [
					{
						name: 'Upload File',
						value: 'uploadFile',
						description: 'Upload a file to storage',
						action: 'Upload file',
					},
					{
						name: 'Download File',
						value: 'downloadFile',
						description: 'Download a file from storage',
						action: 'Download file',
					},
					{
						name: 'List Files',
						value: 'listFiles',
						description: 'List files in a bucket/folder',
						action: 'List files',
					},
					{
						name: 'Delete File',
						value: 'deleteFile',
						description: 'Delete file(s) from storage',
						action: 'Delete file',
					},
					{
						name: 'Move File',
						value: 'moveFile',
						description: 'Move a file to different location',
						action: 'Move file',
					},
					{
						name: 'Copy File',
						value: 'copyFile',
						description: 'Copy a file to different location',
						action: 'Copy file',
					},
					{
						name: 'Create Bucket',
						value: 'createBucket',
						description: 'Create a new storage bucket',
						action: 'Create bucket',
					},
					{
						name: 'Delete Bucket',
						value: 'deleteBucket',
						description: 'Delete a storage bucket',
						action: 'Delete bucket',
					},
					{
						name: 'List Buckets',
						value: 'listBuckets',
						description: 'List all storage buckets',
						action: 'List buckets',
					},
					{
						name: 'Get Bucket Details',
						value: 'getBucketDetails',
						description: 'Get details of a bucket',
						action: 'Get bucket details',
					},
					{
						name: 'Get File Info',
						value: 'getFileInfo',
						description: 'Get information about a file',
						action: 'Get file info',
					},
					{
						name: 'Generate Signed URL',
						value: 'generateSignedUrl',
						description: 'Generate signed URL for file access',
						action: 'Generate signed URL',
					},
				],
				default: 'uploadFile',
			},

			// UI Mode Selection
			{
				displayName: 'UI Mode',
				name: 'uiMode',
				type: 'options',
				options: [
					{
						name: 'Simple',
						value: 'simple',
						description: 'Use form fields and dropdowns',
					},
					{
						name: 'Advanced',
						value: 'advanced',
						description: 'Use JSON inputs for more flexibility',
					},
				],
				default: 'simple',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['create', 'read', 'update', 'delete', 'upsert', 'updateByQuery'],
					},
				},
			},

			// Database Table Name
			{
				displayName: 'Table',
				name: 'table',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getTables',
				},
				required: true,
				default: '',
				description: 'Name of the table to operate on',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['create', 'read', 'update', 'delete', 'upsert', 'findOrCreate', 'updateByQuery'],
					},
				},
			},

			// Simple Mode - Columns for Create/Update/Upsert
			{
				displayName: 'Columns',
				name: 'columns',
				type: 'fixedCollection',
				typeOptions: {
					multipleValues: true,
				},
				default: {},
				placeholder: 'Add Column',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['create', 'update', 'upsert', 'updateByQuery'],
						uiMode: ['simple'],
					},
				},
				options: [
					{
						displayName: 'Column',
						name: 'column',
						values: [
							{
								displayName: 'Name',
								name: 'name',
								type: 'options',
								typeOptions: {
									loadOptionsMethod: 'getColumns',
								},
								default: '',
								description: 'Name of the column',
							},
							{
								displayName: 'Value',
								name: 'value',
								type: 'string',
								default: '',
								description: 'Value for the column',
							},
						],
					},
				],
			},

			// Advanced Mode - JSON Data
			{
				displayName: 'JSON Data',
				name: 'jsonData',
				type: 'json',
				default: '{}',
				description: 'JSON object containing the data',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['create', 'update', 'upsert', 'updateByQuery'],
						uiMode: ['advanced'],
					},
				},
			},

			// On Conflict Columns for Upsert
			{
				displayName: 'On Conflict Columns',
				name: 'onConflict',
				type: 'string',
				default: '',
				placeholder: 'id or company_id,name',
				description: 'Comma-separated column(s) to use for conflict resolution. Required for composite unique constraints (e.g. company_id,name).',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['upsert'],
					},
				},
			},

			// Match Column for Update (identifies which row to update)
			{
				displayName: 'Match Column',
				name: 'matchColumn',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getColumns',
				},
				required: true,
				default: '',
				description: 'Column used to match rows for updating (typically the primary key). Each input item must include this column in its data. Uses upsert internally, so rows will be created if no match is found.',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['update'],
					},
				},
			},

			// Find or Create - Match Columns
			{
				displayName: 'Match Columns',
				name: 'matchColumns',
				type: 'fixedCollection',
				typeOptions: {
					multipleValues: true,
				},
				default: {},
				placeholder: 'Add Match Column',
				description: 'Columns used to look up an existing row. If a row matches all values, it is returned; otherwise a new row is created.',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['findOrCreate'],
					},
				},
				options: [
					{
						displayName: 'Column',
						name: 'column',
						values: [
							{
								displayName: 'Column',
								name: 'name',
								type: 'options',
								typeOptions: {
									loadOptionsMethod: 'getColumns',
								},
								default: '',
								description: 'Column to match on',
							},
							{
								displayName: 'Value',
								name: 'value',
								type: 'string',
								default: '',
								description: 'Value to match',
							},
						],
					},
				],
			},

			// Find or Create - Additional Columns (only written on create)
			{
				displayName: 'Additional Columns',
				name: 'additionalColumns',
				type: 'fixedCollection',
				typeOptions: {
					multipleValues: true,
				},
				default: {},
				placeholder: 'Add Column',
				description: 'Extra columns to set when creating a new row (not used when a match is found)',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['findOrCreate'],
					},
				},
				options: [
					{
						displayName: 'Column',
						name: 'column',
						values: [
							{
								displayName: 'Column',
								name: 'name',
								type: 'options',
								typeOptions: {
									loadOptionsMethod: 'getColumns',
								},
								default: '',
								description: 'Column name',
							},
							{
								displayName: 'Value',
								name: 'value',
								type: 'string',
								default: '',
								description: 'Value to set',
							},
						],
					},
				],
			},

			// Filters for Read/Delete operations
			{
				displayName: 'Filters',
				name: 'filters',
				type: 'fixedCollection',
				typeOptions: {
					multipleValues: true,
				},
				default: {},
				placeholder: 'Add Filter',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['read', 'delete', 'updateByQuery'],
						uiMode: ['simple'],
					},
				},
				options: [
					{
						displayName: 'Filter',
						name: 'filter',
						values: [
							{
								displayName: 'Column',
								name: 'column',
								type: 'options',
								typeOptions: {
									loadOptionsMethod: 'getColumns',
								},
								default: '',
								description: 'Column to filter on',
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
								description: 'Filter operator',
							},
							{
								displayName: 'Value',
								name: 'value',
								type: 'string',
								default: '',
								description: 'Value to filter by',
							},
						],
					},
				],
			},

			// Advanced Filters (JSON)
			{
				displayName: 'Advanced Filters',
				name: 'advancedFilters',
				type: 'json',
				default: '{}',
				description: 'JSON object containing advanced filters',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['read', 'delete', 'updateByQuery'],
						uiMode: ['advanced'],
					},
				},
			},

			// Return Fields
			{
				displayName: 'Return Fields',
				name: 'returnFields',
				type: 'string',
				default: '*',
				placeholder: 'id,name,email',
				description: 'Comma-separated list of fields to return (* for all)',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['read'],
					},
				},
			},

			// Joins
			{
				displayName: 'Joins',
				name: 'joins',
				type: 'fixedCollection',
				typeOptions: {
					multipleValues: true,
				},
				default: {},
				placeholder: 'Add Join',
				description: 'Join related tables via foreign keys (PostgREST resource embedding)',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['read'],
					},
				},
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
								placeholder: 'company_email',
								description: 'Related table to join',
							},
							{
								displayName: 'Columns',
								name: 'columns',
								type: 'string',
								default: '*',
								placeholder: 'email,verified',
								description: 'Comma-separated columns to select from the joined table (* for all)',
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
								description: 'Left returns all rows even without a match; Inner only returns rows with a match',
							},
						],
					},
				],
			},

			// Sorting
			{
				displayName: 'Sort',
				name: 'sort',
				type: 'fixedCollection',
				typeOptions: {
					multipleValues: true,
				},
				default: {},
				placeholder: 'Add Sort Field',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['read'],
					},
				},
				options: [
					{
						displayName: 'Sort Field',
						name: 'sortField',
						values: [
							{
								displayName: 'Column',
								name: 'column',
								type: 'options',
								typeOptions: {
									loadOptionsMethod: 'getColumns',
								},
								default: '',
								description: 'Column to sort by',
							},
							{
								displayName: 'Ascending',
								name: 'ascending',
								type: 'boolean',
								default: true,
								description: 'Whether to sort in ascending order',
							},
						],
					},
				],
			},

			// Pagination
			{
				displayName: 'Return All',
				name: 'returnAll',
				type: 'boolean',
				default: false,
				description: 'Whether to return all results or limit the number of results',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['read'],
					},
				},
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				default: 100,
				description: 'Maximum number of rows to return',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['read'],
						returnAll: [false],
					},
				},
			},
			{
				displayName: 'Offset',
				name: 'offset',
				type: 'number',
				default: 0,
				description: 'Number of rows to skip',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['read'],
						returnAll: [false],
					},
				},
			},
			{
				displayName: 'Single Result Item',
				name: 'singleItem',
				type: 'boolean',
				default: false,
				description: 'Whether to return all rows wrapped in a single n8n item (as a "data" array) instead of one item per row',
				displayOptions: {
					show: {
						resource: ['database'],
						operation: ['read'],
					},
				},
			},

			// Storage Bucket Name
			{
				displayName: 'Bucket',
				name: 'bucket',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getBuckets',
				},
				required: true,
				default: '',
				placeholder: 'Select a bucket or enter manually',
				description: 'Name of the storage bucket',
				displayOptions: {
					show: {
						resource: ['storage'],
						operation: ['uploadFile', 'downloadFile', 'listFiles', 'deleteFile', 'moveFile', 'copyFile', 'getFileInfo', 'generateSignedUrl'],
					},
				},
			},

			// File Upload Options
			{
				displayName: 'Input Type',
				name: 'inputType',
				type: 'options',
				options: [
					{
						name: 'Binary Data',
						value: 'binary',
						description: 'File from previous node',
					},
					{
						name: 'URL',
						value: 'url',
						description: 'Download from URL',
					},
					{
						name: 'Text Content',
						value: 'text',
						description: 'Text content as file',
					},
				],
				default: 'binary',
				displayOptions: {
					show: {
						resource: ['storage'],
						operation: ['uploadFile'],
					},
				},
			},

			{
				displayName: 'File Name',
				name: 'fileName',
				type: 'string',
				required: true,
				default: '',
				placeholder: 'document.pdf',
				description: 'Name of the file to upload',
				displayOptions: {
					show: {
						resource: ['storage'],
						operation: ['uploadFile'],
					},
				},
			},

			{
				displayName: 'Binary Property Name',
				name: 'binaryPropertyName',
				type: 'string',
				default: 'data',
				description: 'Name of the binary property containing the file data',
				displayOptions: {
					show: {
						resource: ['storage'],
						operation: ['uploadFile'],
						inputType: ['binary'],
					},
				},
			},

			{
				displayName: 'File URL',
				name: 'fileUrl',
				type: 'string',
				default: '',
				placeholder: 'https://example.com/file.pdf',
				description: 'URL of the file to download and upload',
				displayOptions: {
					show: {
						resource: ['storage'],
						operation: ['uploadFile'],
						inputType: ['url'],
					},
				},
			},

			{
				displayName: 'Text Content',
				name: 'textContent',
				type: 'string',
				typeOptions: {
					rows: 4,
				},
				default: '',
				description: 'Text content to save as file',
				displayOptions: {
					show: {
						resource: ['storage'],
						operation: ['uploadFile'],
						inputType: ['text'],
					},
				},
			},

			// File Path
			{
				displayName: 'File Path',
				name: 'filePath',
				type: 'string',
				required: true,
				default: '',
				placeholder: 'folder/document.pdf',
				description: 'Path of the file in storage',
				displayOptions: {
					show: {
						resource: ['storage'],
						operation: ['downloadFile', 'getFileInfo', 'generateSignedUrl'],
					},
				},
			},

			// Output Format for Download
			{
				displayName: 'Output Format',
				name: 'outputFormat',
				type: 'options',
				options: [
					{
						name: 'Binary Data',
						value: 'binary',
						description: 'Return as binary data for further processing',
					},
					{
						name: 'Text Content',
						value: 'text',
						description: 'Return as text content',
					},
				],
				default: 'binary',
				displayOptions: {
					show: {
						resource: ['storage'],
						operation: ['downloadFile'],
					},
				},
			},

			// Bucket Management
			{
				displayName: 'Bucket Name',
				name: 'bucketName',
				type: 'string',
				required: true,
				default: '',
				placeholder: 'my-new-bucket',
				description: 'Name of the bucket',
				displayOptions: {
					show: {
						resource: ['storage'],
						operation: ['createBucket', 'deleteBucket', 'getBucketDetails'],
					},
				},
			},

			{
				displayName: 'Public Bucket',
				name: 'isPublic',
				type: 'boolean',
				default: false,
				description: 'Whether the bucket should be publicly accessible',
				displayOptions: {
					show: {
						resource: ['storage'],
						operation: ['createBucket'],
					},
				},
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		console.log(`[Supabase] execute: ${items.length} input items`);

		// Get credentials
		const credentials = await this.getCredentials('supabaseExtendedApi') as unknown as ISupabaseCredentials;

		// Validate credentials
		try {
			validateCredentials(credentials);
		} catch (error) {
			const errorMessage = error instanceof Error ? error.message : 'Unknown error';
			throw new NodeOperationError(this.getNode(), `Invalid credentials: ${errorMessage}`);
		}

		// Create Supabase client
		const supabase = createSupabaseClient(credentials);

		// resource and operation are node-level (noDataExpression), same for all items
		const resource = this.getNodeParameter('resource', 0) as SupabaseResource;
		const operation = this.getNodeParameter('operation', 0) as DatabaseOperation | StorageOperation;

		// Bulk database operations: collect all items, single API call
		// Skip if input is a single empty item (no data to write)
		if (resource === 'database' && ['create', 'upsert', 'update'].includes(operation as string)) {
			const firstItem = items[0];
			if (
				items.length === 1 &&
				firstItem?.json &&
				Object.keys(firstItem.json).length === 0
			) {
				return [[{ json: {} }]];
			}
			try {
				const results = await executeBulkDatabaseOperation.call(
					this,
					supabase,
					operation as DatabaseOperation,
					items.length,
				);
				for (const r of results) returnData.push(r);
			} catch (error) {
				const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
				if (this.continueOnFail()) {
					returnData.push({ json: { error: errorMessage } });
				} else {
					throw new NodeOperationError(this.getNode(), errorMessage);
				}
			}
		} else if (resource === 'database' && operation === 'read') {
			// Read operations execute once (first item), not per-item
			try {
				const operationResults = await executeDatabaseOperation.call(
					this,
					supabase,
					operation as DatabaseOperation,
					0,
					credentials.host,
				);
				for (const r of operationResults) returnData.push(r);
			} catch (error) {
				const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
				if (this.continueOnFail()) {
					returnData.push({ json: { error: errorMessage } });
				} else {
					throw new NodeOperationError(this.getNode(), errorMessage);
				}
			}
		} else {
			// Per-item processing for delete, schema ops, storage, etc.
			for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
				try {
					let operationResults: INodeExecutionData[] = [];

					if (resource === 'database') {
						operationResults = await executeDatabaseOperation.call(
							this,
							supabase,
							operation as DatabaseOperation,
							itemIndex,
							credentials.host,
						);
					} else if (resource === 'storage') {
						operationResults = await executeStorageOperation.call(
							this,
							supabase,
							operation as StorageOperation,
							itemIndex,
						);
					} else {
						throw new NodeOperationError(this.getNode(), `Unknown resource: ${resource}`);
					}

					for (const r of operationResults) returnData.push(r);

				} catch (error) {
					const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
					if (this.continueOnFail()) {
						returnData.push({
							json: {
								error: errorMessage,
								itemIndex,
							},
						});
						continue;
					}
					throw new NodeOperationError(this.getNode(), errorMessage, { itemIndex });
				}
			}
		}

		return [returnData];
	}

	methods = {
		loadOptions: {
			// Load available buckets from Supabase Storage
			async getBuckets(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const credentials = await this.getCredentials('supabaseExtendedApi') as unknown as ISupabaseCredentials;
				
				try {
					validateCredentials(credentials);
					const supabase = createSupabaseClient(credentials);
					
					const { data, error } = await supabase.storage.listBuckets();
					
					if (error) {
						throw new Error(`Failed to fetch buckets: ${error.message}`);
					}
					
					if (!Array.isArray(data)) {
						return [
							{
								name: 'No buckets found',
								value: '',
								description: 'Create a bucket first or check your permissions',
							},
						];
					}
					
					const options: INodePropertyOptions[] = data.map((bucket: any) => ({
						name: `${bucket.name}${bucket.public ? ' (Public)' : ' (Private)'}`,
						value: bucket.name,
						description: `Created: ${bucket.created_at ? new Date(bucket.created_at).toLocaleDateString() : 'Unknown'}`,
					}));
					
					// Sort buckets alphabetically
					options.sort((a, b) => a.name.localeCompare(b.name));
					
					return options;
					
				} catch (error) {
					const errorMessage = error instanceof Error ? error.message : 'Unknown error';
					return [
						{
							name: `Error: ${errorMessage}`,
							value: '',
							description: 'Failed to load buckets. Please check your credentials and try again.',
						},
					];
				}
			},

			// Load available tables from database via PostgREST OpenAPI spec
			async getTables(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const credentials = await this.getCredentials('supabaseExtendedApi') as unknown as ISupabaseCredentials;
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
						return [{ name: 'No tables found', value: '', description: 'No tables are exposed via the REST API' }];
					}
					return tables.map((tableName: string) => ({ name: tableName, value: tableName }));
				} catch (error) {
					const errorMessage = error instanceof Error ? error.message : 'Unknown error';
					return [{ name: `Error: ${errorMessage}`, value: '', description: 'Failed to load tables. Check your credentials.' }];
				}
			},

			// Load available columns from selected table via PostgREST OpenAPI spec
			async getColumns(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const credentials = await this.getCredentials('supabaseExtendedApi') as unknown as ISupabaseCredentials;
				validateCredentials(credentials);
				const table = this.getCurrentNodeParameter('table') as string;
				if (!table) {
					return [{ name: 'Select a table first', value: '', description: 'Choose a table to load its columns' }];
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
					const definitions = response.definitions || {};
					const tableSchema = definitions[table];
					if (!tableSchema?.properties) {
						return [{ name: 'No columns found', value: '', description: `Table "${table}" not found or has no columns` }];
					}
					return Object.keys(tableSchema.properties).sort().map((col: string) => {
						const colDef = tableSchema.properties[col];
						const typeLabel = colDef.format ? `${colDef.type} (${colDef.format})` : colDef.type;
						return { name: col, value: col, description: typeLabel };
					});
				} catch (error) {
					const errorMessage = error instanceof Error ? error.message : 'Unknown error';
					return [{ name: `Error: ${errorMessage}`, value: '', description: 'Failed to load columns. Check your credentials.' }];
				}
			},
		},
	};
}
