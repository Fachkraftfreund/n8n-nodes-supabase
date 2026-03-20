import {
	IDataObject,
	ILoadOptionsFunctions,
	INodeExecutionData,
	INodePropertyOptions,
	INodeType,
	INodeTypeDescription,
	IPollFunctions,
} from 'n8n-workflow';
import { createSupabaseClient, validateCredentials } from './utils/supabaseClient';
import { ISupabaseCredentials } from './types';

export class SupabaseTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Supabase Trigger',
		name: 'supabaseExtendedTrigger',
		icon: 'file:icons/supabase.svg',
		group: ['trigger'],
		version: 1,
		subtitle: '={{$parameter["event"]}}',
		description: 'Triggers when rows are created or updated in a Supabase table',
		polling: true,
		defaults: {
			name: 'Supabase Trigger',
		},
		inputs: [],
		outputs: ['main'],
		credentials: [
			{
				name: 'supabaseExtendedApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Table',
				name: 'table',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getTables',
				},
				required: true,
				default: '',
				description: 'Table to watch for changes',
			},
			{
				displayName: 'Event',
				name: 'event',
				type: 'options',
				options: [
					{
						name: 'Row Created',
						value: 'rowCreated',
						description: 'Trigger when new rows are inserted',
					},
					{
						name: 'Row Updated',
						value: 'rowUpdated',
						description: 'Trigger when existing rows are updated',
					},
					{
						name: 'Row Created or Updated',
						value: 'rowCreatedOrUpdated',
						description: 'Trigger on both inserts and updates',
					},
				],
				default: 'rowCreated',
			},
			// Timestamp column for single event types
			{
				displayName: 'Timestamp Column',
				name: 'timestampColumn',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getColumns',
				},
				required: true,
				default: '',
				description: 'Column that tracks when the row was created or last updated',
				displayOptions: {
					show: {
						event: ['rowCreated', 'rowUpdated'],
					},
				},
			},
			// Separate columns for combined event
			{
				displayName: 'Created At Column',
				name: 'createdAtColumn',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getColumns',
				},
				required: true,
				default: '',
				description: 'Column that stores row creation time',
				displayOptions: {
					show: {
						event: ['rowCreatedOrUpdated'],
					},
				},
			},
			{
				displayName: 'Updated At Column',
				name: 'updatedAtColumn',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getColumns',
				},
				required: true,
				default: '',
				description: 'Column that stores row last-update time',
				displayOptions: {
					show: {
						event: ['rowCreatedOrUpdated'],
					},
				},
			},
			// Key column for deduplication and field tracking
			{
				displayName: 'Key Column',
				name: 'keyColumn',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getColumns',
				},
				required: true,
				default: '',
				description: 'Unique identifier column (e.g. id) used for deduplication and field-change tracking',
			},
			// Watched fields — multi-select of columns
			{
				displayName: 'Watched Fields',
				name: 'watchedFields',
				type: 'multiOptions',
				typeOptions: {
					loadOptionsMethod: 'getColumns',
				},
				default: [],
				description: 'Only trigger when these specific fields change. Leave empty to trigger on any change.',
			},
		],
	};

	async poll(this: IPollFunctions): Promise<INodeExecutionData[][] | null> {
		const credentials = await this.getCredentials('supabaseExtendedApi') as unknown as ISupabaseCredentials;
		validateCredentials(credentials);
		const supabase = createSupabaseClient(credentials);

		const table = this.getNodeParameter('table') as string;
		const event = this.getNodeParameter('event') as string;
		const keyColumn = this.getNodeParameter('keyColumn') as string;
		const watchedFields = this.getNodeParameter('watchedFields', []) as string[];

		const staticData = this.getWorkflowStaticData('node');

		// First poll: initialise state
		if (!staticData.lastTimestamp) {
			if (this.getMode() === 'manual') {
				// Manual testing — look back 24 h so the user sees data
				const lookback = new Date();
				lookback.setHours(lookback.getHours() - 24);
				staticData.lastTimestamp = lookback.toISOString();
			} else {
				// Trigger mode — start from now, don't flood with historical rows
				staticData.lastTimestamp = new Date().toISOString();
				staticData.fieldStates = {};
				return null;
			}
		}

		const lastTimestamp = staticData.lastTimestamp as string;

		// ----- build query -----
		let query = supabase.from(table).select('*');

		if (event === 'rowCreatedOrUpdated') {
			const createdAtCol = this.getNodeParameter('createdAtColumn') as string;
			const updatedAtCol = this.getNodeParameter('updatedAtColumn') as string;
			query = query.or(`${createdAtCol}.gt.${lastTimestamp},${updatedAtCol}.gt.${lastTimestamp}`);
		} else {
			const timestampCol = this.getNodeParameter('timestampColumn') as string;
			query = query.gt(timestampCol, lastTimestamp);
		}

		// Order ascending so the last element has the max timestamp
		const orderCol = event === 'rowCreatedOrUpdated'
			? (this.getNodeParameter('updatedAtColumn') as string)
			: (this.getNodeParameter('timestampColumn') as string);
		query = query.order(orderCol, { ascending: true }).limit(1000);

		const { data, error } = await query;

		if (error) {
			throw new Error(`Supabase trigger error: ${error.message}`);
		}

		if (!data || data.length === 0) {
			return null;
		}

		// ----- deduplicate by key column -----
		const seen = new Set<string>();
		const uniqueRows: IDataObject[] = [];
		for (const row of data) {
			const key = String(row[keyColumn]);
			if (!seen.has(key)) {
				seen.add(key);
				uniqueRows.push(row as IDataObject);
			}
		}

		// ----- advance lastTimestamp to the max across all fetched rows -----
		let maxTimestamp = lastTimestamp;
		for (const row of uniqueRows) {
			if (event === 'rowCreatedOrUpdated') {
				const createdAtCol = this.getNodeParameter('createdAtColumn') as string;
				const updatedAtCol = this.getNodeParameter('updatedAtColumn') as string;
				const c = (row[createdAtCol] as string) || '';
				const u = (row[updatedAtCol] as string) || '';
				if (c > maxTimestamp) maxTimestamp = c;
				if (u > maxTimestamp) maxTimestamp = u;
			} else {
				const timestampCol = this.getNodeParameter('timestampColumn') as string;
				const ts = (row[timestampCol] as string) || '';
				if (ts > maxTimestamp) maxTimestamp = ts;
			}
		}
		staticData.lastTimestamp = maxTimestamp;

		// ----- watched-fields filter -----
		let resultRows: IDataObject[] = uniqueRows;

		if (watchedFields.length > 0) {
			const fieldStates = (staticData.fieldStates || {}) as Record<string, Record<string, unknown>>;
			const changedRows: IDataObject[] = [];

			for (const row of uniqueRows) {
				const rowKey = String(row[keyColumn]);
				const prevState = fieldStates[rowKey];

				// Current values for watched fields
				const currentState: Record<string, unknown> = {};
				for (const field of watchedFields) {
					currentState[field] = row[field];
				}

				if (!prevState) {
					// First time seeing this row — emit it
					const changed: IDataObject = { ...row, _changedFields: watchedFields };
					changedRows.push(changed);
				} else {
					// Check which watched fields actually changed
					const changed = watchedFields.filter(
						(f) => JSON.stringify(prevState[f]) !== JSON.stringify(currentState[f]),
					);
					if (changed.length > 0) {
						changedRows.push({ ...row, _changedFields: changed });
					}
				}

				// Store current state
				fieldStates[rowKey] = currentState;
			}

			// Prune stored states to avoid unbounded growth (keep max 10 000)
			const keys = Object.keys(fieldStates);
			if (keys.length > 10_000) {
				const excess = keys.length - 10_000;
				for (let i = 0; i < excess; i++) {
					const key = keys[i];
					if (key !== undefined) {
						delete fieldStates[key];
					}
				}
			}

			staticData.fieldStates = fieldStates;
			resultRows = changedRows;
		}

		if (resultRows.length === 0) {
			return null;
		}

		return [resultRows.map((row) => ({ json: row }))];
	}

	methods = {
		loadOptions: {
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
					return tables.map((t: string) => ({ name: t, value: t }));
				} catch (error) {
					const msg = error instanceof Error ? error.message : 'Unknown error';
					return [{ name: `Error: ${msg}`, value: '', description: 'Failed to load tables. Check your credentials.' }];
				}
			},

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
					const msg = error instanceof Error ? error.message : 'Unknown error';
					return [{ name: `Error: ${msg}`, value: '', description: 'Failed to load columns. Check your credentials.' }];
				}
			},
		},
	};
}
