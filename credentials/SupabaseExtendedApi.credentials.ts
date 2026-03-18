import {
	IAuthenticateGeneric,
	ICredentialTestRequest,
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class SupabaseExtendedApi implements ICredentialType {
	name = 'supabaseExtendedApi';

	displayName = 'Supabase Extended API';

	documentationUrl = 'https://supabase.com/docs/guides/api';

	properties: INodeProperties[] = [
		{
			displayName: 'Host',
			name: 'host',
			type: 'string',
			default: '',
			placeholder: 'https://your-project.supabase.co',
			required: true,
			description: 'Your Supabase project URL',
		},
		{
			displayName: 'Secret Key',
			name: 'serviceKey',
			type: 'string',
			default: '',
			placeholder: 'sb_secret_...',
			typeOptions: {
				password: true,
			},
			required: true,
			description: 'Your Supabase Secret key (sb_secret_... from Project Settings → API Keys). Legacy service_role JWTs are also supported.',
		},
		{
			displayName: 'Additional Options',
			name: 'additionalOptions',
			type: 'collection',
			placeholder: 'Add Option',
			default: {},
			options: [
				{
					displayName: 'Schema',
					name: 'schema',
					type: 'string',
					default: 'public',
					description: 'Database schema to use (default: public)',
				},
			],
		},
	];

	authenticate: IAuthenticateGeneric = {
		type: 'generic',
		properties: {
			headers: {
				apikey: '={{$credentials.serviceKey}}',
			},
		},
	};

	test: ICredentialTestRequest = {
		request: {
			baseURL: '={{$credentials.host}}',
			url: '/rest/v1/',
			method: 'GET',
			headers: {
				apikey: '={{$credentials.serviceKey}}',
				'Accept': 'application/vnd.pgrst.object+json',
				'Content-Type': 'application/json',
			},
		},
	};

}
