import { FetchOptions } from '@/services/fetchWithAuth';

export const fetchWithoutAuth = async <T>(
  url: string,
  options: FetchOptions = {},
): Promise<T> => {
  const { body, ...restOptions } = options;
  const defaultOptions: RequestInit = {
    headers: {
      'Content-Type': 'application/json',
      ...restOptions.headers,
    },
  };

  // Handle body serialization
  if (body && !(body instanceof FormData)) {
    if (typeof body === 'object') {
      defaultOptions.body = JSON.stringify(body);
    } else {
      defaultOptions.body = body;
    }
  } else if (body instanceof FormData) {
    // Remove Content-Type header for FormData
    delete (defaultOptions.headers as Record<string, string>)['Content-Type'];
    defaultOptions.body = body;
  }

  const response: Response = await fetch(url, { ...defaultOptions, ...restOptions });

  if (response.status === 200) {
    return (await response.json()) as T;
  } else {
    throw new Error('Error fetching data');
  }
};
