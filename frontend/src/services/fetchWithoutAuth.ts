import { FetchOptions } from './fetchWithAuth';

export const fetchWithoutAuth = async <T>(
  url: string,
  options: FetchOptions = {},
): Promise<T> => {
  const defaultOptions: RequestInit = {
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  };

  // Handle body serialization
  if (options.body && !(options.body instanceof FormData)) {
    if (typeof options.body === 'object') {
      defaultOptions.body = JSON.stringify(options.body);
    } else {
      defaultOptions.body = options.body;
    }
  } else if (options.body instanceof FormData) {
    // Remove Content-Type header for FormData
    delete (defaultOptions.headers as Record<string, string>)['Content-Type'];
    defaultOptions.body = options.body;
  }

  const response: Response = await fetch(url, { ...defaultOptions, ...options });

  if (response.status === 200) {
    return (await response.json()) as T;
  } else {
    throw new Error('Error fetching data');
  }
};
