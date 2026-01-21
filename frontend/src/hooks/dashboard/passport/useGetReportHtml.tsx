import { ROUTES } from '@/routes';
import { attemptRefresh, getIsLoggingOut } from '@/services/auth/authState';
import { logoutRequest } from '@/services/auth/authService';
import { toast } from 'sonner';
import useSWR from 'swr';

export class ApiError extends Error {
  status: number;
  detail: string;

  constructor(status: number, detail: string) {
    super(detail);
    this.status = status;
    this.detail = detail;
  }
}

interface ReportHtmlResult {
  htmlContent: string | undefined;
  isLoading: boolean;
  error: ApiError | null;
}

const fetchReportHtml = async (url: string): Promise<string> => {
  const makeRequest = async (): Promise<Response> => {
    return fetch(url, {
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
    });
  };

  let response = await makeRequest();

  // Handle token refresh on 401/403 (same logic as fetchWithAuth)
  if (response.status === 401 || response.status === 403) {
    const refreshResponse = await attemptRefresh();

    if (refreshResponse.ok) {
      // Retry the original request
      response = await makeRequest();
    } else {
      // Only logout once, even if multiple requests fail
      if (!getIsLoggingOut()) {
        await logoutRequest();
        console.error('Authentication failed - please login again');
        toast.error('Authentication failed - please login again');
      }
      throw new Error('Authentication failed - please login again');
    }
  }

  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = await response.json();
      if (body.detail) {
        detail = body.detail;
      }
    } catch {
      // Use default statusText if body parsing fails
    }
    throw new ApiError(response.status, detail);
  }

  const contentType = response.headers.get('content-type');
  if (contentType?.includes('application/json')) {
    return response.json();
  }
  return response.text();
};

const useGetReportHtml = (
  vin: string | undefined,
  reportType: 'premium' | 'readout' = 'premium',
): ReportHtmlResult => {
  const baseRoute =
    reportType === 'readout' ? ROUTES.READOUT_REPORT_HTML : ROUTES.PREMIUM_REPORT_HTML;

  const { data, isLoading, error } = useSWR<string, ApiError>(
    vin ? `${baseRoute}/${vin}/report_html` : null,
    fetchReportHtml,
    {
      revalidateOnFocus: false,
      shouldRetryOnError: false,
      dedupingInterval: 5000,
    },
  );

  return { htmlContent: data, isLoading, error: error ?? null };
};

export default useGetReportHtml;
