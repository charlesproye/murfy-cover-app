import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

interface ReportHtmlResult {
  htmlContent: string | undefined;
  isLoading: boolean;
  error: unknown;
}

const useGetReportHtml = (
  vin: string | undefined,
  reportType: 'premium' | 'readout' = 'premium',
): ReportHtmlResult => {
  const baseRoute = reportType === 'readout' ? ROUTES.READOUT_REPORT_HTML : ROUTES.PREMIUM_REPORT_HTML;

  const { data, isLoading, error } = useSWR(
    vin ? `${baseRoute}/${vin}/report_html` : null,
    fetchWithAuth<string>,
    {
      revalidateOnFocus: false,
      shouldRetryOnError: false,
      dedupingInterval: 5000,
    },
  );

  return { htmlContent: data, isLoading, error };
};

export default useGetReportHtml;
