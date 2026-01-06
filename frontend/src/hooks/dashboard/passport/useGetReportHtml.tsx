import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

interface ReportHtmlResult {
  htmlContent: string | undefined;
  isLoading: boolean;
  error: unknown;
}

const useGetReportHtml = (vin: string | undefined): ReportHtmlResult => {
  const { data, isLoading, error } = useSWR(
    vin ? `${ROUTES.PREMIUM_REPORT_HTML}/${vin}/report_html` : null,
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
