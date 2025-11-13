import { DataPdfRequestSwr, DataPdfResult } from '@/interfaces/pdf/DataPdf';
import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetDataPdf = (vin: string | undefined): DataPdfRequestSwr => {
  const filterFetcher = async (url: string): Promise<DataPdfResult> => {
    return fetchWithAuth(url);
  };

  const { data, isLoading, error } = useSWR<DataPdfResult>(
    `${ROUTES.PDF}/${vin}`,
    filterFetcher,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, isLoading, error };
};

export default useGetDataPdf;
