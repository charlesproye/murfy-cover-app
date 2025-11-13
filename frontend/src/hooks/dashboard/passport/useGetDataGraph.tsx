import { ROUTES } from '@/routes';
import {
  DataGraphRequestSwr,
  DataGraphResponse,
} from '@/interfaces/dashboard/passport/DataGraph';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetDataGraph = (
  vin: string | undefined,
  period: string,
): DataGraphRequestSwr => {
  const { data, isLoading, error } = useSWR(
    `${ROUTES.GRAPHS}/${vin}?period=${period}`,
    fetchWithAuth<DataGraphResponse>,
    {
      revalidateOnFocus: false,
    },
  );
  return { data, isLoading, error };
};

export default useGetDataGraph;
