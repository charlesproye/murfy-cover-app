import { ROUTES } from '@/routes';
import {
  DataCardGraphRequestSwr,
  DataCardGraphResult,
} from '@/interfaces/dashboard/passport/DataGraph';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetDataCardGraph = (vin: string | undefined): DataCardGraphRequestSwr => {
  const { data, isLoading, error } = useSWR(
    `${ROUTES.KPIGRAPH}/${vin}`,
    fetchWithAuth<DataCardGraphResult[]>,
    {
      revalidateOnFocus: false,
    },
  );
  return { data, isLoading, error };
};

export default useGetDataCardGraph;
