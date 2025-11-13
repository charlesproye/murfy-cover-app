import { ROUTES } from '@/routes';
import { SearchVehicleResult } from '@/interfaces/common/SearchVehicleResult';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetSearchVehicle = (
  input: string | undefined,
): {
  data: SearchVehicleResult[] | undefined;
  isLoading: boolean;
  error: unknown;
} => {
  const { data, isLoading, error } = useSWR(
    input && input.length > 0 ? `${ROUTES.SEARCH_VEHICLE}/${input}` : null,
    fetchWithAuth<SearchVehicleResult[]>,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, isLoading, error };
};

export default useGetSearchVehicle;
