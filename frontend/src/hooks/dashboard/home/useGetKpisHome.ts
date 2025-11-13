import { ROUTES } from '@/routes';
import { DataCardResult } from '@/interfaces/common/DataCard';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';
import { Fleet } from '@/interfaces/auth/auth';
import { ApiGetRequestSwr } from '@/interfaces/common/api';

const useGetKpisHome = (fleet: Fleet | null): ApiGetRequestSwr<DataCardResult> => {
  const { data, isLoading, error } = useSWR(
    `${ROUTES.KPIHOME}?fleet_id=${fleet?.id}`,
    fetchWithAuth<DataCardResult[]>,
    {
      revalidateOnFocus: false,
    },
  );
  return { data, isLoading, error };
};

export default useGetKpisHome;
