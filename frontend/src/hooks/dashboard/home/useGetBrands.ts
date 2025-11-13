import { ROUTES } from '@/routes';
import {
  DashboardDataTrendline,
  ResponseTrendline,
} from '@/interfaces/dashboard/home/ResponseApi';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetBrands = (fleet: string | null): ResponseTrendline => {
  const { data, isLoading, error } = useSWR(
    `${ROUTES.BRANDS}?fleet_id=${fleet}`,
    fetchWithAuth<DashboardDataTrendline>,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, isLoading, error };
};

export default useGetBrands;
