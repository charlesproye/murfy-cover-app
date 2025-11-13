import { ROUTES } from '@/routes';
import {
  DashboardDataTrendline,
  ResponseTrendline,
} from '@/interfaces/dashboard/home/ResponseApi';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetTrendlineBrands = (
  fleet: string | null,
  selectedBrand: string,
): ResponseTrendline => {
  const { data, isLoading, error } = useSWR(
    `${ROUTES.TRENDLINE_BRANDS}?fleet_id=${fleet ?? ''}&brand=${selectedBrand}`,
    fetchWithAuth<DashboardDataTrendline>,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, isLoading, error };
};

export default useGetTrendlineBrands;
