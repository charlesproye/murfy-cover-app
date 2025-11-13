import {
  PriceForecastRequestSwr,
  PriceForecastResult,
} from '@/interfaces/dashboard/passport/infoVehicle';
import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetPriceForecast = (vin: string | undefined): PriceForecastRequestSwr => {
  const { data, isLoading, error } = useSWR(
    vin ? `${ROUTES.PRICE_FORECAST}/${vin}` : null,
    fetchWithAuth<PriceForecastResult>,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, isLoading, error };
};

export default useGetPriceForecast;
