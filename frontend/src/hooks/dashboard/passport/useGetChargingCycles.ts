import useSWR from 'swr';
import {
  ChargingCyclesData,
  ChargingCyclesRequestSwr,
} from '@/interfaces/dashboard/passport/ChargingCycles';
import fetchWithAuth from '@/services/fetchWithAuth';
import { ROUTES } from '@/routes';

const useGetChargingCycles = (
  vin: string | undefined,
  formatDate: string,
): ChargingCyclesRequestSwr => {
  const { data, isLoading, error } = useSWR(
    vin ? `${ROUTES.CHARGING_CYCLES}/${vin}?format_date=${formatDate}` : null,
    fetchWithAuth<ChargingCyclesData>,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, isLoading, error };
};

export default useGetChargingCycles;
