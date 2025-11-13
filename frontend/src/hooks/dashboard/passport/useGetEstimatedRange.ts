import {
  EstimatedRangeRequestSwr,
  EstimatedRangeResult,
} from '@/interfaces/dashboard/passport/infoVehicle';
import { ROUTES } from '@/routes';

import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetEstimatedRange = (vin: string | undefined): EstimatedRangeRequestSwr => {
  const { data, isLoading, error } = useSWR(
    `${ROUTES.ESTIMATED_RANGE}/${vin}`,
    fetchWithAuth<EstimatedRangeResult>,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, isLoading, error };
};

export default useGetEstimatedRange;
