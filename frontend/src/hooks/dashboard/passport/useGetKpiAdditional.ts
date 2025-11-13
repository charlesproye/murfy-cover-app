import {
  KpiAdditionalRequestSwr,
  KpiAdditionalResult,
} from '@/interfaces/dashboard/passport/infoVehicle';
import { ROUTES } from '@/routes';

import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetKpiAdditional = (vin: string | undefined): KpiAdditionalRequestSwr => {
  const { data, isLoading, error } = useSWR(
    `${ROUTES.KPIS_ADDITIONAL}/${vin}`,
    fetchWithAuth<KpiAdditionalResult>,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, isLoading, error };
};

export default useGetKpiAdditional;
