import {
  InfoVehicleRequestSwr,
  InfoVehicleResult,
} from '@/interfaces/dashboard/passport/infoVehicle';
import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';
import { useRouter } from 'next/navigation';

const useGetInfoVehicle = (vin: string | undefined): InfoVehicleRequestSwr => {
  const router = useRouter();
  const { data, isLoading, error } = useSWR(
    vin ? `${ROUTES.INFO_VEHICLE}/${vin}` : null,
    fetchWithAuth<InfoVehicleResult>,
    {
      revalidateOnFocus: false,
      shouldRetryOnError: false,
      dedupingInterval: 5000,
      onError: (err) => {
        if (err.status === 404) {
          return router.push('/dashboard');
        }
      },
    },
  );

  return { data, isLoading, error };
};

export default useGetInfoVehicle;
