import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';
import { useState, useEffect } from 'react';
import { NewVehicles } from '@/interfaces/dashboard/home/response';

const useGetNewVehicles = (
  fleet: string | null,
  selected: 'Monthly' | 'Annually',
): {
  data: NewVehicles[];
  isLoading: boolean;
  error: unknown;
} => {
  const [chartData, setChartData] = useState<NewVehicles[]>([]);

  const fetcherNewVehicles = (url: string): Promise<NewVehicles[]> => {
    return fetchWithAuth(url);
  };

  const { data, isLoading, error } = useSWR(
    `${ROUTES.NEW_VEHICLES}?fleet_id=${fleet}&period=${selected.toLowerCase()}`,
    fetcherNewVehicles,
    {
      revalidateOnFocus: false,
    },
  );

  useEffect(() => {
    if (data) {
      setChartData(
        data.map((item: NewVehicles) => ({
          period: item?.period,
          vehicle_count: item?.vehicle_count,
        })),
      );
    }
  }, [data]);

  return { data: chartData, isLoading, error };
};

export default useGetNewVehicles;
