import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';
import { useState, useEffect } from 'react';
import { RangeSoh } from '@/interfaces/dashboard/home/response';

const useGetSohRange = (
  fleet: string | null,
  selected: 'SoH' | 'Mileage',
): {
  data: RangeSoh[];
  isLoading: boolean;
  error: unknown;
} => {
  const [chartData, setChartData] = useState<RangeSoh[]>([]);

  const fetcherSohRange = (url: string): Promise<RangeSoh[]> => {
    return fetchWithAuth(url);
  };

  const { data, isLoading, error } = useSWR(
    `${ROUTES.SOHRANGE}?fleet_id=${fleet}&type=${selected.toLowerCase()}`,
    fetcherSohRange,
    {
      revalidateOnFocus: false,
    },
  );

  useEffect(() => {
    if (data) {
      setChartData(
        data.map((item: RangeSoh) => ({
          lower_bound: item?.lower_bound,
          upper_bound: item?.upper_bound,
          vehicle_count: item?.vehicle_count,
        })),
      );
    }
  }, [data]);

  return { data: chartData, isLoading, error };
};

export default useGetSohRange;
