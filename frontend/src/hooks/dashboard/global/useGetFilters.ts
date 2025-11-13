import {
  DataRequestFilter,
  FilterOption,
} from '@/interfaces/common/optionSelector/DataFilter';
import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

interface ApiFilterResponse {
  type: string;
  data: {
    id: string;
    name: string;
  };
}

const useGetFilters = (fleet: string | null): DataRequestFilter => {
  const filterFetcher = async (url: string): Promise<FilterOption[]> => {
    const response = (await fetchWithAuth(url)) as ApiFilterResponse[];

    // Grouper les données par type
    const groupedData = response.reduce(
      (acc: { [key: string]: FilterOption }, item: ApiFilterResponse) => {
        const type = item.type;
        if (!acc[type]) {
          acc[type] = {
            title: type.charAt(0).toUpperCase() + type.slice(1),
            data: [],
          };
        }
        acc[type].data.push({
          id: item.data.id,
          name: item.data.name,
          vehicle_model_id: item.data.id,
        });
        return acc;
      },
      {},
    );

    return Object.values(groupedData);
  };

  const { data, isLoading, error } = useSWR<FilterOption[]>(
    `${ROUTES.FILTERS}?fleet_id=${fleet}`,
    filterFetcher,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, isLoading, error };
};

export default useGetFilters;
