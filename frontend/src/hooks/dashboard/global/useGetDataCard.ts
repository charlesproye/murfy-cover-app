import { ApiGetRequestSwr } from '@/interfaces/common/api';
import { DataCardResult } from '@/interfaces/common/DataCard';
import { SelectedFilter } from '@/interfaces/common/optionSelector/DataFilter';
import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetDataCard = (filters: SelectedFilter[]): ApiGetRequestSwr<DataCardResult> => {
  const pinnedFilter = filters.find((filter) => filter.key === 'pinned');
  const isPinnedOnly = pinnedFilter?.values.includes('true');

  const queryParams = filters
    .filter((filter) => filter.key !== 'pinned')
    .map((filter) => `${filter.key}=${filter.values.join(',')}`)
    .join('&');

  const url = `${ROUTES.KPI}${queryParams ? `?${queryParams}` : ''}${isPinnedOnly ? `${queryParams ? '&' : '?'}pinned_vehicles=true` : ''}`;

  const { data, isLoading, error } = useSWR<DataCardResult[]>(
    url,
    fetchWithAuth<DataCardResult[]>,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, isLoading, error };
};

export default useGetDataCard;
