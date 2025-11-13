import { ApiGetRequestSwr } from '@/interfaces/common/api';
import { SelectedFilter } from '@/interfaces/common/optionSelector/DataFilter';
import { GlobalDashboardResult } from '@/interfaces/dashboard/global/GlobalDashboard';
import { ROUTES } from '@/routes';

import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetGlobalDashboard = (
  filters: SelectedFilter[],
): ApiGetRequestSwr<GlobalDashboardResult> => {
  const pinnedFilter = filters.find((filter) => filter.key === 'pinned');
  const isPinnedOnly = pinnedFilter?.values.includes('true');

  const queryParams = filters
    .filter((filter) => filter.key !== 'pinned')
    .map((filter) => `${filter.key}=${filter.values.join(',')}`)
    .join('&');

  const url = `${ROUTES.GLOBAL_DASHBOARD}${queryParams ? `?${queryParams}` : ''}${isPinnedOnly ? `${queryParams ? '&' : '?'}pinned_vehicles=true` : ''}`;

  const { data, isLoading, error } = useSWR(url, fetchWithAuth<GlobalDashboardResult[]>, {
    revalidateOnFocus: false,
  });
  return { data, isLoading, error };
};

export default useGetGlobalDashboard;
