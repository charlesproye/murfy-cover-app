import { SelectedFilter } from '@/interfaces/common/optionSelector/DataFilter';
import {
  FleetBrandData,
  GlobalRegionRequestSwr,
} from '@/interfaces/dashboard/global/GlobalDashboard';
import { ROUTES } from '@/routes';

import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetRegionGlobal = (
  filters: SelectedFilter[],
  fleetSelected: boolean,
): GlobalRegionRequestSwr => {
  const fleetFilter = filters.find((f) => f.key === 'Fleets');
  const pinnedFilter = filters.find((filter) => filter.key === 'pinned');
  const isPinnedOnly = pinnedFilter?.values.includes('true');

  const queryParams = filters
    .filter((filter) => filter.key !== 'pinned')
    .map((filter) => `${filter.key}=${filter.values.join(',')}`)
    .join('&');

  const { data, isLoading, error } = useSWR(
    fleetSelected && fleetFilter && fleetFilter.values && fleetFilter.values.length > 0
      ? `${ROUTES.GLOBAL_SCATTER_REGION}?${queryParams}${isPinnedOnly ? `${queryParams ? '&' : '?'}pinned_vehicles=true` : ''}`
      : null,
    fetchWithAuth<FleetBrandData>,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, isLoading, error };
};

export default useGetRegionGlobal;
