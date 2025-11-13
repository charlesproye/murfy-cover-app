import { SelectedFilter } from '@/interfaces/common/optionSelector/DataFilter';
import {
  FleetBrandData,
  GlobalScatterRequestSwr,
} from '@/interfaces/dashboard/global/GlobalDashboard';
import { ROUTES } from '@/routes';

import fetchWithAuth from '@/services/fetchWithAuth';
import { useEffect, useState } from 'react';
import useSWR from 'swr';

const useGetScatterGlobal = (filters: SelectedFilter[]): GlobalScatterRequestSwr => {
  const [fleetSelected, setFleetSelected] = useState(false);

  const pinnedFilter = filters.find((filter) => filter.key === 'pinned');
  const isPinnedOnly = pinnedFilter?.values.includes('true');

  const queryParams = filters
    .filter((filter) => filter.key !== 'pinned')
    .map((filter) => `${filter.key}=${filter.values.join(',')}`)
    .join('&');

  useEffect(() => {
    const hasFleetFilter = filters.some(
      (filter) => filter.key === 'Fleets' && filter.values.length > 0, // vérifie si Fleets existe
    );
    setFleetSelected(hasFleetFilter);
  }, [filters]);

  const fleetFilter = filters.find((f) => f.key === 'Fleets');

  const { data, isLoading, error } = useSWR(
    fleetSelected && fleetFilter && fleetFilter.values && fleetFilter.values.length > 0
      ? `${ROUTES.GLOBAL_SCATTER_BRANDS}?${queryParams}${isPinnedOnly ? `${queryParams ? '&' : '?'}pinned_vehicles=true` : ''}`
      : null,
    fetchWithAuth<FleetBrandData>,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, fleetSelected, isLoading, error };
};

export default useGetScatterGlobal;
