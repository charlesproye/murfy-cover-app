import { ROUTES } from '@/routes';
import { TableFleetResult } from '@/interfaces/dashboard/home/table/TableFleetResult';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetTableFleet = (
  fleet: string | null,
  label: string | null,
  currentPage: number,
): {
  data?: TableFleetResult;
  isLoading: boolean;
  error: unknown;
  hasMorePages: boolean;
} => {
  const { data, isLoading, error } = useSWR(
    `${ROUTES.SOH_BY_GROUPS}?fleet_id=${fleet}&group=${label}&page=${currentPage}`,
    fetchWithAuth<TableFleetResult>,
    {
      revalidateOnFocus: false,
    },
  );
  const hasMorePages = data?.pagination
    ? data.pagination.current_page < data.pagination.total_pages
    : false;
  return { data, isLoading, error, hasMorePages };
};

export default useGetTableFleet;
