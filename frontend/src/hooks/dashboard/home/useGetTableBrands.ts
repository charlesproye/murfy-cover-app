import { ROUTES } from '@/routes';
import { TableBrandsResult } from '@/interfaces/dashboard/home/table/TablebrandResult';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

const useGetTableBrands = (
  fleet: string | null,
  selected: 'Make' | 'Region' | 'Both',
): {
  data: TableBrandsResult[] | undefined;
  isLoading: boolean;
  error: unknown;
} => {
  const { data, isLoading, error } = useSWR(
    `${ROUTES.TABLE_BRANDS}?fleet_id=${fleet}&filter=${selected}`,
    fetchWithAuth<TableBrandsResult[]>,
    {
      revalidateOnFocus: false,
    },
  );
  return { data, isLoading, error };
};

export default useGetTableBrands;
