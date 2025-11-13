import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';
import {
  SearchBarInputRequestSwr,
  SearchBarInput,
} from '@/interfaces/common/search/SearchBar';

const useGetVinSearch = (input: string): SearchBarInputRequestSwr => {
  const shouldFetch = input.trim() !== '';

  const { data, isLoading, error } = useSWR(
    shouldFetch ? `${ROUTES.SEARCHBAR}/${input}` : null,
    fetchWithAuth<SearchBarInput[]>,
    {
      revalidateOnFocus: false,
    },
  );

  return { data, isLoading, error };
};
export default useGetVinSearch;
