import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

export interface PinnedVehicleDataResponse {
  vin: string;
  makeName: string;
  odometer: number;
  soh: number;
  sohOem: number;
  sohPer10000km: number;
  startDate?: string;
}
export const PinnedVehicleColumns = [
  'vin',
  'makeName',
  'odometer',
  'soh',
  'sohOem',
  'sohPer10000km',
  'startDate',
];

export const usePinnedVehicles = (
  fleet: string | null,
  label: string | null,
  page: number,
) => {
  const { data, isLoading, error } = useSWR(
    fleet ? `${ROUTES.PINNED_VEHICLES}/${fleet}?page=${page}&limit=50` : null,
    fetchWithAuth<PinnedVehicleDataResponse[]>,
    { revalidateOnFocus: false },
  );

  return {
    data: data ? { data } : undefined,
    isLoading,
    error,
    hasMorePages: data ? data.length === 50 : false,
  };
};
