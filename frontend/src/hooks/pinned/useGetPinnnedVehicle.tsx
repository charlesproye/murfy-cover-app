import {
  IndividualVehicleConsumption,
  IndividualVehiclePinned,
} from '@/interfaces/individual/IndividualVehicle';
import { IndividualVehicleFastCharge } from '@/interfaces/individual/IndividualVehicle';
import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

export const usePinnedVehicles = (
  fleet: string | null,
  label: string | null,
  page: number,
): {
  data: { data: IndividualVehiclePinned['data'] } | undefined;
  isLoading: boolean;
  error: unknown;
  hasMorePages: boolean;
} => {
  const { data, isLoading, error } = useSWR(
    fleet ? `${ROUTES.PINNED_VEHICLE}/${fleet}?page=${page}&limit=50` : null,
    fetchWithAuth<IndividualVehiclePinned['data']>,
    { revalidateOnFocus: false },
  );

  return {
    data: data ? { data } : undefined,
    isLoading,
    error,
    hasMorePages: data ? data.length === 50 : false,
  };
};

export const useFastChargeVehicles = (
  fleet: string | null,
  label: string | null,
  page: number,
): {
  data: { data: IndividualVehicleFastCharge['data'] } | undefined;
  isLoading: boolean;
  error: unknown;
  hasMorePages: boolean;
} => {
  const { data, isLoading, error } = useSWR(
    fleet ? `${ROUTES.FAST_CHARGE_VEHICLE}/${fleet}?page=${page}&limit=50` : null,
    fetchWithAuth<IndividualVehicleFastCharge['data']>,
    { revalidateOnFocus: false },
  );

  return {
    data: data ? { data } : undefined,
    isLoading,
    error,
    hasMorePages: data ? data.length === 50 : false,
  };
};

export const useConsumptionVehicles = (
  fleet: string | null,
  label: string | null,
  page: number,
): {
  data: { data: IndividualVehicleConsumption['data'] } | undefined;
  isLoading: boolean;
  error: unknown;
  hasMorePages: boolean;
} => {
  const { data, isLoading, error } = useSWR(
    fleet ? `${ROUTES.CONSUMPTION_VEHICLE}/${fleet}?page=${page}&limit=50` : null,
    fetchWithAuth<IndividualVehicleConsumption['data']>,
    { revalidateOnFocus: false },
  );

  return {
    data: data ? { data } : undefined,
    isLoading,
    error,
    hasMorePages: data ? data.length === 50 : false,
  };
};
