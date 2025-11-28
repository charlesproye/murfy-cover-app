import { useState } from 'react';
import { ROUTES } from '@/routes';
import { PinVehicleResponse } from '@/interfaces/dashboard/passport/DataGraph';
import fetchWithAuth from '@/services/fetchWithAuth';
import useSWR from 'swr';

export const usePostPinVehicle = (
  vin: string | undefined,
): {
  isSubmitting: boolean;
  error: string | null;
  postPinVehicle: (isPinned: boolean) => Promise<void>;
} => {
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const postPinVehicle = async (isPinned: boolean): Promise<void> => {
    setIsSubmitting(true);
    setError(null);
    try {
      await fetchWithAuth(`${ROUTES.PIN_VEHICLE}/${vin}`, {
        method: 'POST',
        body: JSON.stringify(isPinned),
      });
    } catch (err: unknown) {
      setError(err as string);
      throw err;
    } finally {
      setIsSubmitting(false);
    }
  };

  return { isSubmitting, error, postPinVehicle };
};

export const useGetPinVehicle = (vin: string | undefined): PinVehicleResponse => {
  const { data, isLoading, error } = useSWR(
    `${ROUTES.GET_PINNED_VEHICLE}/${vin}`,
    fetchWithAuth<
      | {
          is_pinned: boolean;
        }
      | undefined
    >,
    {
      revalidateOnFocus: false,
    },
  );
  return { data, isLoading, error };
};
