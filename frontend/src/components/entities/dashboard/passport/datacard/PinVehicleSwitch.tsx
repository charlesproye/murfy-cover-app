import React, { useState } from 'react';
import { useGetPinVehicle } from '@/hooks/dashboard/passport/usePinVehicle';
import { toast } from 'sonner';
import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';

const PinVehicleSwitch: React.FC<{ vin: string | undefined }> = ({ vin }) => {
  const {
    data: pinData,
    isLoading: isPinLoading,
    mutate: mutatePinVehicle,
  } = useGetPinVehicle(vin);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handlePinVehicle = async (isPinned: boolean): Promise<void> => {
    setIsSubmitting(true);
    try {
      await fetchWithAuth(`${ROUTES.PIN_VEHICLE}/${vin}`, {
        method: 'POST',
        body: JSON.stringify(isPinned),
      });
      mutatePinVehicle();
      toast.success(isPinned ? 'Vehicle pinned' : 'Vehicle unpinned');
    } catch (error: unknown) {
      toast.error('Error pinning vehicle' + error);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="flex justify-between items-center select-none">
      <span className="text-black text-sm mb-1">Pin to favorites</span>
      <label className="flex items-center cursor-pointer select-none relative">
        <input
          type="checkbox"
          className="sr-only peer"
          checked={pinData?.is_pinned ?? false}
          onChange={() => handlePinVehicle(!pinData?.is_pinned)}
          disabled={isPinLoading || isSubmitting}
        />
        <div className="w-11 h-6 bg-white border border-primary/30 peer-checked:bg-primary rounded-full transition-all"></div>
        <div className="absolute left-1 top-1 bg-white w-4 h-4 rounded-full shadow-sm border border-gray/30 peer-checked:border-primary peer-checked:translate-x-5 transition-all"></div>
      </label>
    </div>
  );
};

export default PinVehicleSwitch;
