import React, { useState, useEffect } from 'react';
import {
  useGetPinVehicle,
  usePostPinVehicle,
} from '@/hooks/dashboard/passport/usePinVehicle';

interface PinButtonProps {
  vin: string;
}

const PinButton: React.FC<PinButtonProps> = ({ vin }) => {
  const { data: pinData, isLoading: isPinLoading } = useGetPinVehicle(vin);
  const { postPinVehicle, isSubmitting } = usePostPinVehicle(vin);
  const [localPinned, setLocalPinned] = useState<boolean>(!!pinData?.is_pinned);

  // Synchronise l'état local avec la donnée serveur si elle change
  useEffect(() => {
    if (typeof pinData?.is_pinned === 'boolean') {
      setLocalPinned(pinData.is_pinned);
    }
  }, [pinData]);

  const handleClick = async (): Promise<void> => {
    setLocalPinned((prev) => !prev); // Optimistic update
    try {
      await postPinVehicle(!localPinned);
    } catch {
      setLocalPinned((prev) => !prev); // revert if error
    }
  };

  return (
    <button
      onClick={handleClick}
      disabled={isSubmitting || isPinLoading}
      className={`px-3 py-1 rounded-lg text-xs transition-all ${
        localPinned
          ? 'bg-red-500 text-white hover:bg-red-600'
          : 'bg-primary text-white hover:bg-primary/90'
      } ${isSubmitting || isPinLoading ? 'opacity-50 cursor-not-allowed' : ''}`}
    >
      {isSubmitting || isPinLoading ? (
        <div className="animate-spin rounded-full h-4 w-4 border-2 border-white border-t-transparent mx-auto"></div>
      ) : localPinned ? (
        'Unpin'
      ) : (
        'Pin'
      )}
    </button>
  );
};

export default PinButton;
