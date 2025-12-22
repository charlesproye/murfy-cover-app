import React, { useState, useEffect } from 'react';
import {
  useGetPinVehicle,
  usePostPinVehicle,
} from '@/hooks/dashboard/passport/usePinVehicle';
import { IconHeart, IconHeartFilled } from '@tabler/icons-react';

interface PinButtonProps {
  vin: string;
}

const PinButton: React.FC<PinButtonProps> = ({ vin }) => {
  const { data: pinData, isLoading: isPinLoading } = useGetPinVehicle(vin);
  const { postPinVehicle, isSubmitting } = usePostPinVehicle(vin);
  const [localPinned, setLocalPinned] = useState<boolean>(!!pinData?.is_pinned);

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
      className={`p-1 rounded-lg text-xs transition-all text-primary hover:bg-primary/30 ${
        isSubmitting || isPinLoading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'
      }`}
    >
      {isSubmitting || isPinLoading ? (
        <div className="animate-spin rounded-full h-4 w-4 border-2 border-white border-t-transparent mx-auto"></div>
      ) : localPinned ? (
        <IconHeartFilled className="h-4 w-4" />
      ) : (
        <IconHeart className="h-4 w-4" />
      )}
    </button>
  );
};

export default PinButton;
